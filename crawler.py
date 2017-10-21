import requests as req
import time
import queue
from threading import Thread
import urllib3

"""
KAKAO 2017 Recruitment 2nd Code Test.

created by: 서동우
"""


class Crawler:
    MY_TOKEN = 'UJ-Kbx7xqmJ2pFWxzR0VdnG80sW8VWb-1N6pjCKZ'

    def __init__(self):
        self.base_url = 'http://api.welcome.kakao.com'
        self.auth_file_name = 'auth.txt'
        self.auth_token = self.obtain_token()
        self.auth_header = {'X-Auth-Token': self.auth_token}

        self.sleep_time = 10
        self.max_sleep_time = 20

        # init
        doc_list = self.seed()  # get document seeds

        # initialize worker threads.
        self.url_queue = queue.Queue(maxsize=50)
        # 2 threads found to be optimal without disconnections
        self.workers = self.create_workers(num_workers=2)
        # url generator generates URL for requests and pushes them to url_queue
        self.url_generator = Thread(target=self.url_generate, args=(self.url_queue, ))

        # states (data) to maintain
        self.to_add = set()  # set of image ids to add (to extract features from)
        self.to_delete = set()  # list of deletion ids
        self.to_save = list()  # list of ids to save
        self.total_added = set()  # total saved images
        self.docs = []  # list of (category, id)

        # put initial urls from seed
        self.put_initial_urls(url_queue=self.url_queue, doc_list=doc_list)
        # initial queue of documents to collect urls from
        self.url_generator.start()
        self.start_workers()

        # blocks until thread exits (returns)
        self.url_queue.join()

    def start_workers(self):
        """
        Tell the workers to start.
        """
        for worker in self.workers:
            worker.start()

    def create_workers(self, num_workers):
        """
        Create worker threads, each by category.

        Args:
            num_workers (int): number of worker threads

        Returns:
            worker threads by category
        """
        workers = []
        for _ in range(num_workers):
            workers.append(Thread(
                target=self.crawl, args=(self.url_queue, )))
        return workers

    def put_initial_urls(self, url_queue, doc_list):
        """
        Put initial URLs to url_queue.

        Args:
            url_queue (Queue): thread-safe queue
            doc_list: list of intial seed documents
        """
        parsed, _ = self.parse_doc_list(doclist=doc_list)
        for category, doc_id, orig_url in parsed:
            url = '{}/doc/{}/{}'.format(self.base_url, category, doc_id)
            url_queue.put((url, 'collect', orig_url))

    def url_generate(self, url_queue):
        """
        Generates URL and puts them to thread-safe queue
        for threads to read and process.

        Args:
            url_queue (Queue): thread-safe queue
        """
        while True:
            while len(self.to_add) > 50:
                req_extract = [self.to_add.pop() for _ in range(50)]
                url = '{}/image/feature'.format(self.base_url)
                url_queue.put((url, 'extract', req_extract))
                print('Generating: ', url, 'extract')
            while len(self.to_save) > 50:
                save_send_req, self.to_save = self.to_save[:50], self.to_save[50:]
                url = '{}/image/feature'.format(self.base_url)
                url_queue.put((url, 'save', save_send_req))
                print('Generating: ', url, 'save')
            while len(self.to_delete) > 50:
                req_delete = [self.to_delete.pop() for _ in range(50)]
                url = '{}/image/feature'.format(self.base_url)
                url_queue.put((url, 'delete', req_delete))
                print('Generating: ', url, 'delete')

            if url_queue.full():
                print('Queue full - sleeping.')
                time.sleep(1)

    def crawl(self, url_queue):
        """
        Begin crawl loop.

        Args:
            url_queue (Queue): queue containing request urls
        """
        while True:
            req_url, req_type, payload = url_queue.get()
            print('Handling: ', req_type)

            if req_type == 'collect':
                # payload in type 'collect' contains previous url
                prev_url = payload
                try:
                    next_url, images, scenario = self.collect(req_url)

                    # put next target url
                    category, doc_id = self.parse_doc_string(next_url)
                    url = '{}/doc/{}/{}'.format(self.base_url, category, doc_id)
                    url_queue.put((url, 'collect', prev_url))

                    # in case next url remains the same and no images are found
                    # the document has not yet been created.
                    if next_url == payload or len(images) == 0:
                        print('No images - sleeping')
                        continue

                    for image in images:
                        if image['type'] == 'add' and image['id'] not in self.total_added:
                            self.to_add.add(image['id'])
                        elif image['type'] == 'del' and image['id']:
                            self.to_delete.add(image['id'])

                    self.reduce_sleep_time()
                except self.UnAuthorizedException:
                    raise
                except (urllib3.exceptions.RequestError, req.exceptions.ReadTimeout,
                        urllib3.exceptions.ConnectionError, urllib3.exceptions.ProtocolError,
                        urllib3.exceptions.MaxRetryError, req.exceptions.ConnectTimeout) as ex:
                    print('Connect timeout at collect: sleeping', ex)
                    self.sleep()
                    url_queue.put((req_url, 'collect', prev_url))
                except Exception as ex:
                    print('Exception from collect : ', ex)
                    self.sleep()
                    # if exception arises, simply put the message back into queue
                    category, doc_id = self.parse_doc_string(prev_url)
                    url = '{}/doc/{}/{}'.format(self.base_url, category, doc_id)
                    url_queue.put((url, 'collect', prev_url))

                url_queue.task_done()
            elif req_type == 'extract':
                image_ids = payload
                try:
                    features, ok_ids = self.extract(req_url, image_ids)

                    # in case of feature extraction fails
                    if len(features) > 0:
                        self.to_save += features

                    # remove successful ids
                    ok_id_set = set(ok_ids)
                    self.to_add -= ok_id_set
                    self.total_added |= ok_id_set
                    self.reduce_sleep_time()
                except self.UnAuthorizedException:
                    raise
                except (urllib3.exceptions.RequestError, req.exceptions.ReadTimeout,
                        urllib3.exceptions.ConnectionError, urllib3.exceptions.ProtocolError,
                        urllib3.exceptions.MaxRetryError, req.exceptions.ConnectTimeout) as ex:
                    print('Connect timeout at extract : sleeping', ex)
                    self.sleep()
                    url_queue.put((req_url, 'extract', image_ids))
                except Exception as ex:
                    print('Exception from extract : ', ex)
                    self.sleep()
                    # if exception arises, simply put the message back into queue
                    url_queue.put((req_url, 'extract', image_ids))

                url_queue.task_done()
            elif req_type == 'save':
                save_features = payload
                try:
                    save_ok = self.save_feature(req_url, features=save_features)
                    # add to pool if save failed
                    if not save_ok:
                        self.to_save = save_features + self.to_save
                        url_queue.put((req_url, 'save', save_features))
                    else:
                        self.reduce_sleep_time()
                except self.UnAuthorizedException:
                    raise
                except (urllib3.exceptions.RequestError, req.exceptions.ReadTimeout,
                        urllib3.exceptions.ConnectionError, urllib3.exceptions.ProtocolError,
                        urllib3.exceptions.MaxRetryError, req.exceptions.ConnectTimeout) as ex:
                    print('Connect timeout at save : sleeping', ex)
                    self.sleep()
                    url_queue.put((req_url, 'save', save_features))
                except Exception as ex:
                    print('Exception from save : ', ex)
                    self.sleep()
                    # if exception arises, simply put the message back into queue
                    url_queue.put((req_url, 'save', save_features))

                url_queue.task_done()
            elif req_type == 'delete':
                req_delete = payload
                try:
                    del_ok = self.delete_feature(url=req_url, ids=req_delete)
                    # add to pool if deletion failed
                    if not del_ok:
                        self.to_delete |= set(req_delete)
                        url_queue.put((req_url, 'delete', req_delete))
                    else:
                        self.reduce_sleep_time()
                        # self.total_deleted |= set(req_delete)
                except self.UnAuthorizedException:
                    raise
                except (urllib3.exceptions.RequestError, req.exceptions.ReadTimeout,
                        urllib3.exceptions.ConnectionError, urllib3.exceptions.ProtocolError,
                        urllib3.exceptions.MaxRetryError, req.exceptions.ConnectTimeout) as ex:
                    print('Connect timeout at delete : sleeping', ex)
                    self.sleep()
                    url_queue.put((req_url, 'delete', req_delete))
                except Exception as ex:
                    print('Exception from delete : ', ex)
                    self.sleep()
                    # if exception arises, simply put the message back into queue
                    url_queue.put((req_url, 'delete', req_delete))
                url_queue.task_done()

    def reduce_sleep_time(self):
        """
        Reduces time of sleep upon request success.
        """
        self.sleep_time = max(self.sleep_time - 1, 10)

    def sleep(self):
        """
        Sleeps for a certain amount of time. For each sleep, the next sleep time increases
        because of unstable connection.
        """
        time.sleep(self.sleep_time)
        print('sleeping: ', self.sleep_time)
        self.sleep_time = min(self.sleep_time + 1, self.max_sleep_time)

    def collect(self, url):
        """
        Collects images to process.

        Args:
            url (str): request url

        Returns:
            next_url (str): next_url to crawl
            images: list of images and its commands ('add' or 'del')
        """
        res = req.get(url, headers=self.auth_header, timeout=self.sleep_time)
        status = res.status_code
        print('collect : {}'.format(status))
        if status == 401:
            raise self.UnAuthorizedException('Unauthorized while delete()')
        if status == 200:
            body = res.json()
            next_url = body['next_url']
            images = body['images']
        else:
            raise self.DocumentCollectionFailException(status)
        return next_url, images

    def parse_doc_list(self, doclist):
        """
        Parse document url list and return tokenized tuples + categories.

        Args:
            doclist (List[str]): list of document url list

        Returns:
            (doc_parsed(List[str, str]), categories: set)
            includes category, seed_id tuples and a set of categories
        """
        doc_parsed = []
        categories = set()
        for doc in doclist:
            category, doc_id = self.parse_doc_string(doc)
            categories.add(category)
            doc_parsed.append((category, doc_id, doc))
        return doc_parsed, categories

    @staticmethod
    def parse_doc_string(doc: str):
        """
        Parse string represented as "/doc/sport/DdOJeMGKKOzb7HRXMg5zGV409"
        and return the category (sport) and id (DdOj...)
        Args:
            doc (str): document url specifier

        Returns:
            (category, id)
        """
        tokenized = doc.strip().split('/')
        doc_id, category = tokenized[-1], tokenized[-2]
        return category, doc_id

    def obtain_token(self):
        """
        Obtain token for authorization.
        Token expires in 10 minutes, and if a token is already obtained,
        use the token previously saved in file.

        Returns:
            auth (str): request authorization token
        """
        url = '{}/token/{}'.format(self.base_url, self.MY_TOKEN)
        res = req.get(url)
        if res.status_code == 200:
            with open(self.auth_file_name, 'w') as f:
                f.write(res.text)
            auth = res.text
        elif res.status_code == 403:
            print('Token not yet expired : using previous token.')
            with open(self.auth_file_name, 'r') as f:
                auth = f.readline().strip()
        else:
            print('Token obtain failed : ' + res.status_code)
            auth = None
        return auth

    def seed(self):
        """
        Retrieve seeds for category of documents.
        Returns:
            seed document url list
        """
        url = '{}/seed'.format(self.base_url)
        res = req.get(url, headers=self.auth_header)
        result_contents = res.text
        search_doc_list = [doc for doc in result_contents.split()]
        return search_doc_list

    def extract(self, url: str, image_ids):
        """
        Extracts features of image ids.

        Args:
            url: request url
            image_ids: ids of images to crawl from

        Returns:
            List if tuple of shape ('id_string', 'feature')
            List of ids that successfully feature-extracted
        """
        params = {'id': ','.join(image_ids)}
        features = []
        successful_ids = []
        res = req.get(url, params=params, headers=self.auth_header, timeout=self.sleep_time)
        status = res.status_code
        print('extract : {}'.format(status))
        raw_features = res.json()['features']

        if status == 401:
            raise self.UnAuthorizedException('Unauthorized while delete()')
        for feature in raw_features:
            successful_ids.append(feature['id'])
            features.append({'id': feature['id'], 'feature': int(feature['feature'])})
        return features, successful_ids

    def save_feature(self, url, features):
        """
        Given the features send a post request to save features.
        Feature's length should be max 50.

        Args:
            features (a list of {'id': ~, 'feature': ~}): features to save

        Returns:
            stats (bool): true if save successful
        """
        payload = {'data': features}
        res = req.post(url, headers=self.auth_header, json=payload)
        status = res.status_code
        print('save : {}'.format(status))

        if status == 401:
            raise self.UnAuthorizedException('Unauthorized while delete()')
        return status == 200

    def delete_feature(self, url, ids):
        """
        Delete images by ids.

        Args:
            ids(List[str]): list of ids to delete

        Returns:
            status (bool): true if delete successful
        """
        # uses tuples..strangely
        payload = {'data': [{'id': doc_id} for doc_id in ids]}
        res = req.delete(url, headers=self.auth_header, json=payload, timeout=self.sleep_time)
        status = res.status_code
        print('delete : {}'.format(res.status_code))

        if status == 401:
            raise self.UnAuthorizedException('Unauthorized while delete()')
        return status == 200

    class DocumentCollectionFailException(Exception):
        def __init__(self, status):
            self.status = status

    class UnAuthorizedException(Exception):
        """Indicates authorization token has expired."""
        def __init__(self, message):
            self.message = message


if __name__ == '__main__':
    while True:
        try:
            crawler = Crawler()
        except:
            time.sleep(1)
