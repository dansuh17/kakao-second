import requests as req
import time
import queue
from threading import Thread


class Crawler:
    MY_TOKEN = 'UJ-Kbx7xqmJ2pFWxzR0VdnG80sW8VWb-1N6pjCKZ'

    def __init__(self):
        self.base_url = 'http://api.welcome.kakao.com'
        self.auth_file_name = 'auth.txt'
        self.auth_token = self.obtain_token()
        self.auth_header = {'X-Auth-Token': self.auth_token}

        # init
        doc_list = self.seed()  # get document seeds
        parsed, categories = self.parse_doc_list(doclist=doc_list)
        print('Categories : ', categories, parsed)

        # initialize worker threads.
        self.queues = self.create_queues(categories)
        self.workers = self.create_workers(categories)

        # initial queue of documents to collect urls from
        for category, doc_id in parsed:
            print('putting: ', category, doc_id)
            self.queues[category].put((category, doc_id))
        self.start_workers()

        for queue in self.queues:
            queue.join()  # it will block further until all elements have been processed

    def start_workers(self):
        """
        Tell the workers to start.
        """
        for _, worker in self.workers.items():
            worker.start()
        # self.workers['art'].start()

    def create_queues(self, categories):
        """
        Create thread-safe queues for each category:
        Args:
            categories (List[str]): list of categories

        Returns:
            queues in shape {'category': Queue}
        """
        queues = {}
        for cat in categories:
            queues[cat] = queue.Queue()
        return queues

    def create_workers(self, categories):
        """
        Create worker threads, each by category.

        Args:
            categories (List[str]): categories

        Returns:
            worker threads by category
        """
        workers = {}
        for category in categories:
            workers[category] = Thread(
                target=self.crawl_by_category, args=(self.queues[category], category))
        return workers

    def crawl_by_category(self, category_q, category):
        self.crawl(category_q)

    def crawl(self, cat_q):
        """
        Begin crawl loop.

        Args:
            cat_q (Queue): queue storing (category, doc_id) for each categories.
        """
        # crawl loop
        to_add = set()  # set of image ids to add (to extract features from)
        to_delete = set()  # list of deletion ids
        to_save = list()  # list of ids to save

        total_added = set()
        total_deleted = set()
        while True:
            category, docid = cat_q.get()
            print(category, '\n\n')
            try:
                next_url, images, scenario = self.collect_docs(category, docid)
                # in case next url remains the same and no images are found
                # the document has not yet been created.
                if next_url == docid or len(images) == 0:
                    print('No images - sleeping')
                    cat_q.put(self.parse_doc_string(next_url))
                    time.sleep(7)
                    continue

                cat_q.put(self.parse_doc_string(next_url))

                for image in images:
                    if image['type'] == 'add' and image['id'] not in total_added:
                        to_add.add(image['id'])
                    elif image['type'] == 'del' and image['id'] not in total_deleted:
                        to_delete.add(image['id'])

                # indicate that an element have been processed
                cat_q.task_done()
            except req.exceptions.ReadTimeout:
                cat_q.put((category, docid))
                cat_q.task_done()  # re-adding the element will increase the queue count

            # extract features
            # do not extract unless 50 images are ready
            if len(to_add) > 50:
                while len(to_add) > 50:
                    req_extract = [to_add.pop() for _ in range(50)]
                    features, ok_ids = self.extract_feature(req_extract)

                    # in case of feature extraction fails
                    if len(features) > 0:
                        to_save += features

                    # remove successful ids
                    ok_id_set = set(ok_ids)
                    to_add -= ok_id_set
                    total_added |= ok_id_set

            # save features - do not save unless 50 are ready
            if len(to_save) > 50:
                while len(to_save) > 50:
                    save_send_req, to_save = to_save[:50], to_save[50:]
                    save_ok = self.save_feature(features=save_send_req)
                    # add to pool if save failed
                    if not save_ok:
                        to_save = save_send_req + to_save

            # delete images
            if len(to_delete) > 50:
                while len(to_delete) > 50:
                    req_delete = [to_delete.pop() for _ in range(50)]
                    del_ok = self.delete_feature(ids=req_delete)
                    # add to pool if deletion failed
                    if not del_ok:
                        to_delete |= set(req_delete)
                    else:
                        total_deleted |= set(req_delete)

            time.sleep(0.1)

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
            doc_parsed.append((category, doc_id))
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

    def collect_docs(self, category, doc_id):
        url = '{}/doc/{}/{}'.format(self.base_url, category, doc_id)
        res = req.get(url, headers=self.auth_header, timeout=1)
        status = res.status_code
        print('collect : {}'.format(status))
        if status == 401:
            raise self.UnAuthorizedException('Unauthorized while delete()')
        if status == 200:
            body = res.json()
            next_url = body['next_url']
            images = body['images']
            scenario = None
            if 'scenario' in body:
                scenario = body['scenario']
        else:
            raise self.DocumentCollectionFailException(status)
        return next_url, images, scenario

    def extract_feature(self, image_ids):
        """
        Extracts features of image ids.

        Args:
            image_ids:

        Returns:
            List if tuple of shape ('id_string', 'feature')
            List of ids that successfully feature-extracted
        """
        url = '{}/image/feature'.format(self.base_url)
        params = {'id': ','.join(image_ids)}
        features = []
        successful_ids = []
        try:
            res = req.get(url, params=params, headers=self.auth_header, timeout=1)
            status = res.status_code
            print('extract : {}'.format(status))
            raw_features = res.json()['features']

            if status == 401:
                raise self.UnAuthorizedException('Unauthorized while delete()')
            for feature in raw_features:
                successful_ids.append(feature['id'])
                features.append({'id': feature['id'], 'feature': int(feature['feature'])})
                # features.append((feature['id'], int(feature['feature'])))
        except req.exceptions.ReadTimeout or req.exceptions.ConnectionError:
            print('extract : timeout')

        return features, successful_ids

    def save_feature(self, features):
        """
        Given the features send a post request to save features.
        Feature's length should be max 50.

        Args:
            features (a list of {'id': ~, 'feature': ~}): features to save

        Returns:
            stats (bool): true if save successful
        """
        url = '{}/image/feature'.format(self.base_url)
        try:
            payload = {'data': features}
            res = req.post(url, headers=self.auth_header, json=payload, timeout=1)
            status = res.status_code
            print('save : {}'.format(status))

            if status == 401:
                raise self.UnAuthorizedException('Unauthorized while delete()')
        except req.exceptions.ReadTimeout:
            print('save : timeout')
            status = 900
        return status == 200

    def delete_feature(self, ids):
        """
        Delete images by ids.

        Args:
            ids(List[str]): list of ids to delete

        Returns:
            status (bool): true if delete successful
        """
        # uses tuples..strangely
        payload = {'data': [{'id': doc_id} for doc_id in ids]}
        url = '{}/image/feature'.format(self.base_url)
        try:
            res = req.delete(url, headers=self.auth_header, json=payload, timeout=1)
            status = res.status_code
            print('delete : {}'.format(res.status_code))

            if status == 401:
                raise self.UnAuthorizedException('Unauthorized while delete()')
        except req.exceptions.ReadTimeout:
            print('delete : timeout')
            status = 900
        return status == 200

    class DocumentCollectionFailException(Exception):
        def __init__(self, status):
            self.status = status

    class UnAuthorizedException(Exception):
        def __init__(self, message):
            self.message = message


if __name__ == '__main__':
    while True:
        try:
            crawler = Crawler()
        except:
            time.sleep(1)
