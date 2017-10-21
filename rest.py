import requests
import json

# get
res = requests.get('https://api.github.com/user', auth=('deNsuh', 'zarch248'))
print(res)
print(dir(res))
print(res.status_code)
print(res.headers)
print(res.headers.keys())
print(res.headers['content-type'])
print(res.encoding)
print(res.cookies)
print(res.text)
print(res.json())

# post
res = requests.post('http://httpbin.org/post', data={'somekey': 'hahah'})

# form-encoded post
data = {'key': 'v1', 'key2': 'v2'}
res = requests.post('http://httpbin.org/post', data=data)

# form-encoded post 2
data = (('key', 'v1'), ('key1', 'v2'))  # allows multiple values for same key
res = requests.post('http://httpbin.org/post', data=data)

# json encoded post
data = {'key': 'v1', 'key2': 'v2'}
data = json.dumps(data)
res = requests.post('http://httpbin.org/post', data=data)

# multipart encoded post
# files = {'file': open('sample.csv', 'rb')}
# res = requests.post('http://httpbin.org/post', files=files)

# multipart encoded post with specifics
# files = {'file': ('renamed_file_name.csv', open('sample.csv', 'rb'), 'application/vnd.ms-excel', {'Expires': '0'})}
# res = requests.post('http://httpbin.org/post', files=files)


# others
res = requests.put('http://httpbin.org/post', data={'somekey': 'hahah'})
res = requests.delete('http://httpbin.org/post')
res = requests.head('http://httpbin.org/post')
res = requests.options('https://httpbin.org/post')

# url params
params = {'key1': 'haha', 'key2': 'hahah2'}
res = requests.get('https://api.github.com/user', auth=('deNsuh', 'zarch248'), params=params)
print(res.url)
assert res.url == 'https://api.github.com/user?key1=haha&key2=hahah2'

# raw socket response
res = requests.get('https://api.github.com/user', auth=('deNsuh', 'zarch248'), stream=True)
print(res.raw.read(10))

# custom headers
header = {'user-agent': 'myapp/1.0'}
res = requests.get('https://api.github.com/user', auth=('deNsuh', 'zarch248'), headers=header)

# setting timeouts
res = requests.get('https://api.github.com/user', auth=('deNsuh', 'zarch248'), timeout=0.001)


# AND SOME JSON MODULE EXAMPLES
# dump to string
json.dumps(['foo', {'bar': ('baz', None, 1.0, 2)}])

# dump to file
# json.dump(['foo', {'bar': ('baz', None, 1.0, 2)}], output_file/or I/O port)

# pretty encoding
print(json.dumps(['foo', {'bar': ('baz', None, 1.0, 2)}], sort_keys=True, indent=4))

# decoding
print(json.loads('["foo", {"bar":["baz", null, 1.0, 2]}]'))
