# KAKAO 2nd TEST

by 서동우, kaehops@gmail.com

## Prerequisites

`python` v.3.6 or above

## Installing Dependencies

`pip3 install -r requirements.txt`

## Running

`python3 crawler.py`

## Structure

There exists 2 types of workers (threads) for this program.
One is URL generator, which generates URLs for requests and adds them to
thread-safe queue. The other is request handler, which collects urls from
the url queue and handles requests to the server.

The URL generator puts the message in url-queue in form `(url, type, payload)`, where
`url` is the request url, `type` is the type of request (not REST API type, but the type
defined in this application - explained below), and `payload` if there are any data required
for requests to send in the body.

The reqeusts are divided in four types: `collect`, `extract`, `save`, and `delete`.

- `collect`: collects images and its command, as well as next crawl target url.
- `extract`: extracts features from images
- `save`: saves the features extracted to the server
- `delete`: deletes the images on the server

Each handler handles requests as described in the requirements.

In order to minimize duplicate requests, the program keeps a data structure that maintains
ids of images saved and deleted. If the collected images are already in the data structure,
the request is discarded.

In order to cope with connection refusals upon too much requests, sleep times are increased
upon every connection denials. Also, keeping only 2 request handler threads is found to be optimal
for the target server.
