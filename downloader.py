#!/usr/bin/env python3
import logging
import re
import signal
import sys
import asyncio
import urllib.parse
import time

import aiohttp


class Crawler:
    def __init__(self, rooturl, loop, maxtasks=2):
        self.rooturl = rooturl
        self.loop = loop
        self.done = set()

        self.network_sem = asyncio.Semaphore(maxtasks)
        self.todo_queue = asyncio.Queue()
        self.domain_queues = {}

        self.downloaded_html_queue = asyncio.Queue()

        # connector stores cookies between requests and uses connection pool
        self.connector = aiohttp.TCPConnector(share_cookies=True, loop=loop)

    @asyncio.coroutine
    def run(self):
        yield from self.todo_queue.put(self.rooturl)
        task_parser = asyncio.ensure_future(self.parser())  # Set initial work.
        task = asyncio.ensure_future(self.worker())  # Set initial work.
        yield from asyncio.sleep(1)
        while True:
            yield from asyncio.sleep(1)

        self.connector.close()
        print("##############################")
        print("##############################")
        print("##############################")
        self.loop.stop()

    @asyncio.coroutine
    def parser(self):
        while True:
            try:
                url, html = yield from self.downloaded_html_queue.get()
                urls = re.findall(r'(?i)href=["\']?([^\s"\'<>]+)', html)
                for u in urls:
                    if u not in self.done:
                        yield from self.todo_queue.put(u)
            finally:
                self.downloaded_html_queue.task_done()

    @asyncio.coroutine
    def worker(self):
        while True:
            try:
                #print(len(self.done), 'completed tasks,', self.todo_queue.qsize(),'in queue.')
                #yield from asyncio.sleep(2)
                url = yield from self.todo_queue.get()
                domain = urllib.parse.urlparse(url).netloc

                first_domain = False
                if domain not in self.domain_queues:
                    first_domain = True
                    self.domain_queues[domain] = asyncio.Queue()
                    asyncio.ensure_future(self.domain_worker(domain))  # Set initial work.

                yield from self.domain_queues[domain].put(url)
            finally:
                self.todo_queue.task_done()


    @asyncio.coroutine
    def domain_worker(self, domain):
        import datetime
        domain_queue = self.domain_queues[domain]

        least_interval = datetime.timedelta(seconds=10)
        next_downloadable = datetime.datetime.now()
        while True:
            try:
                now = datetime.datetime.now()
                if now < next_downloadable:
                    wait_second = (next_downloadable-now).total_seconds() 
                    yield from asyncio.sleep(wait_second)
                url = yield from domain_queue.get()
                yield from self.download(url)
                next_downloadable += least_interval
            finally:
                domain_queue.task_done()


    @asyncio.coroutine
    def download(self, url):
        if url not in self.done:
            yield from self.network_sem.acquire()
            task = asyncio.ensure_future(self.download_worker(url))
            task.add_done_callback(lambda t: self.network_sem.release())

    @asyncio.coroutine
    def download_worker(self, url):
        try:
            print("downloading",url)
            resp = yield from aiohttp.request('get', url, connector=self.connector)
        except Exception as exc:
            print('...', url, 'has error', repr(str(exc)))
        else:
            if (resp.status == 200 and
                    ('text/html' in resp.headers.get('content-type'))):
                data = (yield from resp.read()).decode('utf-8', 'replace')

                yield from self.downloaded_html_queue.put((url,data))

            resp.close()
            self.done.add(url)


def main(rooturl):
    loop = asyncio.get_event_loop()

    c = Crawler(rooturl, loop)
    asyncio.ensure_future(c.run())

    try:
        loop.add_signal_handler(signal.SIGINT, loop.stop)
    except RuntimeError:
        pass
    loop.run_forever()
    print('todo:', c.todo_queue.qsize())
    print('done:', len(c.done), '; ok:', sum(c.done.values()))


if __name__ == '__main__':
    if '--iocp' in sys.argv:
        from asyncio import events, windows_events
        sys.argv.remove('--iocp')
        logging.info('using iocp')
        el = windows_events.ProactorEventLoop()
        events.set_event_loop(el)

    url = "http://news.yahoo.co.jp"
    main(url)
