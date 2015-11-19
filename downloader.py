#!/usr/bin/env python3
import logging
import re
import signal
import sys
import asyncio
import urllib.parse
import time
import datetime

import aiohttp

class DownloadProxy:
    def __init__(self):
        self.queue = []

    def register(self, url, priority=0):
        self.queue.append(url)


class Downloader:


    
    def __init__(self, download_concurrency=10, domain_download_interval=datetime.timedelta(seconds=2)):
        self.network_sem = asyncio.Semaphore(download_concurrency)
        self.todo_queue = asyncio.Queue()

        self.domain_queues = {}
        self.downloaded_html_queue = asyncio.Queue()
        self.callbacks = []

    def run(self, initial_url):
        loop = asyncio.get_event_loop()
        self.connector = aiohttp.TCPConnector(share_cookies=True, loop=loop)
        try:
            print("added handler")
            loop.add_signal_handler(signal.SIGINT, loop.stop)
        except RuntimeError:
            pass
        done, _ = loop.run_until_complete(asyncio.ensure_future(self._initiate(initial_url)))
        self.connector.close()

        for future in done:
            future.result()

    @asyncio.coroutine
    def _initiate(self, initial_url):
        try:
            yield from self.todo_queue.put(initial_url)
            html_processor = asyncio.ensure_future(self._html_processor())  # Set initial work.
            task = asyncio.ensure_future(self._worker())  # Set initial work.
            while True: yield from asyncio.sleep(10)
        finally:
            self.connector.close()

    @asyncio.coroutine
    def register(self, url):
        yield from self.todo_queue.put(url)

    def subscribe(self, callback):
        self.callbacks.append(callback)

    @asyncio.coroutine
    def _html_processor(self):
        while True:
            try:
                url, html  = yield from self.downloaded_html_queue.get()

                proxy = DownloadProxy()
                for callback in self.callbacks:
                    callback(proxy, url, html)

                for next_url in proxy.queue:
                    yield from self.register(next_url)

            finally:
                self.downloaded_html_queue.task_done()

    @asyncio.coroutine
    def _worker(self):
        while True:
            try:
                url = yield from self.todo_queue.get()
                domain = urllib.parse.urlparse(url).netloc

                first_domain = False
                if domain not in self.domain_queues:
                    first_domain = True
                    self.domain_queues[domain] = asyncio.Queue()
                    asyncio.ensure_future(self._domain_worker(domain))  # Set initial work.

                yield from self.domain_queues[domain].put(url)
            finally:
                self.todo_queue.task_done()


    @asyncio.coroutine
    def _domain_worker(self, domain):
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
        yield from self.network_sem.acquire()
        task = asyncio.ensure_future(self.download_worker(url))
        task.add_done_callback(lambda t: self.network_sem.release())

    @asyncio.coroutine
    def download_worker(self, url):
        try:
            resp = yield from aiohttp.request('get', url, connector=self.connector)
        except Exception as exc:
            print('...', url, 'has error', repr(str(exc)))
        else:
            if (resp.status == 200):
                html = (yield from resp.read()).decode('utf-8', 'replace')
                yield from self.downloaded_html_queue.put((url, html))

            resp.close()


