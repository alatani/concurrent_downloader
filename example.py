#!/usr/bin/env python3

import downloader
import re


if __name__ == '__main__':
    done = set()
    d = downloader.Downloader()

    def parser(d, url, html):
        print(url)
        #raise "hoge"
        urls = re.findall(r'(?i)href=["\']?([^\s"\'<>]+)', html)
        for u in urls:
            if u not in done:
                done.add(u)
                d.register(u)

    d.subscribe(parser)

    url = "http://news.yahoo.co.jp"
    d.run(url)
    print("RUN")

