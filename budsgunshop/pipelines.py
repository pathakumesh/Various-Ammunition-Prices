import requests
import csv
import os
import glob
import pytz
import time
import datetime
import logging as log
from scrapy import signals
from scrapy.exporters import CsvItemExporter
from rotating_proxies.policy import BanDetectionPolicy
from lxml.html import fromstring
from itertools import cycle
import sys


class ExtractPipeline(object):
    def __init__(self):
        self.files = {}
        self.file_name = f'budsgunshop_results_{time.time()}.csv'
        self.export_fields = [
            'Name',
            'Reviews',
            'Rating',
            'Caliber',
            'BulletWeight',
            'BulletType',
            'Units',
            'Price',
            'Link',
        ]

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        output_file = open(
            self.file_name,
            'w+b',
        )
        self.files[spider] = output_file
        self.exporter = CsvItemExporter(
            output_file,
            fields_to_export=self.export_fields
        )
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        output_file = self.files.pop(spider)
        output_file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class BanPolicy(BanDetectionPolicy):
    def response_is_ban(self, request, response):
        # use default rules, but also consider HTTP 200 responses
        # a ban if there is 'captcha' word in response body.
        # ban = super(BanPolicy, self).response_is_ban(request, response)
        # ban = ban or response.status == 429
        # return ban

        return response.status == 429

    def exception_is_ban(self, request, exception):
        # override method completely: don't take exceptions in account
        return None
