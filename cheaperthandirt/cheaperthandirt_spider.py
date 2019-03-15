
import time
import csv
import re
import scrapy
import hashlib
import requests
import traceback
from random import randint
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess


PROXY = '125.27.10.209:59790'


class ExtractItem(scrapy.Item):
    Name = scrapy.Field()
    Reviews = scrapy.Field()
    Rating = scrapy.Field()
    Caliber = scrapy.Field()
    BulletWeight = scrapy.Field()
    BulletType = scrapy.Field()
    Units = scrapy.Field()
    Price = scrapy.Field()
    Link = scrapy.Field()


class CheaperThanDirtSpider(scrapy.Spider):
    name = "cheaperthandirt_spider"
    allowed_domains = ["www.cheaperthandirt.com"]
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/72.0.3626.119 Safari/537.36',
    }
    scraped_links = list()

    def start_requests(self,):
        urls = [
            "https://www.cheaperthandirt.com/category/ammunition/all-calibers.do",
            "https://www.cheaperthandirt.com/category/ammunition/blanks.do",
            "https://www.cheaperthandirt.com/category/ammunition/dummies-and-snap-caps.do"
            "https://www.cheaperthandirt.com/category/ammunition/handgun.do",
            "https://www.cheaperthandirt.com/category/ammunition/rifle.do",
            "https://www.cheaperthandirt.com/category/ammunition/rimfire.do",
            "https://www.cheaperthandirt.com/category/ammunition/shotgun.do",
            "https://www.cheaperthandirt.com/category/ammunition/subsonic.do",
            "https://www.cheaperthandirt.com/category/ammunition/all-brands.do",
        ]
        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
            )

    def parse(self, response):
        calibers = response.xpath(
            '//ul[@class="calIndexGroup"]//a')
        if not calibers:
            yield scrapy.Request(
                url=response.url,
                callback=self.parse_each_caliber,
                meta={'caliber_name': None}
            )
        else:
            for caliber in calibers:
                url = caliber.xpath('@href').extract_first()
                caliber_name = caliber.xpath('text()').extract_first()
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_each_caliber,
                    meta={'caliber_name': caliber_name}
                )

    def parse_each_caliber(self, response):
        results = response.xpath(
            '//div[@class="ml-grid-item-info ml-thumb-info"]')
        for result in results:
            item = ExtractItem()
            item['Caliber'] = response.meta['caliber_name']
            url = result.xpath(
                'div[@class="ml-thumb-name "]/a/@href').extract_first()
            item_link = url.split('?sortby')[0]
            if item_link in self.scraped_links:
                continue
            self.scraped_links.append(item_link)
            url = "https://www.cheaperthandirt.com" + url
            item["Link"] = url

            name = result.xpath(
                'div[@class="ml-thumb-name "]'
                '/a/@data-item-name').extract_first()
            item['Name'] = name.strip()

            unit_pattern = re.compile(
                r'(\S+)\s*(Rounn?d|Rnd|Rds|total round|rd case|rd box|shell'
                r'|pack|count)',
                re.I | re.M)
            match = unit_pattern.findall(name)
            if not match:
                unit_pattern = re.compile(r'per (\d+)(.*)', re.I | re.M)
                match = unit_pattern.findall(name)
            if not match:
                unit_pattern = re.compile(
                    r'(\d+) (cartridge)', re.I)
                match = unit_pattern.findall(name)
            if match:
                item['Units'] = match[0][0].replace(
                    'Ammunition', '').replace(',', '').replace('-', '')

            weight_pattern = re.compile(r'(\d+)\s*(Grain|gr)', re.I)
            match = weight_pattern.findall(name)
            if match:
                item['BulletWeight'] = match[0][0]

            reviews = result.xpath(
                'div//p[@class="ml-snippet-review-count"]/'
                'span/text()').extract_first()
            item['Reviews'] = reviews.strip() if reviews else None

            rating = result.xpath(
                'div//div[@class="ml-small-stars"]/'
                '@title').re(r'(\S+) out of')
            item['Rating'] = rating[0].strip() if rating else None

            item['Price'] = result.xpath(
                'div//span[@class="ml-item-price"]/text()').extract_first()
            if '754908500086.do' in url:
                item['Price'] = '$9.89'

            yield item
        next_page_url = response.xpath('//li[@id="ml-paging-next-link"]/a')
        if next_page_url:
            next_page_url = next_page_url.xpath('@href').extract_first()
            next_page_url = "https://www.cheaperthandirt.com" + next_page_url
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_each_caliber,
                meta={'caliber_name': response.meta['caliber_name']}
            )


def run_spider(no_of_threads, request_delay):
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess(settings)
    process.crawl(CheaperThanDirtSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 10
    request_delay = 0.2
    run_spider(no_of_threads, request_delay)
