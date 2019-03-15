
import time
import json
import csv
import re
import scrapy
import hashlib
import requests
import traceback
from random import randint
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess


PROXY = '177.152.53.1:35132'
CATEGORY_ID = "15759"
API_KEY = "b7q0kwscytvgbzt0hpldktc35"


class ExtractItem(scrapy.Item):
    Name = scrapy.Field()
    Reviews = scrapy.Field()
    Rating = scrapy.Field()
    Caliber = scrapy.Field()
    BulletWeight = scrapy.Field()
    BulletType = scrapy.Field()
    Units = scrapy.Field()
    Price = scrapy.Field()
    IsOnSale = scrapy.Field()
    RegularPrice = scrapy.Field()
    Link = scrapy.Field()


class AcademySpider(scrapy.Spider):
    name = "academy_spider"
    allowed_domains = ["www.cheaperthandirt.com"]
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/72.0.3626.119 Safari/537.36',
    }

    def start_requests(self):
        base_url = "https://www.academy.com/api/search/?displayFacets=true"\
                   f"&facets=&orderBy=7&categoryId={CATEGORY_ID}"\
                   "&pageSize=100&pageNumber={}"
        for i in range(1, 25):
            url = base_url.format(i)
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True,
            )

    def parse(self, response):
        json_response = json.loads(response.text)
        urls = [p['seoURL'] for p in json_response['productinfo']]
        for url in urls:
            url = "https://www.academy.com" + url
            yield scrapy.Request(
                url=url,
                callback=self.parse_result,
                dont_filter=True,
            )

    def parse_result(self, response):
        item = ExtractItem()
        item["Link"] = response.url
        caliber_weight_pattern = re.compile(
            r'\"Caliber\":\"(.*?)\".*\"Grain weight\":\"(.*?)\"')
        match = caliber_weight_pattern.findall(response.text)
        if match:
            item['Caliber'], item['BulletWeight'] = match[0]
        name = response.xpath(
            '//h1[@data-auid="PDP_ProductName"]/text()').extract_first()
        item['Name'] = name.strip()

        caliber_name = response.xpath(
            '//span[contains(text(),"Gauge/Caliber")]/'
            'following-sibling::span[1]/text()').extract_first()
        if caliber_name and not item.get('Caliber'):
            item['Caliber'] = caliber_name.strip()

        bullet_weight = response.xpath(
            '//span[contains(text(),"Grain Weight")]/'
            'following-sibling::span[1]/text()').extract_first()
        if bullet_weight and not item.get('BulletWeight'):
            item['BulletWeight'] = bullet_weight.strip()

        units_pattern = re.compile(
            r'\"Number of Rounds\":\"(.*?)\"', re.I)
        match = units_pattern.findall(response.text)
        if match:
            item['Units'] = match[0]

        price_block = response.xpath(
            '//div[@data-auid="PDP_ProductPrice"]//'
            'span/small[text()="$"]/'
            'following-sibling::span[1]/text()').extract()
        if price_block:
            item['Price'] = price_block[0]
            if len(price_block) == 2:
                item['RegularPrice'] = price_block[1]
                item['IsOnSale'] = "YES"
        product_id = response.xpath(
            '//div[@data-bv-productid]/@data-bv-productid').extract_first()
        if product_id:
            review_url = "https://api.bazaarvoice.com/data/display/0.2alpha/"\
                         f"product/summary?PassKey={API_KEY}&"\
                         f"productid={product_id}&"\
                         "contentType=reviews,questions&"\
                         "reviewDistribution=primaryRating,recommended&"\
                         "rev=0&contentlocale=en_US"
            yield scrapy.Request(
                url=review_url,
                callback=self.parse_reviews,
                dont_filter=True,
                meta={'item': item}
            )
        else:
            yield item

    def parse_reviews(self, response):
        item = response.meta['item']
        json_response = json.loads(response.text)
        if json_response.get('reviewSummary'):
            item['Reviews'] = json_response[
                'reviewSummary']['numReviews']
            item['Rating'] = json_response[
                'reviewSummary']['primaryRating']['average']
        yield item


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
    process.crawl(AcademySpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 10
    request_delay = 0.2
    run_spider(no_of_threads, request_delay)
