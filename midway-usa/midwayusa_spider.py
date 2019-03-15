
import json
import time
import csv
import re
import scrapy
import hashlib
import requests
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
    IsOnSale = scrapy.Field()
    RegularPrice = scrapy.Field()
    Availability = scrapy.Field()
    Link = scrapy.Field()


class MidwayUSASpider(scrapy.Spider):
    name = "midway_usa_spider"
    allowed_domains = ["midwayusa.com"]
    start_urls = ["https://www.midwayusa.com/ammunition/br?cid=653"]
    code = "316606"
    apikey = "de1ea431-69fc-407f-b33c-04933ca167f6"

    def parse(self, response):
        url = response.xpath(
            '//a[@aria-label="Select 96 Per Page"]/@href').extract_first()
        url = "https://www.midwayusa.com" + url
        yield scrapy.Request(
            url=url,
            callback=self.parse_result,
            dont_filter=True,
        )

    def parse_result(self, response):
        header = response.xpath('//div[@class="results-heading-range"]')
        print(header.xpath('string()').extract_first())
        results = response.xpath('//li[@class="product list"]')
        for result in results:
            item = ExtractItem()
            # ------------------------------------------------------------
            name = result.xpath(
                'a/div[@class="product-description"]/text()').extract_first()
            name = name.strip()
            name = re.sub(r'\s+\S*\.\.\.', '', name)
            item['Name'] = name
            # ------------------------------------------------------------
            link = result.xpath(
                'a[div[@class="product-description"]]/@href').extract_first()
            item['Link'] = link
            product_id = re.findall(r'product/(\d+)', link)
            # ------------------------------------------------------------
            if result.xpath(
             'div[@class="priceblock price-container"]/'
             '*[@class="price-discount-type"]'):
                is_on_sale = True
            else:
                is_on_sale = False
            item['IsOnSale'] = is_on_sale
            # ------------------------------------------------------------
            regular_price = result.xpath(
                'div[@class="priceblock price-container"]/'
                'span[@class="price-retail"]/span/text()').extract_first()
            if regular_price:
                regular_price = regular_price.strip()
                item['RegularPrice'] = regular_price
            # ------------------------------------------------------------
            availability = result.xpath(
                'a[@class="product-status"]/text()').extract_first()
            availability = availability.strip()
            item['Availability'] = availability
            # ------------------------------------------------------------
            price = result.xpath(
                'div[@class="priceblock price-container"]/'
                'div[@class=" price" or @class="price " or'
                '@class="price is-discounted" or @class="is-discounted price"]'
                '/text()').extract_first()
            price = price.strip()
            item["Price"] = price
            # ------------------------------------------------------------
            if product_id:
                review_url = "https://display.powerreviews.com/m/"\
                      f"{self.code}/l/en_US/product/{product_id[0]}/reviews?"\
                      f"apikey={self.apikey}"
                yield scrapy.Request(
                    url=review_url,
                    callback=self.parse_reviews,
                    dont_filter=True,
                    meta={
                        'item': item,
                        'product_id': product_id[0]
                    }
                )
            else:
                yield item

        next_page = response.xpath(
            '//a[@class="secondary-button pagination-next"]'
            '/@href').extract_first()
        if next_page:
            next_page = "https://www.midwayusa.com" + next_page
            yield scrapy.Request(
                url=next_page,
                callback=self.parse_result,
                dont_filter=True,
            )

    def parse_reviews(self, response):
        item = response.meta['item']
        product_id = response.meta['product_id']
        json_response = json.loads(response.text)
        result = json_response['results'][0]
        if result.get('rollup'):
            item['Rating'] = result['rollup']['average_rating']
            item['Reviews'] = result['rollup']['review_count']

        unit_url = f"https://www.midwayusa.com/productdata/{product_id}"
        yield scrapy.Request(
            url=unit_url,
            callback=self.parse_units,
            dont_filter=True,
            meta={'item': item}
        )

    def parse_units(self, response):
        item = response.meta['item']
        units = []
        json_response = json.loads(response.text)
        for filter_group in json_response['filterGroups']:
            if filter_group['name'] == 'Quantity':
                units = [
                    opt['name'].lower().replace('round', '').strip()
                    for opt in filter_group['filterOptions']
                ]
        if units:
            item['Units'] = units[0]
        attributes = json_response.get(
            'productFamily', {}).get('attributes', [])
        for attribute in attributes:
            if attribute['name'] == 'Cartridge':
                item['Caliber'] = attribute['value']
            if attribute['name'] == 'Grain Weight':
                item['BulletWeight'] = attribute['value']
            if attribute['name'] == 'Bullet Style':
                item['BulletType'] = attribute['value']
        price = item['Price']
        regular_price = item.get('RegularPrice')
        if (' - ') in price:
            item["Price"] = price.split(' -')[0].strip()
            if regular_price:
                item['RegularPrice'] = regular_price.split(
                                ' -')[0].strip()
            yield item
            item = ExtractItem()
            item['Units'] = units[1]
            item["Price"] = price.split(' -')[1].strip()
            if regular_price:
                item['RegularPrice'] = regular_price.split(
                    ' -')[1].strip()
            yield item

        else:
            yield item


def get_customer_reviews(product_id):
    code = "316606"
    apikey = "de1ea431-69fc-407f-b33c-04933ca167f6"
    customer_reviews = None
    url = f"https://display.powerreviews.com/m/{code}/l/en_US/"\
        f"product/{product_id}/reviews?apikey={apikey}"
    response = requests.get(url)
    json_response = response.json()
    result = json_response['results'][0]
    if result.get('rollup'):
        customer_reviews = {
            'rating': result['rollup']['average_rating'],
            'review': result['rollup']['review_count'],
        }
    return customer_reviews


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
    process.crawl(MidwayUSASpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 10
    request_delay = 1
    run_spider(no_of_threads, request_delay)
