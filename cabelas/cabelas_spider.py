
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
    IsOnSale = scrapy.Field()
    RegularPrice = scrapy.Field()
    Availability = scrapy.Field()
    ItemCode = scrapy.Field()
    Link = scrapy.Field()


class CabelasSpider(scrapy.Spider):
    name = "cabelas_spider"
    allowed_domains = ["www.cabelas.com"]
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/72.0.3626.119 Safari/537.36',
    }
    total_items = dict()

    def start_requests(self):
        url = "https://www.cabelas.com/catalog/browse/ammunition/_/N-1100188"
        yield scrapy.Request(
            url=url,
            callback=self.parse,
            headers=self.headers,
            dont_filter=True,
        )

    def parse(self, response):
        categories = response.xpath(
            '//a[@class="itemName"]/@href').extract()
        for category in categories:
            url = "https://www.cabelas.com" + category
            category = re.findall(r'browse/(.*?)/', category)[0]
            self.total_items.update({category: 0})
            yield scrapy.Request(
                url=url,
                callback=self.parse_max_items_url,
                headers=self.headers,
                dont_filter=True,
                meta={'category': category},
            )

    def parse_max_items_url(self, response):
        category = response.meta['category']
        values = response.xpath(
            '//select[@name="itemsPerPage"]/option/@value').extract()
        if not values:
            url = response.url
        else:
            value = values[-1]
            url = "https://www.cabelas.com" + value
        yield scrapy.Request(
                url=url,
                callback=self.parse_result,
                headers=self.headers,
                dont_filter=True,
                meta={'category': category},
            )

    def parse_result(self, response):
        header = response.xpath(
            '//div[@class="pageCount"]/span/text()').extract_first()
        print(header)
        category = response.meta['category']
        results = response.xpath('//div[@class="productItem"]')
        self.total_items.update({
            category: self.total_items[category] + len(results)
        })
        for result in results:
            item = ExtractItem()
            name = result.xpath(
                'div[@class="productContentBlock"]/a/'
                'h3/text()').extract_first()
            name = name.strip()
            item['Name'] = name

            link = result.xpath(
                'div[@class="productContentBlock"]/a/@href').extract_first()
            link = "https://www.cabelas.com" + link
            item['Link'] = link

            code = result.xpath(
                'div[@class="productContentBlock"]/'
                'div[@class="itemNumber"]/a/text()').re(r'Item: (.*)')
            if code:
                item['ItemCode'] = code[0]

            customer_review_block = result.xpath(
                'div//div[@class="customerReviews"]/a')
            if customer_review_block:
                rating = customer_review_block.xpath(
                    'img/@title').re(r'(\S+) out of ')
                item['Rating'] = rating[0]
                reviews = customer_review_block.xpath(
                    'span/text()').re(r'\((\S+)\)')
                item['Reviews'] = reviews[0]
            yield scrapy.Request(
                url=link,
                callback=self.parse_item_detail,
                headers=self.headers,
                dont_filter=True,
                meta={'item': item}
            )
        next_page = response.xpath(
            '//div[@class="paginationFilter"]/a[text()="Next"]'
            '/@href').extract_first()
        if next_page:
            next_page = "https://www.cabelas.com" + next_page
            yield scrapy.Request(
                url=next_page,
                callback=self.parse_result,
                headers=self.headers,
                dont_filter=True,
                meta={'category': category},
            )

    def parse_item_detail(self, response):
        item = response.meta['item']
        code = item['ItemCode']
        detail_block = response.xpath(
            '//table[@id="product-chart-table"]//tr')
        headers = response.xpath(
            '//table[@id="product-chart-table"]//th/text()').extract()
        headers = [h.strip() for h in headers if h.strip()]
        if not detail_block:
            yield item
        for each in detail_block:
            caliber_name = each.xpath(
                'td[1]/text()').extract_first()
            if not caliber_name or not caliber_name.strip():
                continue
            item['Caliber'] = caliber_name.strip()
            try:
                weight_index = headers.index('Bullet Weight') + 1
                bullet_weight = each.xpath(
                    f'descendant::td[{weight_index}]/text()').extract_first()
                item['BulletWeight'] = bullet_weight.strip()\
                    if bullet_weight else None
            except:
                pass

            try:
                type_index = headers.index('Bullet Type') + 1
                bullet_type = each.xpath(
                    f'descendant::td[{type_index}]/text()').extract_first()
                item['BulletType'] = bullet_type.strip()\
                    if bullet_type else None
            except:
                pass

            try:
                unit_index = headers.index("Number of Rounds") + 1
                units = each.xpath(
                    f'descendant::td[{unit_index}]/text()').extract_first()
                item['Units'] = units.lower().replace('per', '').strip()\
                    if units else None
            except:
                pass
            try:
                price_index = headers.index('Price') + 1
                regular_price = each.xpath(
                    f'descendant::td[{price_index}]//dd'
                    '[@class="regularnprange" or @class="nprange"]/'
                    'text()').extract_first()
                item['RegularPrice'] = regular_price.strip()\
                    if regular_price else None

                sale_price = each.xpath(
                    f'descendant::td[{price_index}]//dd[@class="saleprice"]/'
                    f'text()').extract_first()
                item['Price'] = sale_price.strip()\
                    if sale_price else item['RegularPrice']
                if sale_price:
                    item['IsOnSale'] = "Yes"

                availability = each.xpath(
                    f'descendant::td[{price_index}]//div[@class="stockstatus"]'
                    '/div/text()').extract_first()
                item['Availability'] = availability.strip()\
                    if availability else None
            except:
                pass
            yield item
            item = ExtractItem()
            item['ItemCode'] = code


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
        'HTTPERROR_ALLOWED_CODES': [404],

    }
    process = CrawlerProcess(settings)
    process.crawl(CabelasSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 10
    request_delay = 0.2
    run_spider(no_of_threads, request_delay)
