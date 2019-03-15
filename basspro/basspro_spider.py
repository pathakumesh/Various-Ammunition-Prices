
import time
import csv
import re
import scrapy
import hashlib
import requests
from math import ceil
from random import randint
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess


PROXY = '125.27.10.209:59790'


class ExtractItem(scrapy.Item):
    ItemCode = scrapy.Field()
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


class BassproSpider(scrapy.Spider):
    name = "basspro_spider"
    allowed_domains = ["basspro.com"]
    start_urls = ["https://www.basspro.com/shop/en/ammunition"]
    total_pages = None
    done = False

    def parse(self, response):
        pattern = re.compile(
            r'SearchBasedNavigationDisplayJS\.init\(.*,\'(\S+)\',{')
        api_url = pattern.findall(response.text)
        if not api_url:
            print('API URL not found')
            return
        self.api_url = api_url[0].replace('resultsPerPage=24', 'pageSize=100')
        yield scrapy.Request(
            url=self.api_url,
            callback=self.parse_results,
            dont_filter=True,
        )

    def parse_results(self, response):
        if not self.total_pages and not self.done:
            total_products = response.xpath(
                '//span[@class="num_products"]/text()').re(r' of (\d+)')
            self.total_pages = ceil(int(total_products[0])/100)

        results = response.xpath('//div[@class="product_info"]')
        for result in results:
            item = ExtractItem()
            name = result.xpath(
                'div[@class="product_name"]/a/text()').extract_first()
            name = name.strip()
            code = result.xpath(
                'div[contains(@class, "product_price")]')\
                .re(r'product_price_(\d+)')
            item['ItemCode'] = code[0]
            item['Name'] = name

            link = result.xpath(
                'div[@class="product_name"]/a/@href').extract_first()
            item['Link'] = link
            yield scrapy.Request(
                url=link,
                callback=self.parse_item_detail,
                dont_filter=True,
                meta={'item': item}
            )
        if self.total_pages and not self.done:
            self.api_url = self.api_url + '&beginIndex={}&productBeginIndex={}'
            for i in range(1, self.total_pages):
                next_page = self.api_url.format(i*100, i*100)
                print('next page is ')
                print(next_page)
                yield scrapy.Request(
                    url=next_page,
                    callback=self.parse_results,
                    dont_filter=True,
                )
            self.done = True

    def parse_item_detail(self, response):
        extracted_items = list()
        item = response.meta['item']
        code = item['ItemCode']
        rating = response.xpath(
            '//span[@class="bvseo-ratingValue"]/text()').extract_first()
        item['Rating'] = rating
        reviews = response.xpath(
            '//span[@class="bvseo-reviewCount"]/text()').extract_first()
        item['Reviews'] = reviews
        detail_block = response.xpath(
            '//div[@class="row entry full"]')
        for each in detail_block:
            caliber_name = each.xpath(
                'div[@class="col2 gridCell CartridgeorGauge unanchored"]/'
                'div/text()').extract_first()
            item['Caliber'] = caliber_name.strip() if caliber_name else None

            bullet_weight = each.xpath(
                'div[@class="col2 gridCell Grain unanchored"]/'
                'div/text()').extract_first()
            item['BulletWeight'] = bullet_weight.strip()\
                if bullet_weight else None

            bullet_type = each.xpath(
                'div[@class="col2 gridCell BulletType unanchored"]/'
                'div/text()').extract_first()
            item['BulletType'] = bullet_type.strip()\
                if bullet_type else None

            units = each.xpath(
                'div[@class="col2 gridCell Quantity unanchored"]/'
                'div/text()').extract_first()
            item['Units'] = units.lower().replace('rounds', '').strip()\
                if units else None

            sale_price = each.xpath(
                'div[@class="col2 gridCell PriceAvailability anchored"]//'
                'div[@itemprop="offers"]/span[contains(@id, "offerPrice_")]'
                '/span/text()').extract_first()
            item['Price'] = sale_price.strip()\
                if sale_price else None

            regular_price = each.xpath(
                'div[@class="col2 gridCell PriceAvailability anchored"]//'
                'div[@itemprop="offers"]/'
                'span[@class="old_price"]/text()').extract_first()
            if regular_price:
                item['IsOnSale'] = "Yes"
            item['RegularPrice'] = regular_price.lower().replace('reg:', '')\
                .strip() if regular_price else item['Price']

            availability = each.xpath(
                'div//div[contains(@id, "WC_Online_Inventory_Section")]'
                '/span[@class="text" and @content]/text()').extract_first()
            item['Availability'] = availability.strip()\
                if availability else None
            extracted_items.append(item)
            item = ExtractItem()
            item['ItemCode'] = code
        for item in extracted_items:
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
    process = CrawlerProcess({
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
    })
    process.crawl(BassproSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 40
    request_delay = 0.01
    run_spider(no_of_threads, request_delay)
