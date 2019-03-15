
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


PROXY = '110.136.147.83:3128'


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


class BudsGunShopSpider(scrapy.Spider):
    name = "budgunshop_spider"
    allowed_domains = ["www.budsgunshop.com"]
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/72.0.3626.119 Safari/537.36',
    }
    start_urls = [
        "https://www.budsgunshop.com/catalog/search.php/type/Ammunition"]

    def parse(self, response):
        caliber_types = response.xpath(
            '//div[@class="row see-more-attributes-content" and '
            '@data-facet-type="manu"]//a')
        for caliber_type in caliber_types:
            url = caliber_type.xpath('@href').extract_first()
            url = "https://www.budsgunshop.com" + url
            total_count = caliber_type.xpath(
                'span[@class="products-available"]/text()').extract_first()
            total_count = int(total_count)
            if total_count < 1000:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_result,
                )
            else:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_manufacturer,
                )

    def parse_manufacturer(self, response):
        manu_types = response.xpath(
            '//div[@class="row see-more-attributes-content" and '
            '@data-facet-type="caliber"]//a')
        for manu_type in manu_types:
            url = manu_type.xpath('@href').extract_first()
            url = "https://www.budsgunshop.com" + url
            yield scrapy.Request(
                url=url,
                callback=self.parse_result,
            )

    def parse_result(self, response):
        print(f'next_page_url: {response.url}')
        items = response.xpath(
            '//a[@class="product-box-link"]/@href').extract()
        for item in items:
            url = "https://www.budsgunshop.com/catalog/" + item
            yield scrapy.Request(
                url=url,
                callback=self.parse_item,
            )
        next_page_url = response.xpath(
            '//li[contains(@class, "active")]/'
            'following-sibling::li[1]/a/@href').extract_first()
        if next_page_url:
            next_page_url = "https://www.budsgunshop.com" + next_page_url
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
            )

    def parse_item(self, response):
        item = ExtractItem()
        name = response.xpath(
            '//h1[@class="item_header"]/text()').extract_first()
        name = name.strip()
        item['Name'] = name
        item['Link'] = response.url
        item['Reviews'] = response.xpath(
            '//span[@itemprop="ratingCount"]/text()').extract_first()
        item['Rating'] = response.xpath(
            '//div[@id="ProductReviews"]/'
            'span[@itemprop="ratingValue"]/text()').extract_first()

        caliber_block = response.xpath(
            '///td[text()="Caliber/Gauge"]/'
            'following-sibling::td[1]/'
            'descendant::text()').extract()
        item['Caliber'] = ''.join(caliber_block)

        bullet_weight = response.xpath(
            '///td[text()="Bullet Weight"]/'
            'following-sibling::td[1]/'
            'text()').extract_first()
        item['BulletWeight'] = bullet_weight.strip() if bullet_weight else None

        bullet_type = response.xpath(
            '///td[text()="Bullet Type"]/'
            'following-sibling::td[1]/'
            'text()').extract_first()
        item['BulletType'] = bullet_type.strip() if bullet_type else None

        units = response.xpath(
            '///td[text()="Box Qty"]/'
            'following-sibling::td[1]/'
            'text()').extract_first()
        item['Units'] = units.strip() if units else None

        price = response.xpath(
            '//span[@itemprop="price"]/text()').extract_first()
        item['Price'] = price.strip() if price else None

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
    process.crawl(BudsGunShopSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 10
    request_delay = 0.2
    run_spider(no_of_threads, request_delay)
