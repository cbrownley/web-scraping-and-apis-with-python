# -*- coding: utf-8 -*-
import codecs
from datetime import datetime
import MySQLdb
import re
import scrapy
from string import replace, split, lstrip, strip
from scrapy.contrib.linkextractors import LinkExtractor
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.http import Request

from abtV2.items import AbtV2Item

date_time = datetime.strftime(datetime.today(), '%Y-%m-%d %H:%M:%S')

all_products = [ ]

regexp = re.compile('|'.join([
  r'(?P<my_pattern1>^\$?(\d*\.\d{1,2})$)',  # e.g., $.50, .50, $1.50, $.5, .5
  r'(?P<my_pattern2>^\$?(\d+)$)',           # e.g., $500, $5, 500, 5
  r'(?P<my_pattern3>^\$(\d+\.?)$)',         # e.g., $5.
  r'(?P<my_pattern4>^\w+\s+\w+\s+\$?(\d*\.\d{1,2})$)',
]))

connection = MySQLdb.connect(host='localhost', port=3306, db='products_poc', user='clinton', passwd='mypassword', use_unicode=True, charset="utf8")
c = connection.cursor()

class AbtV2CrawlerSpider(CrawlSpider):
    name = 'abt_v2_crawlerII'
    allowed_domains = ['abt.com']
    start_urls = ['http://www.abt.com/category/654/Counter-Depth-Refrigerators.html?category_id=654&start_urls_index=0',
                  'http://www.abt.com/category/115/Bottom-Freezer-Refrigerators.html?category_id=115&start_urls_index=0',
                  'http://www.abt.com/category/117/Side-by-Side-Refrigerators.html?category_id=117&start_urls_index=0',
                  'http://www.abt.com/category/219/Top-Freezer-Refrigerators.html?category_id=219&start_urls_index=0',
                  'http://www.abt.com/category/449/Wine-Refrigerators-Beverage-Centers.html?category_id=449&start_urls_index=0',
                  'http://www.abt.com/category/114/Compact-Refrigerators.html?category_id=114&start_urls_index=0'
                 ]

    rules = (
        #Rule(SgmlLinkExtractor(allow='http:\/\/www\.abt\.com\/category\/654\/Counter-Depth-Refrigerators\.html\?category_id=654&start_index=.*'), callback='parse_page', follow=True),
        #Rule(SgmlLinkExtractor(allow='http:\/\/www\.abt\.com\/category\/654\/Counter-Depth-Refrigerators\.html'), restrict_xpaths=('//div[@class="pagination"]' ,), callback='parse_page', follow=True),
        #, restrict_xpaths=('//div[@class="pagination"]' ,))
        Rule(SgmlLinkExtractor(allow=['http:\/\/www\.abt\.com\/category\/654\/Counter-Depth-Refrigerators\.html\?category_id=654&start_index=.*',\
                                      'http:\/\/www\.abt\.com\/category\/115\/Bottom-Freezer-Refrigerators\.html\?category_id=115&start_index=.*',\
                                      'http:\/\/www\.abt\.com\/category\/117\/Side-by-Side-Refrigerators\.html\?category_id=117&start_index=.*',\
                                      'http:\/\/www\.abt\.com\/category\/219\/Top-Freezer-Refrigerators\.html\?category_id=219&start_index=.*',\
                                      'http:\/\/www\.abt\.com\/category\/449\/Wine-Refrigerators-Beverage-Centers\.html?category_id=449&start_index=.*',\
                                      'http:\/\/www\.abt\.com\/category\/114\/Compact-Refrigerators\.html?category_id=114&start_index=.*'], restrict_xpaths='//div[@class="pagination"]'), callback='parse_page', follow=True),
        )

    def parse_page(self, response):
        hxs = Selector(response)

        rows = hxs.xpath('//div[@class="cl_outer_box"]')

        for row in rows:
            item = AbtV2Item()

            item['upc'] = ''
            item['model_number'] = ''
            item['regular_price'] = '0.00'
            item['sale_price'] = '0.00'
            item['review_score'] = ''
            item['review_count'] = ''
            item['retailer'] = ''
            item['department'] = ''
            item['sku'] = ''
            item['brand'] = ''
            item['color'] = ''
            item['name'] = ''
            item['url'] = ''
            item['image_small'] = ''
            item['image_medium'] = ''
            item['image_large'] = ''
            item['long_description'] = ''
            item['date_time'] = ''

            link = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_title"]/a')
            
            item_title = link.xpath('text()').extract()
            if len(item_title) > 0:
                item['title'] = str(link.xpath('text()').extract()[0])

            item_link = link.xpath('@href').extract()
            if len(item_link) > 0:
                item['link'] = str(link.xpath('@href').extract()[0])

            item_url = link.xpath('@href').extract()
            if len(item_url) > 0:
                item['url'] = str('http://www.abt.com') + str(link.xpath('@href').extract()[0])

            model_number = link.xpath('text()').extract()
            if len(model_number) > 0:
                item['model_number'] = str(link.xpath('text()').extract()[0].split()[-1])

            image_medium = row.xpath('.//div[@class="cl_img_container"]/a/img/@src').extract()
            if len(image_medium) > 0:
                item['image_medium'] = str(row.xpath('.//div[@class="cl_img_container"]/a/img/@src').extract()[0])

            long_description = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_description"]/text()').extract()
            if len(long_description) > 0:
                item['long_description'] = str(row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_description"]/text()').extract()[0].encode('ascii', 'ignore')).strip().replace('/','')

            regular_price1 = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/span[@class="regular_price"]/strike/text()').extract()
            for value in regular_price1:
                value = value.strip()
                result = regexp.search(value)
                if result:
                    amount = [x for x in result.groups() if x is not None].pop()
                    item['regular_price'] = amount
            
            #if len(regular_price1) > 0:
            #    item['regular_price'] = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/span[@class="regular_price"]/strike/text()').extract()
            #else:
            #item['regular_price'] = ''       

            regular_price2 = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/div[@class="deal_price"]/text()').extract()
            for value in regular_price2:
                value = value.strip()
                result = regexp.search(value)
                if result:
                    amount = [x for x in result.groups() if x is not None].pop()
                    item['regular_price'] = amount

            #if len(regular_price2) > 0:
            #    item['regular_price'] = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/div[@class="deal_price"]/text()').extract()
            #else:
            #    item['regular_price'] = ''

            regular_price3 = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/div[@class="sale_block"]/strong/div[@class="regular_price"]/text()').extract()
            for value in regular_price3:
                value = value.strip()
                result = regexp.search(value)
                if result:
                    amount = [x for x in result.groups() if x is not None].pop()
                    item['regular_price'] = amount

            #if len(regular_price3) > 0:
            #    item['regular_price'] = str(row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/div[@class="sale_block"]/strong/div[@class="regular_price"]/text()').extract()[0]).strip().split()[2].strip('$')
            #else:
            #    item['regular_price'] = ''

            #if len(regular_price1) > 0 and '.' in regular_price1:
            #    item['regular_price'] = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/span[@class="regular_price"]/strike/text()').extract()
            #elif len(regular_price2) > 0 and '.' in regular_price2:
            #    item['regular_price'] = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/div[@class="deal_price"]/text()').extract()
            #elif len(regular_price3) > 0 and '.' in regular_price3:
            #    item['regular_price'] = str(row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/div[@class="sale_block"]/strong/div[@class="regular_price"]/text()').extract()[0]).strip().split()[2].strip('$')
            #else:
            #    item['regular_price'] = ''

            sale_price1 = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/div[@class="sale_block"]/strong/span[@class="sale_block_price"]/text()').extract()
            for value in sale_price1:
                value = value.strip()
                result = regexp.search(value)
                if result:
                    amount = [x for x in result.groups() if x is not None].pop()
                    item['sale_price'] = amount
            #if len(sale_price1) > 0:
            #    item['sale_price'] = str(row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_price"]/div[@class="sale_block"]/strong/span[@class="sale_block_price"]/text()').extract()[0]).strip().split()[2].strip('$').replace(',','')
            #else:
            #    item['sale_price'] = ''

            department = row.xpath('//div[@class="bread_crumb_bar"]/div[@class="bread_crumbs"]/text()').extract()[-1]
            if len(department) > 0:
                item['department'] = str(row.xpath('//div[@class="bread_crumb_bar"]/div[@class="bread_crumbs"]/text()').extract()[-1].encode('ascii', 'ignore')).strip().strip('>')

            sku = row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_abt_model"]/text()').extract()
            if len(sku) > 0:
                item['sku'] = str(row.xpath('.//div[@class="cl_prod_container"]/div[@class="cl_title_container"]/div[@class="cl_abt_model"]/text()').extract()[0]).strip().split()[2]

            brand = link.xpath('text()').extract()
            if len(brand) > 0:
                item['brand'] = str(link.xpath('text()').extract()[0].split()[0])

            url = 'http://www.abt.com{}'.format(''.join(item['link']))

            yield Request(url=url, meta={'item': item}, callback=self.parse_item_page)


    def parse_item_page(self, response):
        product = [ ]

        hxs = Selector(response)

        item = response.meta['item']

        name = hxs.xpath('//h1[@id="product_title"]/span[@itemprop="name"]/text()').extract()
        if len(name) > 0:
            name = str(hxs.xpath('//h1[@id="product_title"]/span[@itemprop="name"]/text()').extract()[0])
            item['name'] = str(hxs.xpath('//h1[@id="product_title"]/span[@itemprop="name"]/text()').extract()[0])
            if "stainless" in name.lower().split():
                item['color'] = "Stainless Steel"
            elif "white" in name.lower().split():
                item['color'] = "White"
            elif "black" in name.lower().split():
                item['color'] = "Black"
            else:
                item['color'] = "Unknown"

        image_small = hxs.xpath('.//div[@id="prod_thumbs"]/a/img/@src').extract()
        if len(image_small) > 0:
            item['image_small'] = str(hxs.xpath('.//div[@id="prod_thumbs"]/a/img/@src').extract()[0])

        image_large1 = hxs.xpath('.//div[@id="productimage"]/div[@id="productimagecontainer"]/div[@id="abt_product"]/div[@id="wrap"]/a[@class="cloud-zoom"]/img[@id="main_std_img"]/@src').extract()
        if len(image_large1) > 0:
            item['image_large'] = str(hxs.xpath('.//div[@id="productimage"]/div[@id="productimagecontainer"]/div[@id="abt_product"]/div[@id="wrap"]/a[@class="cloud-zoom"]/img[@id="main_std_img"]/@src').extract()[0])

        image_large2 = hxs.xpath('.//div[@id="productimagecontainer"]/div[@id="abt_product"]/a[@id="main_std_anchor"]/img[@id="main_std_img"]/@src').extract()
        if len(image_large2) > 0:
            item['image_large'] = str(hxs.xpath('.//div[@id="productimagecontainer"]/div[@id="abt_product"]/a[@id="main_std_anchor"]/img[@id="main_std_img"]/@src').extract()[0])       

        upc = hxs.xpath('//div[@class="abt_model"]/span[@id="product-bottom-info-and-pricegrabber-upc"]/text()').extract()
        if len(upc) > 0:
            item['upc'] = hxs.xpath('//div[@class="abt_model"]/span[@id="product-bottom-info-and-pricegrabber-upc"]/text()').extract()[0].encode('ascii', 'ignore')

        review_score = hxs.xpath('//div[@id="mid_body_content"]/div[@class="col_large right_margin"]/form[@id="product_form"]/div[@style="position:relative; border: 1px solid #cdcdcd; padding: 7px 15px; margin-bottom: 15px;"]\
                                /div[@id="pBody"]/div[@id="pricingSection"]/div[@class="product-snippet"]/div[@class="product-review-snippet"]/div[@id="review_container"]/div[@style="float:left;"]/span[@id="review_avg"]/text()').extract()
        if len(review_score) > 0:
            item['review_score'] = str(hxs.xpath('//div[@id="mid_body_content"]/div[@class="col_large right_margin"]/form[@id="product_form"]/div[@style="position:relative; border: 1px solid #cdcdcd; padding: 7px 15px; margin-bottom: 15px;"]\
                                /div[@id="pBody"]/div[@id="pricingSection"]/div[@class="product-snippet"]/div[@class="product-review-snippet"]/div[@id="review_container"]/div[@style="float:left;"]/span[@id="review_avg"]/text()').extract()[0])

        review_count = hxs.xpath('//div[@id="mid_body_content"]/div[@class="col_large right_margin"]/form[@id="product_form"]/div[@style="position:relative; border: 1px solid #cdcdcd; padding: 7px 15px; margin-bottom: 15px;"]\
                                /div[@id="pBody"]/div[@id="pricingSection"]/div[@class="product-snippet"]/div[@class="product-review-snippet"]/div[@id="review_container"]/div[@style="float:left;"]/span[@id="review_num_rev"]/a/strong/text()').extract()
        if len(review_count) > 0:
            item['review_count'] = str(hxs.xpath('//div[@id="mid_body_content"]/div[@class="col_large right_margin"]/form[@id="product_form"]/div[@style="position:relative; border: 1px solid #cdcdcd; padding: 7px 15px; margin-bottom: 15px;"]\
                                /div[@id="pBody"]/div[@id="pricingSection"]/div[@class="product-snippet"]/div[@class="product-review-snippet"]/div[@id="review_container"]/div[@style="float:left;"]/span[@id="review_num_rev"]/a/strong/text()').extract()[0])
        
        regular_price4 = hxs.xpath('.//div[@id="product_pricing_container"]/text()').extract()
        for value in regular_price4:
            value = value.strip()
            result = regexp.search(value)
            if result:
                amount = [x for x in result.groups() if x is not None].pop()
                item['regular_price'] = amount
        #if len(regular_price4) > 0:
        #    item['regular_price'] = str(hxs.xpath('.//div[@id="product_pricing_container"]/text()').extract()[0].encode('ascii', 'ignore')).split()[2].strip().strip('$')
        #else:
        #    item['regular_price'] = ''

        regular_price5 = hxs.xpath('.//div[@id="product_pricing_container"]/div[@style="font-size: 7pt; font-weight:normal;"]/text()').extract()
        for value in regular_price5:
            value = value.strip()
            result = regexp.search(value)
            if result:
                amount = [x for x in result.groups() if x is not None].pop()
                item['regular_price'] = amount
        #if len(regular_price5) > 0:
        #    item['regular_price'] = str(hxs.xpath('.//div[@id="product_pricing_container"]/div[@style="font-size: 7pt; font-weight:normal;"]/text()').extract()[0].encode('ascii', 'ignore')).split()[2].strip().strip('$')
        #else:
        #    item['regular_price'] = ''

        #if len(regular_price4) > 0 and '.' in regular_price4:
        #    item['regular_price'] = str(hxs.xpath('.//div[@id="product_pricing_container"]/text()').extract()[0].encode('ascii', 'ignore')).split()[2].strip().strip('$')
        #elif len(regular_price5) > 0 and '.' in regular_price5:
        #    item['regular_price'] = str(hxs.xpath('.//div[@id="product_pricing_container"]/div[@style="font-size: 7pt; font-weight:normal;"]/text()').extract()[0].encode('ascii', 'ignore')).split()[2].strip().strip('$')

        sale_price2 = hxs.xpath('.//div[@id="product_pricing_container"]/span[@style="font-size: 14px; padding-top: 0px; font-weight:bold; color: #bf1c1c;"]/span[@itemprop="price"]/text()').extract()
        for value in sale_price2:
            value = value.strip()
            result = regexp.search(value)
            if result:
                amount = [x for x in result.groups() if x is not None].pop()
                item['sale_price'] = amount
        #if len(sale_price2) > 0:
        #    item['sale_price'] = str(hxs.xpath('.//div[@id="product_pricing_container"]/span[@style="font-size: 14px; padding-top: 0px; font-weight:bold; color: #bf1c1c;"]/span[@itemprop="price"]/text()').extract()[0].encode('ascii', 'ignore')).strip().strip('$')
        #else:
        #    item['sale_price'] = ''

        item['retailer'] = 'Abt Electronics'

        item['date_time'] = date_time

        product.append(item['upc'])
        product.append(item['model_number'])
        product.append(item['regular_price'])
        product.append(item['sale_price'])
        product.append(item['review_score'])
        product.append(item['review_count'])
        product.append(item['retailer'])
        product.append(item['department'])
        product.append(item['sku'])
        product.append(item['brand'])
        product.append(item['color'])
        product.append(item['name'])
        product.append(item['url'])
        product.append(item['image_small'])
        product.append(item['image_medium'])
        product.append(item['image_large'])
        product.append(item['long_description'])
        product.append(item['date_time'])

        if item['upc'] != '' and (item['regular_price'] != '0.00' or item['sale_price'] != '0.00'):
            #all_products.append(product)
            print product

            c.execute("""INSERT INTO products (upc, model_number, regular_price, sale_price, review_score, review_count, retailer,
                         department, sku, brand, color, name, url, image_small, image_medium, image_large, long_description, date_time)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", product)
            connection.commit()

#        return item


