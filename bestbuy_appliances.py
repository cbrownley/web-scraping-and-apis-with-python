#!/usr/bin/python
# -*- coding: utf-8 -*-
import codecs
from datetime import datetime
import json
import requests
from string import *
import time
import MySQLdb

my_api_key = 'myapikey'

date_time = datetime.strftime(datetime.today(), '%Y-%m-%d %H:%M:%S')

# Maximum number of calls allowed per day
MAX_CALLS = 50000
# API call results are paginated...First page number is 1
page_number = 1

# Make an API call to determine the total number of pages in the result
response = requests.get('http://api.remix.bestbuy.com/v1/products(longDescription=Refrigerator*)?show=name,upc,manufacturer,modelNumber,regularPrice,salePrice,color,image,mediumImage,largeImage,url,longDescription,customerReviewAverage,customerReviewCount,sku,department&apiKey=' + my_api_key + '&pageSize=100\
&page=' + str(page_number) + '&format=json')

# If the API call's status code isn't a success code (i.e. 200) then print the error message
if response.status_code != 200:
    print '%s' % (response.status_code)

# Load the JSON result into a dictionary called products_dict
products_dict1 = response.json()
# Determine the total number of pages in the result set by accessing the value associated with the key 'totalPages'
total_pages = products_dict1['totalPages']

all_products = []
unique_upcs = {}

# While the page_number is less than the total number of pages in the result set and
# the page_number is less than the maximum number of calls allowed per day...do the following...
while page_number <= total_pages and page_number < MAX_CALLS:
    response = requests.get('http://api.remix.bestbuy.com/v1/products(longDescription=Refrigerator*)?show=name,upc,manufacturer,modelNumber,regularPric\
e,salePrice,color,image,mediumImage,largeImage,url,longDescription,customerReviewAverage,customerReviewCount,sku,department&apiKey=' + my_api_key + '&pageSize=100&page=' + str(page_number) + '&format=json')

    # If the API call's status code isn't a success code (i.e. 200) then print the error message
    if response.status_code != 200:
        print '%s' % (response.status_code)

    # Load the JSON result into a dictionary called products_dict
    try:
        products_dict = response.json()
    except:
        pass
    # Pretty-print the JSON results to the screen
    #print json.dumps(products_dict, indent=4)

    for product in products_dict['products']:
        record = [ ]

        if 'upc' in product.keys():
            record.append(product['upc'])
        else:
            record.append('')

        if 'modelNumber' in product.keys():
            record.append(product['modelNumber'])
        else:
            record.append('')

        if 'regularPrice' in product.keys():
            record.append(str(product['regularPrice']))
        else:
            record.append('0.00')

        if 'salePrice' in product.keys():
            record.append(str(product['salePrice']))
        else:
            record.append('0.00')

        if 'customerReviewAverage' in product.keys():
            record.append(product['customerReviewAverage'])
        else:
            record.append('')

        if 'customerReviewCount' in product.keys():
            record.append(str(product['customerReviewCount']))
        else:
            record.append('')

        record.append('Best Buy')

        if 'department' in product.keys():
            record.append(product['department'])
        else:
            record.append('')

        if 'sku' in product.keys():
            record.append(str(product['sku']))
        else:
            record.append('')

        if 'manufacturer' in product.keys():
            record.append(product['manufacturer'])
        else:
            record.append('')

        if 'color' in product.keys():
            record.append(product['color'])
        else:
            record.append('')

        if 'name' in product.keys():
            record.append(product['name'])
        else:
            record.append('')

        if 'url' in product.keys():
            record.append(product['url'])
        else:
            record.append('')

        if 'image' in product.keys():
            record.append(product['image'])
        else:
            record.append('')

        if 'mediumImage' in product.keys():
            record.append(product['mediumImage'])
        else:
            record.append('')

        if 'largeImage' in product.keys():
            record.append(product['largeImage'])
        else:
            record.append('')

        if 'longDescription' in product.keys():
            record.append(product['longDescription'].encode('ascii', 'ignore'))
        else:
            record.append('')

        record.append(date_time)

        if (record[0] not in unique_upcs.keys() and record[0] != '') and (record[2] != '0.00' or record[2] != '') and (record[3] != '0.00' or record[3] != ''):
            all_products.append(record)
            unique_upcs[record[0]] = 1

    # Increment page_number by 1
    page_number += 1
    # Wait 1 second before making the next API call (Best Buy's rate limit is 5 calls per second)
    time.sleep(1)

connection = MySQLdb.connect(host='localhost', port=3306, db='products_poc', user='clinton', passwd='mypassword', use_unicode=True, charset="utf8")
c = connection.cursor()

for row in all_products:
    print(row)
    c.execute("""INSERT INTO products (upc, model_number, regular_price, sale_price, review_score, review_count, retailer,
                 department, sku, brand, color, name, url, image_small, image_medium, image_large, long_description, date_time)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", row)
connection.commit()
