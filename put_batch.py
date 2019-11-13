#/usr/bin/env python3
"""
This program is for testing DynamoDB easily.
written by sudsator (Nov 2019)

usage : python put_batch.py <the number of items>

reference :
http://tohoho-web.com/python/index.html
https://python.civic-apps.com/char-ord/
https://note.nkmk.me/python-unix-time-datetime/
https://python.ms/sub/misc/division/#%EF%BC%93%E3%81%A4%E3%81%AE%E3%82%84%E3%82%8A%E6%96%B9
https://qiita.com/Scstechr/items/c3b2eb291f7c5b81902a
https://qiita.com/UpAllNight/items/a15367ca883ad4588c05
https://note.nkmk.me/python-timeit-measure/
https://dev.classmethod.jp/cloud/aws/lambda-python-dynamodb/
https://qiita.com/nagataaaas/items/531b1fc5ce42a791c7df
https://w.amazon.com/bin/view/AWS/Teams/Proserve/Japan/Initiatives/DesignCookbook/
https://gitlab.com/yamabass80/python_packaging_note/blob/master/Readme.md
"""

import logging
import boto3
import json
import datetime
import re
import random, string
import sys
import timeit
from boto3.dynamodb.conditions import Key, Attr

TABLE_NAME = "TEST_TABLE"
REGION_NAME ="us-east-1"

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
DYNAMO = boto3.resource('dynamodb',region_name=REGION_NAME)

ord_alph = lambda i : i+ord("a")
chr_alph = lambda i : chr(i+ord("a"))

# get a number as random
def randomnumber(n):
   randlst = [random.choice(string.digits) for i in range(n)]
   return ''.join(randlst)

# get characters as random
def randomname(n):
   randlst = [random.choice(string.ascii_letters + string.digits) for i in range(n)]
   return ''.join(randlst)

# item as json -> string
def str_anitem(anitem):
    if not ('contract_info' in anitem):
       anitem['contract_info'] = ""

    return anitem['user_id'] + '\t' \
        + anitem['update_date_time']  + '\t' \
        + anitem['contract_info'] [0:20]

# display an item
def display_anitem(anitem):
    print(str_anitem(anitem))

# display times
def display_items(items):
    for it in items:
        print(str_anitem(it))

# array data -> json
def item_tok(user_id, update_date_time, dummy, contract_info):
    anitem = {
        'user_id':user_id,
        'update_date_time':update_date_time,
        'dummy':dummy,
        'contract_info':contract_info
    }
    return anitem

# make userid string
def gen_userid(i):
    
    num_ch=ord("z")-ord("a")+1
    userid=""
    i=i+(num_ch+1)*(num_ch+1)

    while i>0:
        userid = userid+chr_alph( i % num_ch )
        i = i // num_ch

    return userid

# make timestump
def strdatetime(i):
    dt=datetime.datetime.fromtimestamp(i)
    return re.sub('[- :]','',str(dt))

# main
def put_batch(num):
    try:

        table = DYNAMO.Table(TABLE_NAME)
        
        with table.batch_writer() as batch:
            for i in range(num):
                anitem = item_tok(gen_userid(i), strdatetime(i+random.randint(0,9999)), 'dummy',randomname(5000))
                display_items([anitem])
                batch.put_item(Item=anitem)
                
        '''LOGGER.info("Completed registration")'''
        print("complete!")
        return "end"
            
    except Exception as error:
        LOGGER.error(error)
        raise error

args = sys.argv

#put_batch(int(args[1]))

results = timeit.timeit(lambda: put_batch(int(args[1])), number=1)
print("exec time[sec]", results)
