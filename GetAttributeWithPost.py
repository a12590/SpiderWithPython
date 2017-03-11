# -*-coding:utf8
# 使用post方式请求数据
# -*-

import requests
import re

url = ''

# 注意这里的page后面跟的数字需要放到引号里面。
data = {
    'entities_only': 'true',
    'page': '2'
}
html_post = requests.post(url, data=data)
title = re.findall('"">(.*?)</div>', html_post.text, re.S)
for each in title:
    print(each)