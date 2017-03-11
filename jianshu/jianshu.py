#!/usr/bin/env python
# encoding: utf8
#
#  如何解决得到的url中间并非都是正确的形式的问题

from bs4 import BeautifulSoup
import requests

#得到当前url下的更多的url
def get_urls(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")
    soups = soup.find_all(target="_blank")
    ff = []
    urls = {}
    for soup0 in soups:
        ff.append(soup0.get('href'))
    for i in range(len(ff)):
        urls = 'http://www.jianshu.com' + str(ff)
    print(urls)
    return urls


# 解析html,以得到目标对象
def get_mywilling(url):
    url_open(url)
    html = open('jianshu.html', 'r')
    soup = BeautifulSoup(html, "html.parser")
    return soup.find_all('p')


def url_open(url):
    urls = get_urls(url)
    for url in urls:
        with open('jianshu.html', 'ab') as file:
            html = requests.get(url).text
            file.write(html)


# 将目标对象排版，储存
def save_mywilling():
    mywilling = get_mywilling(url)
    with open('jianshu.txt', 'ab') as file:
        goal = str(mywilling).encode('utf-8')
        file.write(goal)
        file.close()
        print('成功收集一次！')


# 主函数
def main():
    print('爬虫启动中')
    save_mywilling()


# 模块测试
if __name__ == '__main__':
    url = 'http://www.jianshu.com/'
    main()