import json
from pprint import pprint
from urllib import request
from bs4 import BeautifulSoup

#图片的例子(与文字相同)

# url = "http://www.toutiao.com/search_content/?offset=20&format=json&keyword=%E8%A1%97%E6%8B%8D&autoload=true&count=20&_=1480675595492"
# url = "http://www.toutiao.com/search_content/?offset=40&format=json&keyword=CBA&autoload=true&count=20&cur_tab=1"
url = "http://www.toutiao.com/a6392514815999049986/"

# with request.urlopen(url) as res:
#     d = json.loads(res.read().decode())
#     print(d)

# 通过 get(‘article_url’) 获取到文章的 URL

# with request.urlopen(url) as res:
#     d = json.loads(res.read().decode())
#     d = d.get('data')
#     urls = [article.get('article_url') for article in d if article.get('article_url')]
#     pprint(urls)
#     pprint(d)

# with request.urlopen(url) as res:
#     soup = BeautifulSoup(res.read().decode(errors='ignore'), 'html.parser')
#     article_main = soup.find('div', id='article-main')
#     print(article_main)
#     photo_list = [photo.get('src') for photo in article_main.find_all('img') if photo.get('src')]
#     print(photo_list)


# photo_url = "http://p9.pstatp.com/large/111200020f54729cd558"
# photo_name = photo_url.rsplit('/', 1)[-1] + '.jpg'
# with request.urlopen(photo_url) as res, open(photo_name, 'wb') as f:
#     f.write(res.read())
