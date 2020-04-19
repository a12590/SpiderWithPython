#!/usr/local/miniconda2/bin/python
# _*_ coding: utf-8 _*_
"""
@author: lhh
@time  : 2018-05-06 下午20:06
"""
from cryptography.fernet import Fernet


def cryptography_encode(message,key):
    """
    使用cryptography包对明文进行加密
    :param message:
    :return:
    """
    f = Fernet(key)
    token = f.encrypt(message)
    return token

def get_key():
    key = Fernet.generate_key()
    return key
def cryptography_decode(token, key):
    """
    对密文进行解密
    :param token:
    :param key:
    :return:
    """
    f = Fernet(key)
    return f.decrypt(token)


if __name__ == '__main__':
    key = get_key()
    token = cryptography_encode("刘行行", key)
    print type("刘行行")
    print type(key)
    print key
    print token

    print cryptography_decode(token, key)