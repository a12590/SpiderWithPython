# _*_ coding: utf-8 _*_
from . import userManage
from flask import Flask, jsonify, request, Response
from app.modules.userManage.models import User
from app import db
import json

import sys

reload(sys)
sys.setdefaultencoding('utf8')


@userManage.route("/")
def index():
    return "<h1 style='color:green'> this is userManage</h1>"


@userManage.route("/addUser", methods=["GET", "POST"])
def addUser():
    """
    添加用户
    :return:returnModel:
    """
    returnModel = {}
    username = request.args.get("username")
    email = request.args.get("email")
    user = User(
        username=username,
        email=email
    )
    try:
        db.session.add(user)
        db.session.commit()
        returnModel["result"] = True
    except Exception as e:
        returnModel["result"] = False
        returnModel["reason"] = e

    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


@userManage.route("/deleteUser/<int:id>/", methods=["GET"])
def deleteUser(id=None):
    """
    根据用户id删除用户
    :param id:
    :return:returnModel:
    """
    returnModel = {}
    print "================================{}".format(id)
    try:
        user = User.query.filter_by(id=id).first()
        db.session.delete(user)
        db.session.commit()
        returnModel["result"] = True
    except Exception as e:
        returnModel["result"] = False
        returnModel["reason"] = e

    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


@userManage.route("/updateUserById", methods=["GET"])
def updateUserById():
    """
    根据用户ｉd更新用户
    :return:returnModel:
    """
    returnModel = {}
    id = request.args.get("id")
    email = request.args.get("email")
    try:
        User.query.filter_by(id=id).update({'email': email})
        db.session.commit()
        returnModel["result"] = True
    except Exception as e:
        returnModel["result"] = False
        returnModel["reason"] = e

    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


@userManage.route("/findUserByName/<string:username>/", methods=["GET"])
def findUserByName(username=None):
    """
    根据用户名查找用户
    :param username:
    :return:returnModel:
    """
    returnModel = {}
    try:
        user = User.query.filter_by(username=username).first()

        def obj_2_json(obj):
            return {
                "id": obj.id,
                "username": obj.username,
                "email": obj.email
            }

        returnModel["result"] = True
        returnModel["datum"] = json.dumps(user, default=obj_2_json)
    except Exception as e:
        returnModel["result"] = False
        returnModel["reason"] = e

    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
