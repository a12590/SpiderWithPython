# _*_ coding: utf-8 _*_
from flask import Blueprint

userManage = Blueprint("userManage",__name__,)

import app.modules.userManage.views
