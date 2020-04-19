# coding:utf8
from __future__ import absolute_import, division, print_function

import os
import sys

module_path = os.path.abspath(os.path.join('../'))
sys.path.append(module_path)
reload(sys)
from flask import Flask

from flask_sqlalchemy import SQLAlchemy

from app.conf.db_config import DB_Configure

app = Flask(__name__)

app.config['JSON_AS_ASCII'] = False

app.debug = True

app.config["SQLALCHEMY_DATABASE_URI"] = "mysql://{}:{}@{}:3306/{}".format(DB_Configure.mysql_user,
                                                                          DB_Configure.mysql_password,
                                                                          DB_Configure.mysql_host,
                                                                          DB_Configure.mysql_database)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True

db = SQLAlchemy(app)
