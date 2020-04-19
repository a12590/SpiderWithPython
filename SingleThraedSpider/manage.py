# coding:utf8

from app import app

from app.modules.home import home as home_blueprint
from app.modules.userManage import userManage as userManage_blueprint
from app.modules.api import api as api_blueprint
from app.modules.job import job as job_blueprint
from app.modules.model import model as model_blueprint
from app.modules.list import list as list_blueprint
from app.modules.data import data as data_blueprint

app.register_blueprint(home_blueprint)
app.register_blueprint(userManage_blueprint, url_prefix="/userManage")
app.register_blueprint(api_blueprint, url_prefix="/api")
app.register_blueprint(job_blueprint, url_prefix="/job")
app.register_blueprint(model_blueprint, url_prefix="/model")
app.register_blueprint(list_blueprint, url_prefix="/list")
app.register_blueprint(data_blueprint, url_prefix="/data")

if __name__ == "__main__":
    # app.run(host='10.108.211.136', port=15050)
    app.run(host='localhost', port=15050)
