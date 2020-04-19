# coding:utf8
from app import db

# 用户

class User(db.Model):
    __tablename__ = "test_app_user"
    id = db.Column(db.INT, primary_key=True)
    username = db.Column(db.VARCHAR(80), unique=True)
    email = db.Column(db.VARCHAR(120), unique=True)

    def __init__(self, username, email):
        self.username = username
        self.email = email

    def __repr__(self):
        return '<User %r>' % self.username


if __name__ == "__main__":
    user = User(
        username="超级管理员",
        email="12345678@qq.com"
    )

    db.session.add(user)
    db.session.commit()
