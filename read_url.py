import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
   
class questions(db.Model):
    __tablename__="questions"
    qid = db.Column(db.Integer, primary_key=True)
    subject =db.Column(db.String, nullable=False)
    question =db.Column(db.String, nullable=False)
    option1 = db.Column(db.String, nullable=True)
    option2 = db.Column(db.String, nullable=True)
    option3 = db.Column(db.String, nullable=True)
    option4 = db.Column(db.String, nullable=True)
    answer = db.Column(db.Integer, nullable=True)
    bcol = db.Column(db.String, nullable=True)   

