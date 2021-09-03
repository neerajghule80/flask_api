from flask import Flask, render_template, request
import requests
import json
import pandas as pd
import mysql.connector
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import select, text
import pymysql
import numpy as np
from pathlib import Path
import logging
import databaseconfig as cfg
import csv
from os import path
import atexit
import urllib

dbcon = None
engine = None

class DBConnection:
    def __init__(self):
        self.host = cfg.mysql["host"]
        self.user = cfg.mysql["user"]
        self.password = cfg.mysql["password"]
        self.database = cfg.mysql["database"]

class DBConnection_pymysql(DBConnection):
    def __init__(self):
        super().__init__()
        global dbcon
        if not dbcon:
            try:
                dbcon = pymysql.connect(self.host,self.user, self.password, self.database)
            except pymysql.Error as e:
                return(None)
    
    def __del__(self):
        atexit.register(close_connection, self.dbcon)
     
    def close_connection(dbcon):
        self.dbcon.commit()
        self.dbcon.close()


class DBConnection_sqlalchemy(DBConnection):
    def __init__(self):
        super().__init__()
        global engine
        if not engine:
            try:
                engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}')
            except Exception:
                return(None)
    

    def __del__(self):
        atexit.register(close_connection, self.engine)
     
    def close_connection(engine):
        self.engine.commit()
        self.engine.close()
    

app = Flask(__name__)


@app.route('/')
def form():
    return render_template('form.html')

@app.route("/data", methods = ['POST', 'GET'])
def func_data():
    logging.basicConfig(filename='app.log', filemode='w', format='%(message)s')
    obj = DBConnection_pymysql()
    form_data = request.form
    operation = int(form_data["choice"])
    
    if operation not in range(1,6):
        logging.error("Invalid choice entered")
        return("Enter valid choice")

    if(operation == 1):
        logging.debug("choice entered : 1")
        return render_template('insert.html')
        
    elif (operation == 2):
        logging.debug("choice entered : 2")
        return render_template('date.html')

    elif (operation == 3):
        logging.debug("choice entered : 3")
        return render_template('difference.html')

    elif (operation == 4):
        logging.debug("choice entered : 4")
        return func_avg_difference()
        
    elif (operation == 5):
        logging.debug("choice entered : 5")
        return render_template('consecutive.html')
        

@app.route("/insert", methods = ['POST', 'GET'])
def func_insert():
    _data = request.form
    name = _data["symbol"]
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&apikey=NQCFKOVGZASY3EZ9&symbol="+name
    try:
        uResponse = requests.get(url)
    except requests.ConnectionError:
       return "Connection Error"  
    data = uResponse.json()

    if "Error Message" in data.keys():
        logging.error("Wrong symbol entered")
        return("Enter valid symbol")

    return func_upsert_in_sql(name, data)


@app.route("/json_to_df/<name>/<data>")
def func_json_to_df(name, data):

    _df = pd.DataFrame(columns = ["name","date","open","high","low","close","adjusted_close","volume","dividend_amount","split_coefficient"])
    _dates = list(data['Time Series (Daily)'])
    for _date in _dates:
        _input = data['Time Series (Daily)'][_date]
        _open = _input["1. open"]
        _high = _input["2. high"] 
        _low = _input["3. low"] 
        _close = _input["4. close"] 
        _adjusted_close = _input["5. adjusted close"]
        _volume = _input["6. volume"]
        _dividend_amount = _input["7. dividend amount"]
        _split_coefficient = _input["8. split coefficient"]

        _df.loc[len(_df.index)] = [name, datetime.strptime(_date, r'%Y-%m-%d'), _open, _high, _low, _close, _adjusted_close, _volume, _dividend_amount, _split_coefficient]
    
    return _df

@app.route("/dropDuplicateEntries")
def func_drop_existing_entries_in_db(df,dbcon):
    logging.debug("Check duplicate entries in input")
    SQL_Query = pd.read_sql_query('''select * from stocks''', dbcon)
    table_values = pd.DataFrame(SQL_Query, columns=["name","date","open","high","low","close","adjusted_close","volume","dividend_amount","split_coefficient"])
    
    table_values['date'] = table_values['date'].astype(str)
    df['date'] = df['date'].astype(str)
    
    common = pd.merge(df,table_values,on=['name','date'],how='left',indicator='Exist')
    common['Exist'] = np.where(common.Exist == 'both', True, False)
    ret = common[common['Exist'] == False]
    
    if ret.empty:
        return ret#.to_string()

    if ret.isnull().values.any():
        ret = ret.dropna(axis=1)
        
    if 'Exist' in ret:
        ret.drop(columns={'Exist'}, inplace=True)
    ret.columns = ["name","date","open","high","low","close","adjusted_close","volume","dividend_amount","split_coefficient"]
    return ret#.to_string()


@app.route("/jsonFormat/")#<name>/<data>")
def func_upsert_in_sql(name, data):
    
    df = func_json_to_df(name, data)
    
    try:
        obj1 = DBConnection_sqlalchemy()
        logging.debug("Connected to database by sqlalchemy")

        df = func_drop_existing_entries_in_db(df,dbcon)
        if df.empty:
            logging.warning("Duplicate entries present in the input")
            return("Entries already exists in the table")
        else:
            df.to_sql('stocks',con = engine,if_exists='append', index=False)
            logging.debug("Entries stored in database")
    except mysql.connector.Error as err:
        return("MySQL Connection Error {}".format(err))
    

    base = Path('json_file')
    jsonpath = base / (name + ".json")
    base.mkdir(exist_ok=True)
    jsonpath.write_text(json.dumps(data))
    logging.debug("json file is created")
    return("Successful !")


@app.route("/specificDay", methods = ['POST', 'GET'])
def func_specific_day_stats():
    _date = request.form
    if _date['symbol'] == '':
        return("Symbol is required")
    if _date['date'] == '':
        return("Date is required")
    

    _query = "select * from stocks where name = \'{}\' and date = \'{}\' ;".format(_date['symbol'],_date['date'])
    cur = dbcon.cursor()
    cur.execute(_query)
    results =cur.fetchall()
    if len(results) == 0:
        logging.error("Incorrect date or symbol entered")
        return("Check the symbol/date")
    logging.debug("Data for specific day is fetched")
    json_dict = {}
    _keys = ["name","date","open","high","low","close","adjusted_close","volume","dividend_amount","split_coefficient"]
    _key_index = 0
    with open('specific_day.csv','a', newline = '') as fd:
        lst = []
        for res in results:
            for element in res:
                lst.append(element)
                json_dict[_keys[_key_index]] = str(element)
                _key_index = _key_index + 1
        writer = csv.writer(fd)
        writer.writerow(lst)

#    if path.exists("specific_day.json"):
#        with open('specific_day.json','r+') as json_file:
#            data = json.load(json_file)
#            data.update(json_dict)
#            json_file.seek(0)
#            json.dump(data, json_file)
#    else:
#        with open('specific_day.json','w') as json_file:
#            json.dump(json_dict, json_file)    
      

    return render_template("display.html", data=results)
    

@app.route("/difference", methods = ['POST', 'GET'])
def func_difference_closing_opening():
    _data = request.form
    if _data['symbol'] == '':
        return("Symbol is required")

    _query = "select name, date, (close - open) as difference from stocks where name = \'{}\';".format(_data['symbol'])
    cur = dbcon.cursor()
    cur.execute(_query)
    results =cur.fetchall()
    if len(results) == 0:
        logging.error("Incorrect symbol entered")
        return("Check the symbol")
    
    logging.debug("Difference of closing and opening is fetched")
    return render_template("display_diff.html", data=results)


@app.route("/avg_diff", methods = ['POST', 'GET'])
def func_avg_difference():
    _query = "select date, avg(close-open) as average from stocks group by date;"
    cur = dbcon.cursor()
    cur.execute(_query)
    results =cur.fetchall()
    logging.debug("Average difference of all companies across a date is fetched")
    return render_template("display_avg.html", data=results)

@app.route("/consecutive", methods = ['POST', 'GET'])
def func_consecutive_positives():
    _data = request.form

    if _data['symbol'] == '':
        return("Symbol is required")
    
    _query = "select name, date, (close - open) as difference from stocks where name = \'{}\';".format(_data['symbol'])
    cur = dbcon.cursor()
    cur.execute(_query)
    results =cur.fetchall()
    
    if len(results) == 0:
        logging.error("Incorrect symbol entered")
        return("Check the symbol")
    
    max = 0
    count = 0
    for result in results:
        if float(result[2]) > 0:
            count = count + 1
        else:
            if count > max:
                max = count
            count = 0
    
    return ("Maximum number of consecutive days having closing value greater than opening value is {}".format(max))
    

if __name__ == "__main__":
    app.run(debug = True)