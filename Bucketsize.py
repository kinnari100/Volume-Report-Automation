import pandas as pd
import psycopg2
import json

def connect_to_db(db_name):
    db_creds_file = open('/Users/kinnaripatel/Desktop/k/db_credential/db_creds.json')
    db_file = json.load(db_creds_file)
    host = db_file['db_creds']['host']
    username = db_file['db_creds']['username']
    passw = db_file['db_creds']['password']
    port = db_file['db_creds']['port']
    connection = psycopg2.connect(host=host,port=port,database=db_name, user=username, password=passw) 
    return connection

def nodule_size(size) :
    if float(size) >= 0 and float(size) < 6 :
        return "< 6 cm"
    elif float(size) >= 6 and float(size) <=8:
        return "6-8 cm"
    elif float(size) > 8 and float(size)<10:
        return "8-10 cm"
    elif float(size) > 10 :
        return "> 10 cm"
    else:
        return "negative"

def thyroid_size(size) :

    if float(size) == 0 :
        return "1"
    elif float(size) >= 0.5 and float(size) <= 0.9 :
        return "0.5-0.9"
    elif float(size) >= 1.0 and float(size)<=1.4:
        return "1.0-1.4"
    elif float(size) >= 1.5 and float(size)<=2.4:
        return "1.5-2.4"
    elif float(size) >= 2.5 and float(size)<=5:
        return "2.5-5"
    elif float(size) > 5:
        return ">5"
    else:
        return "negative"

def liver_size(size) :

    if float(size) >= 0.1 and float(size) <= 0.9 :
        return "<1cm"
    elif float(size) >= 1.0 and float(size)<=1.5:
        return "1.0-1.5"
    elif float(size) > 1.5:
        return ">1.5cm"
    else:
        return "negative"

def taa_size(size) :
   
    if float(size) >= 0 and float(size) <= 2.5 :
        return "<2.5"
    elif float(size) >2.5 and float(size)<=3:
        return "2.5-3"
    elif float(size) >3 and float(size)<=4:
        return "3-4"
    elif float(size) >=4 and float(size)<=5:
        return "4-5"
    elif float(size) > 5:
        return ">5"
    else:
        return "negative"

def pancreas_size(size) :
   
    if float(size) >= 0 and float(size) <= 1.5 :
        return "<= 1.5cm"
    elif float(size) >1.5 and float(size)<=2.5:
        return "1.5-2.5"
    elif float(size) >2.5 and float(size)<=4.0:
        return "2.5-4"
    elif float(size) > 4:
        return ">4"
    else:
        return "negative"

def aaa1_size(size) :
   
    if float(size) == 999:
        return "s/p"
    elif float(size) >= 0 and float(size) <= 2.5 :
        return "<2.5"
    elif float(size) >2.5 and float(size)<=3:
        return "2.5-3"
    elif float(size) >3 and float(size)<=4:
        return "3-4"
    elif float(size) >=4 and float(size)<=5:
        return "4-5"
    elif float(size) > 5:
        return ">5"
    else:
        return "negative"





