import time
from pprint import pprint as pp
from polygon import WebSocketClient, STOCKS_CLUSTER
from snapshots.redis_helper import set_kv, get_value, dbsize, keys, memory_usage, flushdb, incr_key, delete_key
import json
import string
import boto3
import random
import pickle, datetime
from signal import signal, SIGINT, SIGKILL
from sys import exit
import pytz
import os

la = pytz.timezone("America/Los_Angeles")
letters = string.ascii_lowercase


sns_resource = boto3.Session(profile_name="tf").resource("sns", region_name="us-east-1")
now = datetime.datetime.now()  # current date and time
topic = sns_resource.Topic('arn:aws:sns:us-east-1:562871273327:telegram-notifications')


def my_custom_process_message(messages):
    messages = json.loads( messages)
    for message in messages:
        # print(message)
        key_name = message['ev'] + "_" + message['sym'] + "_" + str(message['e'])
        new_message = {
            'n': int(message['v'] / message['z']),
            'v': message['v'],
            'c': message['c'],
            'sym': message['sym']
        }
        # Increment the key for the number intervals with trades
        incr_key('mins_' + message['sym'],1)
        redis_set = set_kv(key_name, json.dumps(new_message), 60 * 60 * 24 * 7)  # save for 7 days
        redis_set = set_kv('new_' + key_name, json.dumps(new_message), 50)  # save for 50 seconds

def my_custom_error_handler(ws, error):
    print("this is my custom error handler", error)

def my_custom_close_handler(ws):
    connect()
    print("this is my custom close handler")

def on_exit_handler(signal, frame, polygon_client):
    polygon_client.close_connection()
    exit(0)
    a=1

def inspect(): # cada 5 seg
    current_time = datetime.datetime.now(la)
    market_open = False
    if current_time.weekday() <= 4:
        if 1 <= current_time.hour <= 16:
            market_open = True
    if not market_open:
        return
    
    if current_time.hour <= 7 and current_time.minute < 30:
        summary_status = pickle.load(open('pre_market.p', "rb"))
    elif current_time.hour < 13:
        summary_status = pickle.load(open('open_market.p', "rb"))
    elif current_time.hour < 17:
        summary_status = pickle.load(open('after_hours.p', "rb"))
    try:
        all_keys = keys('new_AM_*')
        for key in all_keys:
            tmp = key.split("_")
            if tmp[2] in summary_status:
                if get_value(key) is None:
                    continue
                val = json.loads(get_value(key))
                s = summary_status[tmp[2]]
                try:
                    val['sn'] = (val['n'] - s['mn']) / s['sn']
                    val['sv'] = (val['v'] - s['mv']) / s['sv']
                except:
                    print(tmp[2])
                    pass
                if (val['sn'] > 10 and val['sv'] > 10) and val['n'] > 10 and val['c'] < 15:
                    if get_value(tmp[2]) is not None:
                        continue
                    if val['c'] < 20:
                        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
                        topic.publish(Message="volume alert $" + tmp[2] + " at " + date_time)
                        os.system('say "alert"')
                    set_kv(tmp[2], 'True', 600)  # save for 10 minutes
                    d = {
                        'key': key,
                        'tmp': tmp,
                        'val': val,
                        's': summary_status[tmp[2]],
                        'n': val['sn'],
                        'v': val['sv']
                    }
                    redis_set = set_kv('stats_' + key, json.dumps(val), 50)  # save for 50 seconds
                    delete_key(key)
                    print(tmp[2]+"\t"+str(round(val['c'],2))+"\t"+str(val['n'])+"\t"+str(val['v'])+"\t"+str(round(val['sn'],2))+"\t"+str(round(val['sv'],2))+"\t"+str(round(s['mn'],2))+"\t"+str(round(s['mv'],2)))

        # print(len(all_keys))
    except Exception as e:
        print(e)

def connect():
    #flushdb()
    key = 'svgXZtGc4bR59akMdUPaeeaml3mX58xQ'
    my_client = WebSocketClient(STOCKS_CLUSTER, key, my_custom_process_message)
    my_client.run_async()
    my_client.subscribe("AM.*")
    signal(SIGINT, lambda signal, frame: on_exit_handler(signal, frame, my_client))
    try:
        signal(SIGKILL, lambda signal, frame: on_exit_handler(signal, frame, my_client))
    except:
        pass

def main():
    connect()
    num_keys = 0
    print("ticker\tprice\tn_trades\tvolume\tsig_n\tsig_v\tmean_n\tmean_v")
    while True:
        # print('running')
        all_keys = keys('AM')
        if num_keys != len(all_keys):
            num_keys = len(all_keys)
        # print("keys")
        # print(len(all_keys))
        inspect()
        time.sleep(5)

    my_client.close_connection()


if __name__ == "__main__":
    main()