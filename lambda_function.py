import json
import os
import urllib
from datetime import datetime as dt

import requests
import redis
from kiteconnect import KiteConnect

kite = KiteConnect(api_key="<YOUR-API-KEY>", debug=True)
redis_con = redis.Redis(host="<REDIS-HOST>", port=6379, db=11, charset="utf-8", decode_responses=True, password="<PASSWORD>")

def set_token(request_token):

    data = kite.generate_session(request_token, api_secret="<ZERODHA-API-SECRET>")
    redis_con.set("my_token", data["access_token"])
    return data["access_token"]

def place_order(received_data, txn_type="BUY"):

    stock_id = [urllib.parse.quote_plus(ts) for ts in received_data['stocks'].split(',')]
    place_at = [float(o) for o in received_data['trigger_prices'].split(',')]
    stop_at, book_at = zip(*[(round(_ - _*0.01, 1), round(_ + _*0.01, 1)) for _ in place_at])
    
    if txn_type == "SELL":
        stop_at, book_at = book_at, stop_at

    qty_to_buy = [int(500/abs(order_price - stop_loss)) for order_price, stop_loss in zip(place_at, stop_at)]
    kite.set_access_token(redis_con.get("my_token"))

    for tradingsymbol, quantity, execute_at, sl, tg in zip(stock_id, qty_to_buy, place_at, stop_at, book_at):
        order_id = kite.place_order(tradingsymbol=tradingsymbol, exchange="NSE", transaction_type=txn_type, quantity=quantity,
        order_type="LIMIT", price=execute_at, trigger_price=sl, product="MIS", validity="DAY", disclosed_quantity=0, 
        squareoff=0, stoploss=0, trailing_stoploss=0, variety='amo')

def lambda_handler(event, context):

    query_dict = event.get('queryStringParameters', {})
    if event.get('rawPath') == '/set-token':
        return {'access_token': set_token(query_dict.get('request_token')), 'msg': 'Token was set successfully.'}
        
    if event.get('rawPath') == '/webhook':
        received_data = json.loads(event['body'])
        
        if received_data['scan_url'] == "near-day-high-108":
            place_order(received_data, txn_type="BUY")
