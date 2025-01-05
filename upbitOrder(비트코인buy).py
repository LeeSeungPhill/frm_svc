import jwt
import hashlib
import os
import requests
import uuid
from urllib.parse import urlencode, unquote
from decimal import Decimal, ROUND_DOWN
import time
from dotenv import load_dotenv
load_dotenv()

# access_key = os.environ['UPBIT_ACCESS_KEY']
# secret_key = os.environ['UPBIT_SECRET_KEY']
# api_url = os.environ['UPBIT_API']
# access_key = os.getenv("UPBIT_ACCESS_KEY")
# secret_key = os.getenv("UPBIT_SECRET_KEY")
access_key = os.getenv("UPBIT_0825_ACCESS_KEY")
secret_key = os.getenv("UPBIT_0825_SECRET_KEY")

api_url = os.getenv("UPBIT_API")

def place_order(market, side, volume, price, ord_type="limit"):
    params= {
        'market': market,       # 마켓 ID
        'side': side,           # bid : 매수, ask : 매도
        'ord_type': ord_type,   # limit : 지정가 주문, price : 시장가 매수, market : 시장가 매도, best : 최유리 주문
        'price': price,         # 호가 매수
        'volume': volume        # 주문량
    }
    print(params)
    query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

    m = hashlib.sha512()
    m.update(query_string)
    query_hash = m.hexdigest()

    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
        'query_hash': query_hash,
        'query_hash_alg': 'SHA512',
    }

    jwt_token = jwt.encode(payload, secret_key)
    authorization = 'Bearer {}'.format(jwt_token)
    headers = {
    'Authorization': authorization,
    }

    # 주문 전송
    res = requests.post(api_url + '/v1/orders', json=params, headers=headers)
    print("result : ", res.json())
    return res.json()

def get_order(order_uuid):
    params = {"uuid": order_uuid}
    print("order_uuid : ",order_uuid)
    query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

    m = hashlib.sha512()
    m.update(query_string)
    query_hash = m.hexdigest()

    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
        'query_hash': query_hash,
        'query_hash_alg': 'SHA512',
    }

    jwt_token = jwt.encode(payload, secret_key)
    authorization = 'Bearer {}'.format(jwt_token)
    headers = {
        'Authorization': authorization,
    }
    # 주문 조회
    response = requests.get(api_url + "/v1/order", params=params, headers=headers)
    print("response : ", response.json())
    return response.json()

while True:
    # 잔고 조회
    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
    }

    jwt_token = jwt.encode(payload, secret_key)
    authorization = 'Bearer {}'.format(jwt_token)
    headers = {
    'Authorization': authorization,
    }
    res = requests.get(api_url + '/v1/accounts',headers=headers)
    # balance = float(res.json()[0]['balance']) # 가용 현금
    # balance = float(res.json()[0]['balance']) / 5 # 가용 현금 5분할
    balance = 2000000
    print("잔고 : ",int(balance))


    # 주문조회
    # order_status = get_order('1c60b526-a37f-4213-82a8-2d9abf79f6ee') # 주문 ID 
    # order_status = get_order('67910f26-48f1-489a-a7c4-4d11f6392d13') # 주문 ID 
    # print("주문 상태:", order_status['state'])
    # if order_status['state'] == "done" or order_status['state'] == "cancel":
    #     break

    # 호가 조회
    params1 = {
        "markets": "KRW-BTC",
        "level": 0
    }
    headers1 = {"accept": "application/json"}
    response = requests.get(api_url + '/v1/orderbook', headers=headers1, params=params1)
    print("params1 : ", params1)
    # 호가 매수
    # ask_price = response.json()[0]['orderbook_units'][0]['ask_price']
    # 지정가 매수
    ask_price = 141000000
    # 호가 매도
    bid_price = response.json()[0]['orderbook_units'][0]['bid_price']
    # 지정가 매도
    # bid_price = 4250
    print("ask_price : ",ask_price)
    print("bid_price : ",bid_price)

    # 수수료를 제외한 잔고 
    calculation = (int(balance) * Decimal('0.9995')).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
    # 호가 매도 기준 수수료를 제외한 잔고에 대한 매수 주문량
    volume = (calculation/ask_price).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)

    if calculation > 5000: # 수수료를 제외한 잔고가 5000보다 큰 경우
        print("order available balance : ",calculation)
        print("order available volume : ",volume)

        order_response = place_order(
            market="KRW-BTC",
            side="bid",         # 매수
            volume=str(volume), # 매수량
            price=ask_price,    # 매수가격
            ord_type="limit"    # 지정가 주문
        )
        
    #     # order_response = place_order(
    #     #     market="KRW-BTC",
    #     #     side="ask",         # 매도
    #     #     volume=str(volume), # 매도량
    #     #     price=bid_price,    # 매도가격
    #     #     ord_type="limit"    # 지정가 주문
    #     # )
        
        print("주문 응답:", order_response)
        
        if "uuid" in order_response:
            order_uuid  = order_response["uuid"]  # 주문 ID
            order_status = get_order(order_uuid)
            print("주문 상태:", order_status['state'])
            if order_status['state'] == "done" or order_status['state'] == "cancel":
                break
        else:
            print("주문 실패:", order_response)
            
    # else:
    #     print("balance not available")
    #     break

    time.sleep(15)
