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

def get_open_order(market, states):
    params = {
	    'market': market,                # 마켓 ID
        'states': states,                # 'wait', 'watch'
    }
    # print("params : ",params)
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
    # 체결 대기 주문 조회
    response = requests.get(api_url + "/v1/orders/open", params=params, headers=headers)
    # print("response : ", response.json())
    return response.json()

def cancel_order(order_uuid):
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
    # 주문 취소 접수
    response = requests.delete(api_url + '/v1/order', params=params, headers=headers)
    # print("response : ", response.json())
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
    accounts = res.json()
    print(f"length : {len(accounts)}")
    print("잔고 : ",accounts[0])
    balance = float(accounts[0]['balance']) # 가용 현금
    # balance = float(res.json()[0]['balance']) / 5 # 가용 현금 5분할
    # balance = 2000000
    # print("잔고 : ",int(balance))

    sell_target_amount = 0
    sell_target_avg_price = 0

    for i, item in enumerate(accounts):
        sell_target_volume = float(item['balance'])   # 보유수량
        sell_target_avg_price = float(item['avg_buy_price'])  # 평균단가    
        sell_target_amt = float(sell_target_volume * sell_target_avg_price)  # 보유금액  
        print(item['currency']+" : sell_target_volume : ",sell_target_volume)
        print(item['currency']+" : sell_target_avg_price : ",sell_target_avg_price)
        print(item['currency']+" : sell_target_amt : ",sell_target_amt)

    # 체결 대기 주문 조회
    open_order_BTC = get_open_order('KRW-BTC', 'wait')
    open_order_ETH = get_open_order('KRW-ETH', 'wait')
    open_order_SOL = get_open_order('KRW-SOL', 'wait')
    print("open_order BTC : ",open_order_BTC)
    print("open_order ETH : ",open_order_ETH)
    print("open_order SOL : ",open_order_SOL)

    # 주문조회
    # order_status = get_order('96a222b7-1b05-49fb-935a-8d72175e7634') # 주문 ID 
    # order_status = get_order('29d0c07d-32c9-4f7a-a4d0-7c3309dec976') # 주문 ID 
    # print("주문 상태:", order_status['state'])
    # if order_status['state'] == "done" or order_status['state'] == "cancel":
    #     break

    # 주문 취소
    # order_status = cancel_order('20767cac-d909-4a7a-903a-4749ad825292') # 주문 ID 
    # order_status = cancel_order('29d0c07d-32c9-4f7a-a4d0-7c3309dec976') # 주문 ID
    # print("주문 상태:", order_status['state'])
    # if order_status['state'] == "done" or order_status['state'] == "cancel":
    #     break

    # 호가 조회
    params1 = {
        "markets": "KRW-ETH",
        "level": 0
    }
    headers1 = {"accept": "application/json"}
    response = requests.get(api_url + '/v1/orderbook', headers=headers1, params=params1)

    # 호가 매도
    # bid_price = float(response.json()[0]['orderbook_units'][0]['bid_price'])
    # 지정가 매도
    bid_price = 6000000
    print("bid_price : ",bid_price)

    # 보유수량 대비 매도 수량 설정
    volume = sell_target_amount / 2

    if bid_price > sell_target_avg_price: # 평균단가보다 호가 매도가 큰 경우
        print("order available volume : ",volume)

        # order_response = place_order(
        #     market="KRW-ETH",
        #     side="ask",         # 매도
        #     volume=str(volume), # 매도량
        #     price=bid_price,    # 매도가격
        #     ord_type="limit"    # 지정가 주문
        # )
        
        # print("주문 응답:", order_response)
        
        # if "uuid" in order_response:
        #     order_uuid  = order_response["uuid"]  # 주문 ID
        #     order_status = get_order(order_uuid)
        #     print("주문 상태:", order_status['state'])
        #     if order_status['state'] == "done" or order_status['state'] == "cancel":
        #         break
        # else:
        #     print("주문 실패:", order_response)
            
    else:
        print("sell not available")
        break

    time.sleep(15)
