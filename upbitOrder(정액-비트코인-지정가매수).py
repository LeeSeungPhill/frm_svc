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

def division_buy(buy_division_amt, buy_count, buy_price, cut_price, goal_price):

    # 수수료를 제외한 잔고 
    buy_division_amt_except_fee = (int(buy_division_amt) * Decimal('0.9995')).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)

    # 매수물량
    buy_vol = (buy_division_amt_except_fee / buy_price).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
    # 손절금액
    cut_amt = int(buy_division_amt_except_fee * (100 - (cut_price / buy_price) * 100) / 100)
    # 손절율
    cut_rate = (100 - (cut_price / buy_price) * 100).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
    # 목표금액
    goal_amt = int(buy_vol * goal_price) - buy_division_amt_except_fee
    # 목표율
    goal_rate = ((100 - (goal_price / buy_price) * 100) * -1).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
    # 안전마진수량
    margin_vol = (cut_amt / (goal_price - cut_price)).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)

    if buy_division_amt_except_fee > 5000: # 수수료를 제외한 잔고가 5000보다 큰 경우
        print("order available balance : ",buy_division_amt_except_fee)
        print("order volume : ",buy_vol)

        order_response = place_order(
            market="KRW-BTC",
            side="bid",             # 매수
            volume=str(buy_vol),    # 매수량
            price=buy_price,        # 매수가격
            ord_type="limit"        # 지정가 주문
        )
        
        print("주문 응답:", order_response)
        
        if "uuid" in order_response:
            order_uuid  = order_response["uuid"]  # 주문 ID
            order_status = get_order(order_uuid)
            print("주문 상태:", order_status['state'])
        else:
            print("주문 실패:", order_response)
            

while True:

    # 정액매수 -> 회차별 일치된 매수 금액 
    # - 입력 : 분할 투자금액, 회차, 매수가, 손절가, 목표가
    # - 리턴 : 투자수량, 손실금액, 목표금액, 안전마진수량(손실금액 / (목표가-손절가) -> 목표가에서 안전마진수량 매도하고, 나머지 물량 손절가에서 매도시 손실금액 = 0), 손절율, 목표율

    buy_division_amt = 2000000
    buy_count = 1
    buy_price = 141000000
    cut_price = 134000000
    goal_price = 157000000

    division_buy(buy_division_amt, buy_count, buy_price, cut_price, goal_price)

    time.sleep(15)
