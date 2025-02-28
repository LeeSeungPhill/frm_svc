import jwt
import hashlib
import os
import requests
import uuid
from urllib.parse import urlencode, unquote
from dotenv import load_dotenv
load_dotenv()
from decimal import Decimal, ROUND_DOWN
import time
import psycopg2
from datetime import datetime, timedelta
import schedule

api_url = os.getenv("UPBIT_API")

# 데이터베이스 연결 정보
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "localhost"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

def get_order(access_key, secret_key, order_uuid):
    params = {"uuid": order_uuid}
    # print("order_uuid : ",order_uuid)
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
    # print("response : ", response.json())
    return response.json()

def get_open_order(access_key, secret_key, prd_nm, state):

    params = {
        'market': "KRW-"+prd_nm,
        'state': state
    }
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

    # 체결대기 주문 조회
    response = requests.get(api_url + '/v1/orders/open', params=params, headers=headers)
    # print("response : ", response.json())
    return response.json()


def analyze_data(user, market, trend_type):
    # 감시할 코인
    chk_params = ["BTC","XRP","ETH","ONDO","STX","SOL","SUI","XLM","HBAR","ADA","LINK","RENDER"]
    
    try:
        
        # PostgreSQL 데이터베이스에 연결
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        cur01 = conn.cursor()
        
        user_id = "TRADE_AUTO"

        # 고객명에 의한 고객정보 조회
        query1 = "SELECT cust_num, cust_nm, market_name, acct_no, access_key, secret_key, access_token, token_publ_date FROM cust_mng WHERE cust_nm = %s AND market_name = %s"
        cur01.execute(query1, (user, market))  
        result_01 = cur01.fetchall()

        cust_info = {}
        if result_01:
            for idx, result in enumerate(result_01):
                cust_info = {
                                "cust_num": result[0],
                                "cust_nm": result[1],
                                "market_name": result[2],
                                "acct_no": result[3],
                                "access_key": result[4],
                                "secret_key": result[5]
                            } 

        if len(cust_info) > 0:
        
            for i in chk_params:
                
                cur02 = conn.cursor()
                chk_ord_list = []
                
                # 대기 상태 주문관리정보 조회
                SELECT_OPEN_ORDER_INFO = """
                    SELECT id, ord_no, prd_nm, ord_state, executed_vol, remaining_vol
                    FROM trade_mng
                    WHERE cust_num = %s AND market_name = %s AND prd_nm = %s AND ord_state IN ('wait', 'watch')
                """
                cur02.execute(SELECT_OPEN_ORDER_INFO, (cust_info['cust_num'], cust_info['market_name'], "KRW-"+i))  
                chk_ord_list = cur02.fetchall()
                
                if len(chk_ord_list) > 0:
                    for chk_ord in chk_ord_list :
                        
                        # 주문 조회
                        order_status = get_order(cust_info['access_key'], cust_info['secret_key'], chk_ord[1])
                        print(order_status)
                        ord_state = order_status['state']

                        # 체결완료 상태인 경우
                        if ord_state == 'done':
                            if chk_ord[1] == order_status['uuid']:
                                    
                                cur06 = conn.cursor()
                                upd_param2 = (
                                    datetime.fromisoformat(order_status['trades'][0]['created_at']).strftime("%Y%m%d%H%M%S"),
                                    order_status['trades'][0]['uuid'],
                                    chk_ord[1],
                                    order_status['state'],
                                    Decimal(order_status['executed_volume']),
                                    Decimal(order_status['remaining_volume']),
                                    user_id,   # chgr_id
                                    datetime.now(),  # chg_date
                                    chk_ord[0]
                                )
                                
                                # 주문관리정보 변경 처리
                                update2 = """UPDATE trade_mng SET 
                                                ord_dtm = %s,
                                                ord_no = %s,
                                                orgn_ord_no = %s,
                                                ord_state = %s,
                                                executed_vol = %s, 
                                                remaining_vol = %s,
                                                chgr_id = %s,
                                                chg_date = %s
                                            WHERE id = %s
                                            AND ord_state = 'wait'
                                        """
                                cur06.execute(update2, upd_param2)
                                conn.commit()
                                cur06.close()

                        # 취소 상태인 경우
                        elif ord_state == 'cancel':
                            if chk_ord[1] == order_status['uuid']:

                                cur06 = conn.cursor()
                                upd_param2 = (
                                    order_status['state'],
                                    Decimal(order_status['executed_volume']),
                                    Decimal(order_status['remaining_volume']),
                                    user_id,   # chgr_id
                                    datetime.now(),  # chg_date
                                    chk_ord[9]
                                )
                                
                                # 주문관리정보 변경 처리
                                update2 = """UPDATE trade_mng SET 
                                                ord_state = %s,
                                                executed_vol = %s, 
                                                remaining_vol = %s,
                                                chgr_id = %s,
                                                chg_date = %s
                                            WHERE id = %s
                                            AND ord_state = 'wait'
                                        """
                                cur06.execute(update2, upd_param2)
                                conn.commit()
                                cur06.close()

                        else:
                            raw_order_list = []
                            # 체결대기 주문 조회
                            raw_order_list = get_open_order(cust_info['access_key'], cust_info['secret_key'], i, ord_state)

                            if len(raw_order_list) > 0:
                                for item in raw_order_list:
                                    if chk_ord[1] == item['uuid']:

                                        if chk_ord[5] != Decimal(item['remaining_volume']) or chk_ord[4] != Decimal(item['executed_volume']):

                                            cur06 = conn.cursor()
                                            upd_param2 = (
                                                item['state'],
                                                Decimal(item['executed_volume']),
                                                Decimal(item['remaining_volume']),
                                                user_id,   # chgr_id
                                                datetime.now(),  # chg_date
                                                chk_ord[0]
                                            )
                                            
                                            # 주문관리정보 변경 처리
                                            update2 = """UPDATE trade_mng SET 
                                                            ord_state = %s,
                                                            executed_vol = %s, 
                                                            remaining_vol = %s,
                                                            chgr_id = %s,
                                                            chg_date = %s
                                                        WHERE id = %s
                                                        AND ord_state = 'wait'
                                                    """
                                            cur06.execute(update2, upd_param2)
                                            conn.commit()
                                            cur06.close()
                
                
                else:
                    raw_order_list = []
                    # 체결대기 주문 조회
                    raw_order_list = get_open_order(cust_info['access_key'], cust_info['secret_key'], i, 'wait')
                    
                    if len(raw_order_list) > 0:
                        count = 0
                        cur031 = conn.cursor()
                        result_31 = []
                        # 매매신호정보 조회
                        query31 = "SELECT id, tr_dtm, tr_price, tr_volume, support_price, regist_price FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'S' AND tr_state = '02' order by tr_dtm desc LIMIT 1"
                        cur031.execute(query31, (i+"/KRW", ))  
                        result_31 = cur031.fetchone()
                    
                        for item in raw_order_list:
                            count = count + 1
                            cur03 = conn.cursor()
                            
                            if len(result_31) > 0:
                                # 매수금액
                                amt = int(Decimal(item['price']) * Decimal(item['volume']))
                                # 손절가
                                cut_price = result_31[4]
                                # 목표가
                                goal_price = result_31[5]
                                # 손절금액
                                cut_amt = int(amt * (100 - (cut_price / Decimal(item['price'])) * 100) / 100)
                                # 손절율
                                cut_rate = (100 - (cut_price / Decimal(item['price'])) * 100).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                                # 목표금액
                                goal_amt = int(Decimal(item['volume']) * goal_price) - amt
                                # 목표율
                                goal_rate = ((100 - (goal_price / Decimal(item['price'])) * 100) * -1).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                                # 안전마진수량
                                margin_vol = (cut_amt / (goal_price - cut_price)).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
                            
                            # 주문정보 생성
                            ins_param1 = (
                                cust_info['cust_num'],
                                cust_info['market_name'],
                                datetime.fromisoformat(item['created_at']).strftime("%Y%m%d%H%M%S"),
                                item['uuid'],
                                item['market'],
                                '01' if item['side'] == 'bid' else '02',
                                item['state'],
                                count,
                                Decimal(item['price']),
                                Decimal(item['volume']),
                                amt,
                                cut_price,
                                cut_rate,
                                cut_amt,
                                goal_price,
                                goal_rate,
                                goal_amt,
                                margin_vol,
                                Decimal(item['executed_volume']),
                                Decimal(item['remaining_volume']),
                                user_id,
                                datetime.now(),
                                user_id,
                                datetime.now(),
                            )
                            print("trade_mng : ",ins_param1)
                            insert1 = """
                                        insert into trade_mng(
                                            cust_num,
                                            market_name, 
                                            ord_dtm, 
                                            ord_no, 
                                            prd_nm, 
                                            ord_tp, 
                                            ord_state, 
                                            ord_count,
                                            ord_price,
                                            ord_vol,
                                            ord_amt,
                                            cut_price,
                                            cut_rate,
                                            cut_amt,
                                            goal_price,
                                            goal_rate,
                                            goal_amt,
                                            margin_vol,
                                            executed_vol,
                                            remaining_vol,
                                            regr_id, 
                                            reg_date, 
                                            chgr_id, 
                                            chg_date
                                        ) values (
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s,
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s,
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s, 
                                            %s,
                                            %s,
                                            %s, 
                                            %s,
                                            %s
                                        )
                                    """
                            cur03.execute(insert1, ins_param1)
                            conn.commit()
                            cur03.close()
                
                cur02.close()

        cur01.close()
        conn.close()        
            
    except Exception as e:
        print("에러 발생:", e)        
    

# 1분마다 실행 설정
schedule.every(1).minutes.do(analyze_data, 'phills2', 'UPBIT', 'mid')        

# 실행
if __name__ == "__main__":
    print("1분마다 분석 작업을 실행합니다...")
    analyze_data('phills2', 'UPBIT', 'mid')  # 첫 실행
    while True:
        schedule.run_pending()
        time.sleep(1)