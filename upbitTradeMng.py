import jwt
import hashlib
import sys
import os
import requests
import uuid
from urllib.parse import urlencode, unquote
from dotenv import load_dotenv
load_dotenv()
from decimal import Decimal
import time
import psycopg2
from datetime import datetime, timedelta
import schedule
import pytz

api_url = os.getenv("UPBIT_API")

# 데이터베이스 연결 정보
DB_NAME = "universe"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "localhost"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

def close_order(access_key, secret_key, cust_num, start_dt, user_id, conn):
    try:
        date_obj = datetime.strptime(start_dt, '%Y%m%d')
        datetime_with_time = datetime.combine(date_obj, datetime.strptime('00:00:00', '%H:%M:%S').time())
        start_dt = datetime_with_time.isoformat() + "+09:00"
        
        # 업비트에서 거래 가능한 종목 목록
        url = "https://api.upbit.com/v1/market/all?is_details=false"
        headers = {"accept": "application/json"}
        market_list = requests.get(url, headers=headers).json()
        
        for m in market_list:
            params = {
                'market': m['market'],           # 마켓 ID
                'states[]': ['done', 'cancel'],
                "start_time": start_dt,             # 조회시작일 이후 7일까지
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
            # 종료된 주문 조회
            result = requests.get(api_url + '/v1/orders/closed', params=params, headers=headers).json()

            if result is not None:
                
                for idx, item in enumerate(result):

                    cur01 = conn.cursor()
                    result_1 = []

                    # 매매관리정보 존재여부 조회
                    select1 = """
                        SELECT 
                            ord_no
                        FROM trade_mng 
                        WHERE market_name = 'UPBIT'
                        AND ord_state IN ('done', 'cancel')
                        AND cust_num = %s
                        AND prd_nm = %s
                        AND (ord_no = %s OR orgn_ord_no = %s)

                        UNION ALL

                        SELECT 
                            ord_no
                        FROM trade_mng_hist
                        WHERE market_name = 'UPBIT'
                        AND ord_state IN ('done', 'cancel')
                        AND cust_num = %s
                        AND prd_nm = %s
                        AND (ord_no = %s OR orgn_ord_no = %s)
                    """
                    
                    param1 = (cust_num, item['market'], item['uuid'], item['uuid'], cust_num, item['market'], item['uuid'], item['uuid'])
                    cur01.execute(select1, param1)  
                    result_1 = cur01.fetchall()

                    if len(result_1) < 1:

                        cur02 = conn.cursor()
                        
                        if item['ord_type'] == 'market':    # 시장가 매도 : 주문가 = 체결금액 / 체결량
                            price = Decimal(item['executed_funds']) / Decimal(item['executed_volume'])
                            vol = Decimal(item['executed_volume'])
                            remaining_vol = 0
                        elif item['ord_type'] == 'price':    # 시장가 매수 : 주문가 = 체결금액 / 체결량
                            price = Decimal(item['executed_funds']) / Decimal(item['executed_volume'])
                            vol = Decimal(item['executed_volume'])
                            remaining_vol = 0
                        elif item['ord_type'] == 'limit':    # 지정가 주문
                            price = Decimal(item['price'])
                            vol = Decimal(item['volume'])
                            remaining_vol = Decimal(item['remaining_volume'])

                        # 잔고조회의 매수평균가, 보유수량 가져오기                     
                        hold_price = 0
                        hold_vol = 0
                        
                        try:
                            payload = {
                                'access_key': access_key,
                                'nonce': str(uuid.uuid4()),
                            }

                            jwt_token = jwt.encode(payload, secret_key)
                            authorization = 'Bearer {}'.format(jwt_token)
                            headers = {
                                'Authorization': authorization,
                            }

                            # 잔고 조회
                            accounts = requests.get(api_url + '/v1/accounts', headers=headers).json()

                        except Exception as e:
                            print(f"[잔고 조회 예외] 오류 발생: {e}")
                            accounts = []  # 또는 None 등, 이후 구문에서 사용할 수 있도록 기본값 설정
                        
                        for a in accounts:
                            name = "KRW-"+a['currency']
                            
                            # 매매관리정보의 상품코드과 잔고조회의 상품코드가 동일한 경우
                            if item['market'] == name:
                                hold_price = float(a['avg_buy_price'])                        # 평균단가    
                                hold_vol = float(a['balance']) + float(a['locked'])     # 보유수량 = 주문가능 수량 + 주문묶여있는 수량    

                        # 매매관리정보 생성
                        insert1 = """
                            INSERT INTO trade_mng (
                                cust_num, 
                                market_name, 
                                ord_dtm, 
                                ord_no, 
                                prd_nm, 
                                ord_tp, 
                                ord_state, 
                                ord_price, 
                                ord_vol, 
                                ord_amt,
                                executed_vol,
                                remaining_vol,
                                hold_price,
                                hold_vol,
                                paid_fee,
                                ord_type,
                                regr_id, 
                                reg_date, 
                                chgr_id, 
                                chg_date
                            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        ins_param1 = (
                            cust_num,
                            'UPBIT',
                            datetime.fromisoformat(item['created_at']).strftime("%Y%m%d%H%M%S"),
                            item['uuid'],
                            item['market'],
                            '01' if item['side'] == 'bid' else '02',
                            'done' if item['ord_type'] == 'price' or item['ord_type'] == 'market' else item['state'],
                            price,
                            vol,
                            Decimal(item['executed_funds']),
                            Decimal(item['executed_volume']),
                            remaining_vol,
                            hold_price,
                            hold_vol,
                            Decimal(item['paid_fee']),
                            item['ord_type'],
                            user_id,
                            datetime.now(),
                            user_id,
                            datetime.now()
                        )
                        cur02.execute(insert1, ins_param1)
                        conn.commit()
                        cur02.close()

                    cur01.close()        

    except Exception as e:
        print(f"[close_order - 전체 예외] 함수 실행 중 예외 발생: {e}")    

def open_order(access_key, secret_key, cust_num, user_id, conn):
    
    try:
        # 업비트에서 거래 가능한 종목 목록
        url = "https://api.upbit.com/v1/market/all?is_details=false"
        headers = {"accept": "application/json"}
        market_list = requests.get(url, headers=headers).json()
        
        for m in market_list:
            params = {
                'market': m['market'],
                'states[]': ['wait', 'watch']
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

            # 체결 대기 주문 조회
            raw_order_list = requests.get(api_url + "/v1/orders/open", params=params, headers=headers).json()             

            if raw_order_list is not None:
                
                for idx, item in enumerate(raw_order_list):

                    cur01 = conn.cursor()
                    result_1 = []

                    # 매매관리정보 존재여부 조회
                    select1 = """
                        SELECT 
                            ord_no
                        FROM trade_mng 
                        WHERE market_name = 'UPBIT'
                        AND cust_num = %s
                        AND prd_nm = %s
                        AND (ord_no = %s OR orgn_ord_no = %s)

                        UNION ALL

                        SELECT 
                            ord_no
                        FROM trade_mng_hist
                        WHERE market_name = 'UPBIT'
                        AND cust_num = %s
                        AND prd_nm = %s
                        AND (ord_no = %s OR orgn_ord_no = %s)
                    """
                    
                    param1 = (cust_num, item['market'], item['uuid'], item['uuid'], cust_num, item['market'], item['uuid'], item['uuid'])
                    cur01.execute(select1, param1)  
                    result_1 = cur01.fetchall()

                    if len(result_1) < 1:

                        # 잔고조회의 매수평균가, 보유수량 가져오기                     
                        hold_price = 0
                        hold_vol = 0
                        
                        try:
                            payload = {
                                'access_key': access_key,
                                'nonce': str(uuid.uuid4()),
                            }

                            jwt_token = jwt.encode(payload, secret_key)
                            authorization = 'Bearer {}'.format(jwt_token)
                            headers = {
                                'Authorization': authorization,
                            }

                            # 잔고 조회
                            accounts = requests.get(api_url + '/v1/accounts', headers=headers).json()

                        except Exception as e:
                            print(f"[잔고 조회 예외] 오류 발생: {e}")
                            accounts = []  # 또는 None 등, 이후 구문에서 사용할 수 있도록 기본값 설정
                        
                        for a in accounts:
                            name = "KRW-"+a['currency']
                            
                            # 매매관리정보의 상품코드과 잔고조회의 상품코드가 동일한 경우
                            if item['market'] == name:
                                hold_price = float(a['avg_buy_price'])                        # 평균단가    
                                hold_vol = float(a['balance']) + float(a['locked'])     # 보유수량 = 주문가능 수량 + 주문묶여있는 수량    

                        cur02 = conn.cursor()
                        
                        price = Decimal(item['price'])
                        vol = Decimal(item['volume'])
                        remaining_vol = Decimal(item['remaining_volume'])
                        
                        # 매매관리정보 생성
                        insert1 = """
                            INSERT INTO trade_mng (
                                cust_num, 
                                market_name, 
                                ord_dtm, 
                                ord_no, 
                                prd_nm, 
                                ord_tp, 
                                ord_state, 
                                ord_price, 
                                ord_vol, 
                                ord_amt,
                                executed_vol,
                                remaining_vol,
                                hold_price,
                                hold_vol,
                                paid_fee,
                                ord_type,
                                regr_id, 
                                reg_date, 
                                chgr_id, 
                                chg_date
                            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        ins_param1 = (
                            cust_num,
                            'UPBIT',
                            datetime.fromisoformat(item['created_at']).strftime("%Y%m%d%H%M%S"),
                            item['uuid'],
                            item['market'],
                            '01' if item['side'] == 'bid' else '02',
                            'done' if item['ord_type'] == 'price' or item['ord_type'] == 'market' else item['state'],
                            price,
                            vol,
                            Decimal(item['executed_funds']),
                            Decimal(item['executed_volume']),
                            remaining_vol,
                            hold_price,
                            hold_vol,
                            Decimal(item['paid_fee']),
                            item['ord_type'],
                            user_id,
                            datetime.now(),
                            user_id,
                            datetime.now()
                        )
                        cur02.execute(insert1, ins_param1)
                        conn.commit()
                        cur02.close()

                    cur01.close()  
                
    except Exception as e:
        print(f"[close_order - 전체 예외] 함수 실행 중 예외 발생: {e}")  

def analyze_data(user):
    
    # PostgreSQL 데이터베이스에 연결
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    
    cur01 = conn.cursor()

    # 고객명에 의한 고객정보 조회
    query1 = "SELECT cust_num, cust_nm, market_name, acct_no, access_key, secret_key, access_token, token_publ_date FROM cust_mng WHERE cust_nm = %s AND market_name = %s"
    cur01.execute(query1, (user, 'UPBIT'))  
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
    
        user_id = "TRADE_MNG"
        start_dt = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        # start_dt = "20250717"
        # 종료된 주문의 매매관리정보 미존재 대상 매매관리정보 생성 처리 
        close_order(cust_info['access_key'], cust_info['secret_key'], cust_info['cust_num'], start_dt, user_id, conn)
        # 체결대기 주문의 매매관리정보 미존재 대상 매매관리정보 생성 처리
        open_order(cust_info['access_key'], cust_info['secret_key'], cust_info['cust_num'], user_id, conn)
        
        timezone = pytz.timezone('Asia/Seoul')
        end_time = datetime.now(timezone)
        print(f"{cust_info['cust_nm']} : {start_dt} 이후 매매관리정보 현행화 작업 종료 시간 : {end_time}")
        
        # start_date = datetime(2024, 1, 1).date()
        # end_date = datetime(2024, 12, 31).date()
        
        # while start_date <= end_date:
        #     start_dt_str = start_date.strftime('%Y%m%d')

        #     try:
        #         # 종료된 주문의 매매관리정보 미존재 대상 매매관리정보 생성 처리
        #         close_order(
        #             cust_info['access_key'],
        #             cust_info['secret_key'],
        #             cust_info['cust_num'],
        #             start_dt_str,
        #             user_id,
        #             conn
        #         )
        #         timezone = pytz.timezone('Asia/Seoul')
        #         end_time = datetime.now(timezone)
        #         print(f"{cust_info['cust_nm']} : {start_dt_str} 이후 매매관리정보 현행화 작업 종료 시간 : {end_time}")
        #     except Exception as e:
        #         print(f"[{start_dt_str}] 처리 중 오류 발생: {e}")

        #     # 다음 7일 간격으로 증가
        #     start_date += timedelta(days=7)

    cur01.close()
    conn.close()

users = ['phills2', 'mama', 'honey']
# users = ['phills2']

# 실행
if __name__ == "__main__":
    # print("매매관리정보 현행화 작업을 1시간마다 실행합니다...")

    for user in users:
        analyze_data(user)
    #     schedule.every(60).minutes.do(analyze_data, user)
    #     # 매일 오전 9시에 실행되도록 스케줄 설정
    #     # schedule.every().day.at("09:00").do(analyze_data, user)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)