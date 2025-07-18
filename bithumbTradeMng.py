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

api_url = os.getenv("BITHUMB_API")

# 데이터베이스 연결 정보
DB_NAME = "universe"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "192.168.50.248"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

def ordno_order(access_key, secret_key, cust_num, user_id, conn):
    try:
        # 빗썸 거래 체결 주문번호
        param = dict( uuid='C0758000000097207818' )
        
        cur01 = conn.cursor()
        result_1 = []

        # 매매관리정보 존재여부 조회
        select1 = """
            SELECT 
                ord_no
            FROM trade_mng 
            WHERE market_name = 'BITHUMB'
            AND ord_state IN ('done')
            AND cust_num = %s
            AND (ord_no = %s OR orgn_ord_no = %s)

            UNION ALL

            SELECT 
                ord_no
            FROM trade_mng_hist
            WHERE market_name = 'BITHUMB'
            AND ord_state IN ('done')
            AND cust_num = %s
            AND (ord_no = %s OR orgn_ord_no = %s)
        """
        
        param1 = (cust_num, param['uuid'], param['uuid'], cust_num, param['uuid'], param['uuid'])
        cur01.execute(select1, param1)  
        result_1 = cur01.fetchall()
        
        if len(result_1) < 1:
            
            query = urlencode(param).encode()
            hash = hashlib.sha512()
            hash.update(query)
            query_hash = hash.hexdigest()
            payload = {
                'access_key': access_key,
                'nonce': str(uuid.uuid4()),
                'timestamp': round(time.time() * 1000), 
                'query_hash': query_hash,
                'query_hash_alg': 'SHA512',
            }   
            jwt_token = jwt.encode(payload, secret_key)
            authorization_token = 'Bearer {}'.format(jwt_token)
            headers = {
                'Authorization': authorization_token
            }

            # 개별 주문 조회
            response = requests.get(api_url + '/v1/order', params=param, headers=headers).json()

            if response['trades'] is not None:

                cur02 = conn.cursor()
                
                total_funds = sum(Decimal(trade['funds']) for trade in response['trades'])
                total_volume = sum(Decimal(trade['volume']) for trade in response['trades'])
                
                if response['ord_type'] == 'market':    # 시장가 매도 : 주문가 = 체결금액 / 체결량
                    price = total_funds / total_volume
                    vol = total_volume
                    remaining_vol = 0
                elif response['ord_type'] == 'price':    # 시장가 매수 : 주문가 = 체결금액 / 체결량
                    price = total_funds / total_volume
                    vol = total_volume
                    remaining_vol = 0
                elif response['ord_type'] == 'limit':    # 지정가 주문
                    price = Decimal(response['price'])
                    vol = Decimal(response['volume'])
                    remaining_vol = Decimal(response['remaining_volume'])

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
                        paid_fee,
                        ord_type,
                        regr_id, 
                        reg_date, 
                        chgr_id, 
                        chg_date
                    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                ins_param1 = (
                    cust_num,
                    'BITHUMB',
                    datetime.fromisoformat(response['created_at']).strftime("%Y%m%d%H%M%S"),
                    response['uuid'],
                    response['market'],
                    '01' if response['side'] == 'bid' else '02',
                    'done' if response['ord_type'] == 'price' or response['ord_type'] == 'market' else response['state'],
                    price,
                    vol,
                    total_funds,
                    total_volume,
                    remaining_vol,
                    Decimal(response['paid_fee']),
                    response['ord_type'],
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
    cur01.execute(query1, (user, 'BITHUMB'))  
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
        # 개별 주문 조회 매매관리정보 생성
        ordno_order(cust_info['access_key'], cust_info['secret_key'], cust_info['cust_num'], user_id, conn)
        
        timezone = pytz.timezone('Asia/Seoul')
        end_time = datetime.now(timezone)
        print(f"{cust_info['cust_nm']} : 매매관리정보 현행화 작업 종료 시간 : {end_time}")

    cur01.close()
    conn.close()

# users = ['phills2', 'mama', 'honey']
users = ['phills2']

# 실행
if __name__ == "__main__":
    print("매매관리정보 현행화 작업을 매일 실행합니다...")

    for user in users:
        analyze_data(user)
        # 매일 오전 9시에 실행되도록 스케줄 설정
        schedule.every().day.at("09:00").do(analyze_data, user)

    while True:
        schedule.run_pending()
        time.sleep(1)