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
DB_HOST = "192.168.50.248"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

def close_order(access_key, secret_key, cust_num, start_dt, user_id, prd_list, conn):
    try:
        date_obj = datetime.strptime(start_dt, '%Y%m%d')
        datetime_with_time = datetime.combine(date_obj, datetime.strptime('00:00:00', '%H:%M:%S').time())
        start_dt = datetime_with_time.isoformat() + "+09:00"

        for prd_nm in prd_list:
            params = {
                'market': "KRW-" + prd_nm,          # 마켓 ID
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
                        AND ord_no = %s

                        UNION ALL

                        SELECT 
                            ord_no
                        FROM trade_mng_hist
                        WHERE market_name = 'UPBIT'
                        AND ord_state IN ('done', 'cancel')
                        AND cust_num = %s
                        AND prd_nm = %s
                        AND ord_no = %s
                    """
                    
                    param1 = (cust_num, "KRW-" + prd_nm, item['uuid'], cust_num, "KRW-" + prd_nm, item['uuid'])
                    cur01.execute(select1, param1)  
                    result_1 = cur01.fetchall()

                    if len(result_1) < 1:

                        cur02 = conn.cursor()

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
                                regr_id, 
                                reg_date, 
                                chgr_id, 
                                chg_date
                            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        ins_param1 = (
                            cust_num,
                            'UPBIT',
                            datetime.fromisoformat(item['created_at']).strftime("%Y%m%d%H%M%S"),
                            item['uuid'],
                            item['market'],
                            '01' if item['side'] == 'bid' else '02',
                            item['state'],
                            Decimal(item['price']),
                            Decimal(item['volume']),
                            int(Decimal(item['price']) * Decimal(item['volume'])),
                            Decimal(item['executed_volume']),
                            Decimal(item['remaining_volume']),
                            Decimal(item['paid_fee']),
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

def analyze_data(user, prd_list):
    
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

        # 종료된 주문의 매매관리정보 미존재 대상 매매관리정보 생성 처리 
        close_order(cust_info['access_key'], cust_info['secret_key'], cust_info['cust_num'], start_dt, user_id, prd_list, conn)

        timezone = pytz.timezone('Asia/Seoul')
        end_time = datetime.now(timezone)
        print(f"{cust_info['cust_nm']} : {start_dt} 이후 매매관리정보 현행화 작업 종료 시간 : {end_time}")

    cur01.close()
    conn.close()

# 매매 대상 설정
prd_list = ('XRP', 'BTC', 'ETH', 'ARK', 'GAS', 'ATOM', 'SOL', 'ADA', 'ONDO', 'XLM', 'HBAR', 'SUI', 'LINK', 'STX', 'RENDER', 'ZETA', 'AVAX')

users = ['phills2', 'mama', 'honey']

# 실행
if __name__ == "__main__":
    print("매매관리정보 현행화 작업을 매일 실행합니다...")

    for user in users:
        analyze_data(user, prd_list)
        # 매일 오전 9시에 실행되도록 스케줄 설정
        schedule.every().day.at("09:00").do(analyze_data, user, prd_list)

    while True:
        schedule.run_pending()
        time.sleep(1)