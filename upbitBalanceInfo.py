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
    # print("response : ", response.json())
    return response.json()

def analyze_data(user, market, trend_type):
    
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
    
        user_id = "BALANCE_AUTO"
        
        cur02 = conn.cursor()
        upd_param1 = (
            user_id,   # chgr_id
            datetime.now(),  # chg_date
            cust_info['cust_num'],   
        )
        
        # 잔고정보 치리여부 일괄 변경 처리(proc_yn = 'N')
        update1 = """UPDATE balance_info SET 
                        proc_yn = 'N',
                        chgr_id = %s,
                        chg_date = %s
                    WHERE cust_num = %s
                """
        cur02.execute(update1, upd_param1)
        conn.commit()
        cur02.close()
        
        payload = {
            'access_key': cust_info['access_key'],
            'nonce': str(uuid.uuid4()),
        }

        jwt_token = jwt.encode(payload, cust_info['secret_key'])
        authorization = 'Bearer {}'.format(jwt_token)
        headers = {
            'Authorization': authorization,
        }

        # 잔고 조회
        accounts = requests.get(api_url + '/v1/accounts',headers=headers).json()
        
        for item in accounts:
            name = item['currency']
            price = float(item['avg_buy_price'])      # 평균단가    
            volume = float(item['balance'])           # 보유수량
            amt = int(price * volume)                 # 보유금액 
            

            cur03 = conn.cursor()
            
            if item['currency'] != "KRW":
                params = {
                    "markets": "KRW-"+item['currency']
                }

                current_price = 0
                current_amt = 0
                loss_profit_amt = 0
                loss_profit_rate = 0
                # 현재가 정보
                res = requests.get(api_url + "/v1/ticker", params=params).json()

                if isinstance(res, dict) and 'error' in res:
                    # 에러 메시지가 반환된 경우
                    error_name = res['error'].get('name', 'Unknown')
                    error_message = res['error'].get('message', 'Unknown')
                    # print(f"Error {error_name}: {error_message}")
                    # print(item['currency'])

                else:
                    current_price = float(res[0]['trade_price'])
                    
                    if current_price == 0:
                        continue

                    # 현재평가금액
                    current_amt = int(current_price * volume)
                    # 손실수익금
                    loss_profit_amt = current_amt - amt
                    # 손실수익률
                    loss_profit_rate = ((100 - Decimal(current_price / price) * 100) * -1).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

                    print(name,"price : ",price,", volume : ",volume,", amt : ",amt,", current_price : ",current_price,", current_amt : ",current_amt,", loss_profit_amt : ",loss_profit_amt,", loss_profit_rate : ",loss_profit_rate)    
                
                    cur031 = conn.cursor()
                    result_31 = []
                    # 매매신호정보 조회
                    query31 = "SELECT id, tr_dtm, tr_price, tr_volume, support_price, regist_price FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'S' AND tr_state = '02' order by tr_dtm desc LIMIT 1"
                    cur031.execute(query31, (item['currency']+"/KRW", ))  
                    result_31 = cur031.fetchone()
                
                    # 잔고정보 현행화
                    ins_param1 = (
                        price,
                        volume,
                        amt,
                        loss_profit_rate,
                        current_price,
                        current_amt,
                        result_31[5] if result_31 != None else 0,
                        result_31[4] if result_31 != None else 0,
                        user_id,
                        datetime.now(),
                        cust_info['cust_num'],
                        cust_info['market_name'],
                        params['markets'],
                        cust_info['acct_no'],
                        cust_info['cust_num'],
                        cust_info['market_name'],
                        params['markets'],
                        price,
                        volume,
                        amt,
                        loss_profit_rate,
                        current_price,
                        current_amt,
                        result_31[5] if result_31 != None else 0,
                        result_31[4] if result_31 != None else 0,
                        'Y',
                        user_id,
                        datetime.now(),
                        user_id,
                        datetime.now(),
                    )
                    
                    insert1 = "with upsert as (update balance_info set hold_price = %s, hold_volume = %s, hold_amt = %s, loss_profit_rate = %s, current_price = %s, current_amt = %s, target_price = %s, loss_price = %s, proc_yn = 'Y', chgr_id = %s, chg_date = %s where cust_num = %s and market_name = %s and prd_nm = %s returning * ) insert into balance_info(acct_no, cust_num, market_name, prd_nm, hold_price, hold_volume, hold_amt, loss_profit_rate, current_price, current_amt, target_price, loss_price, proc_yn, regr_id, reg_date, chgr_id, chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
                    cur03.execute(insert1, ins_param1)
                    conn.commit()
                    cur03.close()
                    cur031.close()
                
                time.sleep(0.1)
            
            else:
                print(name,"-",volume)        
            
        cur04 = conn.cursor()
        del_param1 = (
            cust_info['cust_num'],   
        )
        
        # 잔고정보 치리여부(proc_yn = 'N') 대상 일괄 삭제 처리
        delete1 = """DELETE FROM balance_info WHERE proc_yn = 'N' AND cust_num = %s """
        cur04.execute(delete1, del_param1)
        conn.commit()
        cur04.close()              
    
    cur01.close()
    conn.close()

# 1분마다 실행 설정
schedule.every(1).minutes.do(analyze_data, 'phills2', 'UPBIT', 'mid')        

# 실행
if __name__ == "__main__":
    print("1분마다 분석 작업을 실행합니다...")
    analyze_data('phills2', 'UPBIT', 'mid')  # 첫 실행
    while True:
        schedule.run_pending()
        time.sleep(1)