import jwt
import hashlib
import sys
import os
import requests
import uuid
from urllib.parse import urlencode, unquote
from dotenv import load_dotenv
load_dotenv()
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
import time
import psycopg2
from datetime import datetime, timezone, timedelta
import schedule
import json
import shlex
import pytz

api_url = os.getenv("BITHUMB_API")

# 데이터베이스 연결 정보
DB_NAME = "universe"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "localhost"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

def format_number(value):
    try:
        return f"{float(value):,.2f}" if isinstance(value, float) else f"{int(value):,}"
    except:
        return str(value)
    
def candle_minutes_info(market, in_minutes):

    # UTC 시간을 사용
    now = datetime.now(timezone.utc).isoformat()
    is_breakdown = False

    url = f"{api_url}/v1/candles/minutes/{in_minutes}?market={market}&count=2&to={now}"
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # HTTP 오류 처리
        data = response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from Upbit: {e}")
        return []

    if len(data) < 2:
        print("Not enough candle data available.")
        return []

    # 최근 분봉 (현재 캔들)과 이전 분봉
    current_candle = data[0]
    previous_candle = data[1]

    # 현재 분봉 종가가 이전 분봉 저가를 이탈했는지와 이전 분봉의 거래량보다 현재 분봉의 거래량이 큰 경우 체크
    is_breakdown = current_candle["trade_price"] < previous_candle["low_price"] and current_candle["candle_acc_trade_volume"] > previous_candle["candle_acc_trade_volume"]
    
    return is_breakdown

def create_trade_plan(plan_list, user_id, conn):

    try:
        cur00 = conn.cursor()
        cur01 = conn.cursor()
        cur02 = conn.cursor()
        cur03 = conn.cursor()

        for plan in plan_list:
            
            param0 = (
                plan['cust_nm'], 
                plan['market_name'], 
                plan['prd_nm'], 
                plan['plan_tp'],
                plan['plan_price'],
                plan['plan_vol'],
                plan['plan_amt'],
                plan['regist_price'],
                plan['support_price'],
            )
            
            # 기존 데이터 백업
            select1 = """
                SELECT 1
                FROM trade_plan_hist
                WHERE cust_nm = %s
                AND market_name = %s
                AND prd_nm = %s 
                AND plan_tp = %s
                AND plan_execute = 'N'
                AND plan_price = %s
                AND plan_vol = %s
                AND plan_amt = %s
                AND regist_price = %s
                AND support_price = %s
            """
            
            cur00.execute(select1, param0)  
            result_01 = cur00.fetchone()

            if result_01:
                param1 = (plan['cust_nm'], plan['market_name'], plan['prd_nm'], plan['plan_tp'],)

                # 기존 데이터 백업
                insert1 = """
                    INSERT INTO trade_plan_hist (
                        cust_nm, market_name, plan_dtm, plan_execute, prd_nm, price, volume, 
                        plan_tp, plan_price, plan_vol, plan_amt, regist_price, support_price, 
                        regr_id, reg_date, chgr_id, chg_date
                    )
                    SELECT cust_nm, market_name, plan_dtm, plan_execute, prd_nm, price, volume, 
                        plan_tp, plan_price, plan_vol, plan_amt, regist_price, support_price, 
                        regr_id, reg_date, chgr_id, chg_date
                    FROM trade_plan
                    WHERE cust_nm = %s
                    AND market_name = %s
                    AND prd_nm = %s
                    AND plan_tp = %s
                    AND plan_execute = 'N'
                """
                
                cur01.execute(insert1, param1)
                rows_affected = cur01.rowcount
                
                if rows_affected > 0:
                    conn.commit()

                # 백업이 성공한 경우에만 삭제
                if rows_affected > 0:
                    delete1 = """
                        DELETE FROM trade_plan
                        WHERE cust_nm = %s 
                        AND market_name = %s
                        AND prd_nm = %s 
                        AND plan_tp = %s
                        AND plan_execute = 'N'
                    """
                    cur02.execute(delete1, param1)
                    conn.commit()

                param2 = (
                    plan['cust_nm'], 
                    plan['market_name'], 
                    datetime.now().strftime('%Y%m%d%H%M%S'), 
                    plan['prd_nm'], 
                    plan["price"], 
                    plan["volume"],
                    plan['plan_tp'],
                    plan['plan_price'],
                    plan['plan_vol'],
                    plan['plan_amt'],
                    plan['support_price'],
                    plan['regist_price'],
                    user_id,
                    datetime.now(),
                    user_id,
                    datetime.now(),
                    plan['cust_nm'], 
                    plan['market_name'], 
                    plan['prd_nm'], 
                    plan['plan_tp']
                )

                # 새로운 데이터 삽입 (중복 방지 포함)
                insert2 = """
                    INSERT INTO trade_plan (
                        cust_nm, market_name, plan_dtm, plan_execute, prd_nm, price, volume, 
                        plan_tp, plan_price, plan_vol, plan_amt, support_price, regist_price, 
                        regr_id, reg_date, chgr_id, chg_date
                    )
                    SELECT %s, %s, %s, 'N', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM trade_plan 
                        WHERE cust_nm = %s
                        AND market_name = %s
                        AND prd_nm = %s 
                        AND plan_tp = %s
                        AND plan_execute = 'N'
                    );
                """
                cur03.execute(insert2, param2)
                conn.commit()

    except Exception as e:
        conn.rollback()  # 오류 발생 시 롤백
        print(f"Error: {e}")

    finally:
        cur00.close()
        cur01.close()
        cur02.close()
        cur03.close()
        
def regist_trade_plan_hist(cust_num, cust_nm, market_name, prd_list, conn):
    
    try:
        cur01 = conn.cursor()
        cur02 = conn.cursor()
        
        for prd_nm in prd_list:
            
            param1 = (
                cust_nm, 
                market_name, 
                prd_nm, 
                cust_num,
                market_name, 
                prd_nm,
            )
            
            # 잔고정보 미존재 대상 매매처리된 매매예정정보 백업 처리
            insert1 = """
                INSERT INTO trade_plan_hist (
                    cust_nm, market_name, plan_dtm, plan_execute, prd_nm, price, volume, 
                    plan_tp, plan_price, plan_vol, plan_amt, regist_price, support_price, 
                    regr_id, reg_date, chgr_id, chg_date
                )
                SELECT cust_nm, market_name, plan_dtm, plan_execute, prd_nm, price, volume, 
                    plan_tp, plan_price, plan_vol, plan_amt, regist_price, support_price, 
                    regr_id, reg_date, chgr_id, chg_date
                FROM trade_plan
                WHERE cust_nm = %s
                AND market_name = %s
                AND prd_nm = %s
                AND plan_execute = 'Y'
                AND NOT EXISTS (
                    SELECT 1
                    FROM balance_info 
                    WHERE cust_num = %s AND market_name = %s AND prd_nm = %s
                )                      
            """
            cur01.execute(insert1, param1)
            rows_affected = cur01.rowcount
                
            if rows_affected > 0:
                conn.commit()

            # 백업이 성공한 경우에만 삭제
            if rows_affected > 0:
                delete1 = """
                    DELETE FROM trade_plan
                    WHERE cust_nm = %s
                    AND market_name = %s
                    AND prd_nm = %s
                    AND plan_execute = 'Y'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM balance_info 
                        WHERE cust_num = %s AND market_name = %s AND prd_nm = %s
                    )                      
                """
                cur02.execute(delete1, param1)            
                conn.commit()
            
    except Exception as e:
        conn.rollback()  # 오류 발생 시 롤백
        print(f"Error: {e}")      
        
    finally:
        cur01.close()  
        cur02.close()       

def decimal_converter(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # Decimal을 float으로 변환
    raise TypeError(f"Type {type(obj)} not serializable")

def get_order(access_key, secret_key, order_uuid):
    param = dict( uuid=order_uuid )

    # Generate access token
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

    try:
        # Call API
        response = requests.get(api_url + '/v1/order', params=param, headers=headers)
        # handle to success or fail
        # print(response.status_code)
        # print(response.json())
    except Exception as err:
        # handle exception
        print(err)

    return response.json()  

def open_order(access_key, secret_key, cust_num, market_name, user_id, conn):
    try:
        cur01 = conn.cursor()
        result_1 = []

        # 매매관리정보 조회(주문대기 대상)
        query1 = """
                    SELECT 
                        id, prd_nm, ord_state, executed_vol, remaining_vol, ord_no
                    FROM trade_mng 
                    WHERE market_name = %s
                    AND cust_num = %s
                    AND ord_state IN ('wait' ,'watch')
                """
        param1 = (market_name, cust_num,)
        cur01.execute(query1, param1)  
        result_1 = cur01.fetchall()

        for chk_ord in result_1:
            try:
                order_status = get_order(access_key, secret_key, chk_ord[5])
                ord_state = order_status['state']

                if ord_state == 'done':
                    if chk_ord[5] == order_status['uuid']:
                        
                        # 잔고조회의 매수평균가, 보유수량 가져오기                     
                        price = 0
                        volume = 0
                        
                        try:
                            payload = {
                                'access_key': access_key,
                                'nonce': str(uuid.uuid4()),
                                'timestamp': round(time.time() * 1000)
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
                        
                        for item in accounts:
                            name = "KRW-"+item['currency']
                            
                            # 매매관리정보의 상품코드과 잔고조회의 상품코드가 동일한 경우
                            if chk_ord[1] == name:
                                price = float(item['avg_buy_price'])                        # 평균단가    
                                volume = float(item['balance']) + float(item['locked'])     # 보유수량 = 주문가능 수량 + 주문묶여있는 수량

                        cur02 = conn.cursor()
                        upd_param1 = (
                            datetime.fromisoformat(order_status['trades'][0]['created_at']).strftime("%Y%m%d%H%M%S"),
                            order_status['trades'][0]['uuid'],
                            chk_ord[5],
                            order_status['state'],
                            Decimal(order_status['executed_volume']),
                            Decimal(order_status['remaining_volume']),
                            price,
                            volume,
                            Decimal(order_status['paid_fee']),
                            user_id,
                            datetime.now(),
                            chk_ord[0]
                        )
                        
                        update1 = """UPDATE trade_mng SET 
                                        ord_dtm = %s,
                                        ord_no = %s,
                                        orgn_ord_no = %s,
                                        ord_state = %s,
                                        executed_vol = %s,
                                        remaining_vol = %s,
                                        hold_price = %s,
                                        hold_vol = %s,
                                        paid_fee = %s,
                                        chgr_id = %s,
                                        chg_date = %s
                                    WHERE id = %s
                                """
                        cur02.execute(update1, upd_param1)
                        conn.commit()
                        cur02.close()
                
                elif ord_state == 'cancel':
                    if chk_ord[5] == order_status['uuid']:
                        
                        # 잔고조회의 매수평균가, 보유수량 가져오기                     
                        price = 0
                        volume = 0
                        
                        try:
                            payload = {
                                'access_key': access_key,
                                'nonce': str(uuid.uuid4()),
                                'timestamp': round(time.time() * 1000)
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
                        
                        for item in accounts:
                            name = "KRW-"+item['currency']
                            
                            # 매매관리정보의 상품코드과 잔고조회의 상품코드가 동일한 경우
                            if chk_ord[1] == name:
                                price = float(item['avg_buy_price'])                        # 평균단가    
                                volume = float(item['balance']) + float(item['locked'])     # 보유수량 = 주문가능 수량 + 주문묶여있는 수량

                        cur02 = conn.cursor()
                        upd_param1 = (
                            order_status['state'],
                            Decimal(order_status['executed_volume']),
                            Decimal(order_status['remaining_volume']),
                            price,
                            volume,
                            Decimal(order_status['paid_fee']),
                            user_id,
                            datetime.now(),
                            chk_ord[0]
                        )
                        
                        update1 = """UPDATE trade_mng SET 
                                        ord_state = %s,
                                        executed_vol = %s,
                                        remaining_vol = %s,
                                        hold_price = %s,
                                        hold_vol = %s,
                                        paid_fee = %s,
                                        chgr_id = %s,
                                        chg_date = %s
                                    WHERE id = %s
                                """
                        cur02.execute(update1, upd_param1)
                        conn.commit()
                        cur02.close()

                else:

                    param = dict( uuid=order_status['uuid'] )

                    # Generate access token
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
                    try:
                        # Call API
                        response = requests.get(api_url + '/v1/order', params=param, headers=headers).json()
                    except Exception as err:
                        # handle exception
                        print(err)

                    if response:
                        if chk_ord[5] == response['uuid']:

                            if chk_ord[4] != Decimal(response['remaining_volume']) or chk_ord[3] != Decimal(response['executed_volume']):
                                
                                # 잔고조회의 매수평균가, 보유수량 가져오기                     
                                price = 0
                                volume = 0
                                
                                try:
                                    payload = {
                                        'access_key': access_key,
                                        'nonce': str(uuid.uuid4()),
                                        'timestamp': round(time.time() * 1000)
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
                                
                                for item in accounts:
                                    name = "KRW-"+item['currency']
                                    
                                    # 매매관리정보의 상품코드과 잔고조회의 상품코드가 동일한 경우
                                    if chk_ord[1] == name:
                                        price = float(item['avg_buy_price'])                        # 평균단가    
                                        volume = float(item['balance']) + float(item['locked'])     # 보유수량 = 주문가능 수량 + 주문묶여있는 수량
                                        
                                cur02 = conn.cursor()
                                upd_param1 = (
                                    response['state'],
                                    Decimal(response['executed_volume']),
                                    Decimal(response['remaining_volume']),
                                    price,
                                    volume,
                                    Decimal(response['paid_fee']),
                                    user_id,
                                    datetime.now(),
                                    chk_ord[0]
                                )
                                
                                update1 = """UPDATE trade_mng SET 
                                                ord_state = %s,
                                                executed_vol = %s,
                                                remaining_vol = %s,
                                                hold_price = %s,
                                                hold_vol = %s,
                                                paid_fee = %s,
                                                chgr_id = %s,
                                                chg_date = %s
                                            WHERE id = %s
                                        """
                                cur02.execute(update1, upd_param1)
                                conn.commit()
                                cur02.close()
                                
                            else:
                                # 잔고조회의 매수평균가, 보유수량 가져오기                     
                                price = 0
                                volume = 0
                                
                                try:
                                    payload = {
                                        'access_key': access_key,
                                        'nonce': str(uuid.uuid4()),
                                        'timestamp': round(time.time() * 1000)
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
                                
                                for item in accounts:
                                    name = "KRW-"+item['currency']
                                    
                                    # 매매관리정보의 상품코드과 잔고조회의 상품코드가 동일한 경우
                                    if chk_ord[1] == name:
                                        price = float(item['avg_buy_price'])                        # 평균단가    
                                        volume = float(item['balance']) + float(item['locked'])     # 보유수량 = 주문가능 수량 + 주문묶여있는 수량

                                cur02 = conn.cursor()
                                upd_param1 = (
                                    price,
                                    volume,
                                    user_id,
                                    datetime.now(),
                                    chk_ord[0]
                                )
                                
                                update1 = """UPDATE trade_mng SET 
                                                hold_price = %s,
                                                hold_vol = %s,
                                                chgr_id = %s,
                                                chg_date = %s
                                            WHERE id = %s
                                        """
                                cur02.execute(update1, upd_param1)
                                conn.commit()
                                cur02.close()
                                    
            except Exception as e:
                print(f"[open_order - 내부 처리 중 예외] 주문 번호 {chk_ord[5]} 처리 중 오류 발생: {e}")
                continue  # 다음 주문으로 계속

    except Exception as e:
        print(f"[open_order - 전체 예외] 함수 실행 중 예외 발생: {e}")

    finally:
        if 'cur01' in locals() and cur01:
            cur01.close()                            

def proc_trade_mng_hist(cust_num, market_name, conn):

    try:
        cur01 = conn.cursor()
        cur02 = conn.cursor()

        param1 = (cust_num, market_name)

        # 기존 데이터 백업
        insert1 = """
            INSERT INTO trade_mng_hist (
                cust_num, market_name, hold_price, hold_vol, ord_dtm, ord_no, orgn_ord_no, prd_nm, ord_tp, ord_state, ord_count, ord_expect_totamt, ord_price, ord_vol, ord_amt,
                cut_price, cut_rate, cut_amt, goal_price, goal_rate, goal_amt, margin_vol, executed_vol, remaining_vol, paid_fee, ord_type, regr_id, reg_date, chgr_id, chg_date
            )
            SELECT 
                cust_num, market_name, hold_price, hold_vol, ord_dtm, ord_no, orgn_ord_no, prd_nm, ord_tp, ord_state, ord_count, ord_expect_totamt, ord_price, ord_vol, ord_amt,
                cut_price, cut_rate, cut_amt, goal_price, goal_rate, goal_amt, margin_vol, executed_vol, remaining_vol, paid_fee, ord_type, regr_id, reg_date, chgr_id, chg_date
            FROM trade_mng A
            WHERE A.cust_num = %s
            AND A.market_name = %s
            AND A.ord_state NOT IN ('wait')                          
            AND NOT EXISTS (
                SELECT 1
                FROM balance_info
                WHERE cust_num = A.cust_num
                AND market_name = A.market_name
                AND prd_nm = A.prd_nm
            )
        """
        cur01.execute(insert1, param1)
        
        rows_affected = cur01.rowcount
        if rows_affected > 0:
            conn.commit()
        cur01.close()

        # 백업이 성공한 경우에만 삭제
        if rows_affected > 0:
            delete1 = """
                DELETE FROM trade_mng A
                WHERE A.cust_num = %s 
                AND A.market_name = %s
                AND A.ord_state NOT IN ('wait')
                AND NOT EXISTS (
                    SELECT 1
                    FROM balance_info
                    WHERE cust_num = A.cust_num
                    AND market_name = A.market_name
                    AND prd_nm = A.prd_nm
                )
            """
            cur02.execute(delete1, param1)
            conn.commit()

    except Exception as e:
        conn.rollback()  # 오류 발생 시 롤백
        print(f"Error: {e}")

    finally:
        cur01.close()
        cur02.close()

# ─────────────────────────────────────────
# public.bit_fund_mng 실시간 자산정보/시장추세(market_ratio) 갱신
# upbitBalanceInfo.py 의 동일 처리를 그대로 이식 — BITHUMB API는 잔고/시세 응답 형식이 UPBIT와 동일하므로
# (bithumbBalanceInfo.py 상단 api_url이 이미 BITHUMB_API를 가리킴) 로직은 변경 없이 그대로 사용하고,
# get_business_day()만 BITHUMB의 영업일 경계(당일 00:00~23:59, bitTradingSet.py/bitTradingTrail.py의
# get_business_day('BITHUMB', ...)와 동일 규칙)에 맞게 조정한다.
# - 자산정보 : Batch/kis_interest_item_total.py 의 fund_marketLevel_proc(브로커 API 잔고 응답을
#   그대로 자산관리 테이블에 반영) 방식을 참고 — 매 사이클 잔고정보(balance_info) 현행화 직후 실시간 반영
# - 시장추세(btc_short/eth_short 등) : 같은 파일의 elif len(i[0]) == 4: 블록(코스피/코스닥 기준가 돌파·이탈
#   판정)을 참고하되, 지수 현재가 대신 코인의 당일 고가/저가로 판정하고, 기준값은 bit_interest_item(계좌별
#   BTC/ETH 이탈가/돌파가/지지가/저항가/추세하단가/추세상단가)에서 조회한다. 매 사이클 실시간 재판정하여
#   덮어쓰며, 이번 사이클에 임계값을 넘지 않은 timeframe은 직전 값을 그대로 유지한다.
# - market_ratio : 같은 파일의 compute_market_ratio() 가중합 로직을 그대로 이식
# ─────────────────────────────────────────

def get_business_day(now=None):
    """bit_fund_mng.dt 기준일 : BITHUMB는 당일 00:00부터 23:59까지를 하나의 영업일로 취급한다.
    (bitTradingSet.py/bitTradingTrail.py의 get_business_day('BITHUMB', ...)와 동일 규칙)"""
    now = now or datetime.now()
    return now.strftime("%Y%m%d")

def compute_market_ratio(btc_short, btc_mid, btc_long, eth_short, eth_mid, eth_long):
    """6개 timeframe 신호를 종합한 시장 승률 (0~100). kis_interest_item_total.py의 compute_market_ratio 이식."""
    rules = [
        (btc_short, '01', '02', 5),
        (btc_mid,   '03', '04', 8),
        (btc_long,  '05', '06', 12),
        (eth_short, '01', '02', 5),
        (eth_mid,   '03', '04', 8),
        (eth_long,  '05', '06', 12),
    ]
    score = 0
    for signal, bull, bear, weight in rules:
        if signal == bull:
            score += weight
        elif signal == bear:
            score -= weight
    return max(0, min(100, 50 + score))

def evaluate_trend_code(high, low, up_price, down_price, up_code, down_code):
    """당일 고가가 up_price 이상이면 up_code(상승), 저가가 down_price 이하면 down_code(하락).
    둘 다 충족하면(당일 중 양방향 모두 터치) 더 보수적인 하락 신호를 우선한다.
    둘 다 미충족이면 None을 반환해 호출측에서 직전 값을 유지하도록 한다."""
    if down_price and low <= down_price:
        return down_code
    if up_price and high >= up_price:
        return up_code
    return None

def get_coin_high_low(code):
    try:
        res = requests.get(api_url + "/v1/ticker", params={"markets": "KRW-" + code}).json()
        if isinstance(res, list) and len(res) > 0:
            return float(res[0]['high_price']), float(res[0]['low_price'])
    except Exception as e:
        print(f"[{code}] 시세 조회 오류: {e}")
    return None, None

def update_bit_fund_mng_realtime(cust_info, market, conn):
    acct_no = cust_info['acct_no']
    cust_num = cust_info['cust_num']

    trail_day = get_business_day()
    prev_day = (datetime.strptime(trail_day, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")

    try:
        # 자산정보 : balance_info 현행화 직후 실제 잔고 기준으로 집계
        cur1 = conn.cursor()
        cur1.execute("""
            SELECT
                COALESCE(SUM(hold_amt) FILTER (WHERE prd_nm != 'KRW-KRW' AND hold_volume > 0), 0) AS pchs_amt,
                COALESCE(SUM(current_amt) FILTER (WHERE prd_nm != 'KRW-KRW' AND hold_volume > 0), 0) AS evlu_amt,
                COALESCE(SUM(hold_amt) FILTER (WHERE prd_nm = 'KRW-KRW'), 0) AS cash_amt
            FROM balance_info
            WHERE cust_num = %s AND market_name = %s
        """, (cust_num, market))
        pchs_amt, evlu_amt, cash_amt = cur1.fetchone()
        cur1.close()

        pchs_amt = int(pchs_amt or 0)
        evlu_amt = int(evlu_amt or 0)
        cash_amt = int(cash_amt or 0)
        evlu_pfls_amt = evlu_amt - pchs_amt
        tot_evlu_amt = cash_amt + evlu_amt
        nass_amt = tot_evlu_amt

        # 전일 대비 자산증감액
        cur2 = conn.cursor()
        cur2.execute("""
            SELECT tot_evlu_amt FROM public.bit_fund_mng
            WHERE acct_no = %s AND cust_num = %s AND market_name = %s AND dt = %s
        """, (acct_no, cust_num, market, prev_day))
        prev_row = cur2.fetchone()
        cur2.close()
        prev_tot_evlu_amt = int(prev_row[0]) if prev_row else tot_evlu_amt
        asst_icdc_amt = tot_evlu_amt - prev_tot_evlu_amt

        # 기존(오늘자) 시장추세 값 조회 — 이번 사이클에 임계값 미충족 timeframe은 이 값을 유지
        cur3 = conn.cursor()
        cur3.execute("""
            SELECT btc_short, btc_mid, btc_long, eth_short, eth_mid, eth_long
            FROM public.bit_fund_mng
            WHERE acct_no = %s AND cust_num = %s AND market_name = %s AND dt = %s
        """, (acct_no, cust_num, market, trail_day))
        trend_row = cur3.fetchone()
        cur3.close()
        btc_short, btc_mid, btc_long, eth_short, eth_mid, eth_long = trend_row if trend_row else (None,) * 6

        # BTC/ETH 기준값(bit_interest_item) 조회 및 당일 고가/저가 대비 판정
        cur4 = conn.cursor()
        cur4.execute("""
            SELECT prd_nm, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price
            FROM bit_interest_item
            WHERE market_name = %s AND prd_nm IN ('BTC', 'ETH')
        """, (market,))
        interest_rows = cur4.fetchall()
        cur4.close()

        for prd_nm, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price in interest_rows:
            high, low = get_coin_high_low(prd_nm)
            if high is None:
                continue

            short_code = evaluate_trend_code(high, low, float(through_price or 0), float(leave_price or 0), '01', '02')
            mid_code = evaluate_trend_code(high, low, float(resist_price or 0), float(support_price or 0), '03', '04')
            long_code = evaluate_trend_code(high, low, float(trend_high_price or 0), float(trend_low_price or 0), '05', '06')

            if prd_nm == 'BTC':
                btc_short = short_code if short_code is not None else btc_short
                btc_mid = mid_code if mid_code is not None else btc_mid
                btc_long = long_code if long_code is not None else btc_long
            else:
                eth_short = short_code if short_code is not None else eth_short
                eth_mid = mid_code if mid_code is not None else eth_mid
                eth_long = long_code if long_code is not None else eth_long

        market_ratio = compute_market_ratio(btc_short, btc_mid, btc_long, eth_short, eth_mid, eth_long)

        cur5 = conn.cursor()
        cur5.execute("""
            INSERT INTO public.bit_fund_mng (
                acct_no, cust_num, market_name, dt,
                dnca_tot_amt, prvs_excc_amt, user_evlu_amt, tot_evlu_amt, nass_amt,
                pchs_amt, evlu_amt, evlu_pfls_amt, asst_icdc_amt,
                market_ratio, btc_short, eth_short, btc_mid, eth_mid, btc_long, eth_long,
                last_chg_date
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s
            )
            ON CONFLICT (acct_no, cust_num, market_name, dt) DO UPDATE SET
                dnca_tot_amt  = EXCLUDED.dnca_tot_amt,
                prvs_excc_amt = EXCLUDED.prvs_excc_amt,
                user_evlu_amt = EXCLUDED.user_evlu_amt,
                tot_evlu_amt  = EXCLUDED.tot_evlu_amt,
                nass_amt      = EXCLUDED.nass_amt,
                pchs_amt      = EXCLUDED.pchs_amt,
                evlu_amt      = EXCLUDED.evlu_amt,
                evlu_pfls_amt = EXCLUDED.evlu_pfls_amt,
                asst_icdc_amt = EXCLUDED.asst_icdc_amt,
                market_ratio  = EXCLUDED.market_ratio,
                btc_short     = EXCLUDED.btc_short,
                eth_short     = EXCLUDED.eth_short,
                btc_mid       = EXCLUDED.btc_mid,
                eth_mid       = EXCLUDED.eth_mid,
                btc_long      = EXCLUDED.btc_long,
                eth_long      = EXCLUDED.eth_long,
                last_chg_date = EXCLUDED.last_chg_date
        """, (
            acct_no, cust_num, market, trail_day,
            cash_amt, cash_amt, evlu_amt, tot_evlu_amt, nass_amt,
            pchs_amt, evlu_amt, evlu_pfls_amt, asst_icdc_amt,
            market_ratio, btc_short, eth_short, btc_mid, eth_mid, btc_long, eth_long,
            datetime.now()
        ))
        conn.commit()
        cur5.close()

    except Exception as e:
        conn.rollback()
        print(f"[{trail_day}-{cust_num}-{market}] bit_fund_mng 실시간 갱신 오류: {e}")

def analyze_data(user, market, trend_type, prd_list, plan_amt):
    
    # PostgreSQL 데이터베이스에 연결
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    
    if sys.platform == "win32":
        python_executable = "python"
        script_path = "C:\\Project\\frm_api_svc\\main.py"
    else:
        python_executable = "python3"
        script_path = "/home/terra/work/frm_api_svc/main.py"

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

        # 매매관리정보 주문대기 대상 매매관리정보 현행화
        open_order(cust_info['access_key'], cust_info['secret_key'], cust_info['cust_num'], cust_info['market_name'], user_id, conn)
        
        # 잔고정보 미존재 대상 매매처리된 매매예정정보 백업 처리
        regist_trade_plan_hist(cust_info['cust_num'], cust_info['cust_nm'], cust_info['market_name'], prd_list, conn)
        
        try:
            payload = {
                'access_key': cust_info['access_key'],
                'nonce': str(uuid.uuid4()),
                'timestamp': round(time.time() * 1000)
            }

            jwt_token = jwt.encode(payload, cust_info['secret_key'])
            authorization = 'Bearer {}'.format(jwt_token)
            headers = {
                'Authorization': authorization,
            }

            # 잔고 조회
            accounts = requests.get(api_url + '/v1/accounts', headers=headers).json()

        except Exception as e:
            print(f"[{user}-잔고 조회 예외] 오류 발생: {e}")
            accounts = []  # 또는 None 등, 이후 구문에서 사용할 수 있도록 기본값 설정
        
        if not isinstance(accounts, list):
            print(f"[{user}-잔고 조회] 리스트 형태 아님: {accounts}")
            accounts = []
            
        trade_cash = 0
        
        for item in accounts:
            if isinstance(item, dict) and item.get("currency") == "KRW":
                trade_cash = float(item.get("balance", 0))    # 주문가능 금액

        # 주문가능 금액이 매매예정금액보다 큰 경우 매수 진행
        if plan_amt < trade_cash:
        
            cur031 = conn.cursor()
            result_31 = []

            # 매매예정정보 조회(주문정보 미처리 매수 대상)
            query31 = """
                    SELECT A.id, A.plan_dtm, split_part(A.prd_nm, '-', 2), A.plan_price, A.plan_vol, A.plan_amt, A.support_price, A.regist_price,
                        (SELECT CASE WHEN last_buy_count = 0 THEN 1 ELSE last_buy_count + 1 END FROM balance_info where cust_num = %s AND market_name = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '-', 2)),
                        (SELECT CASE WHEN last_sell_count = 0 THEN 1 ELSE last_sell_count + 1 END FROM balance_info where cust_num = %s AND market_name = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '-', 2))
                    FROM TRADE_PLAN A
                    WHERE A.cust_nm = %s
                    AND A.market_name = %s
                    AND A.plan_tp = 'B1'
                    AND A.plan_execute = 'N' 
                    AND split_part(A.prd_nm, '-', 2) IN %s
                """
            param1 = (cust_info['cust_num'], market, cust_info['cust_num'], market, cust_info['cust_nm'], market, prd_list)
            cur031.execute(query31, param1)  
            result_31 = cur031.fetchall()

            trade_list = []

            if len(result_31) > 0:
                for item in result_31:

                    params = {
                        "markets": "KRW-"+ item[2]
                    }

                    cu_price = 0

                    try:
                        # 현재가 정보
                        res = requests.get(api_url + "/v1/ticker", params=params).json()

                        if isinstance(res, dict) and 'error' in res:
                            # 에러 메시지가 반환된 경우
                            error_name = res['error'].get('name', 'Unknown')
                            error_message = res['error'].get('message', 'Unknown')
                            print(f"[{user}-Ticker 조회 오류] {error_name}: {error_message}")

                    except Exception as e:
                        print(f"[{user}-Ticker 조회 예외] 오류 발생: {e}")
                        res = None 
                    
                    if len(res) > 0:                
                        cu_price = float(res[0]['trade_price'])    
                        support_price = float(Decimal(cu_price * 0.98).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP))    # cu_price의 -2%
                        regist_price = float(Decimal(cu_price * 1.04).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP))     # cu_price의 4%

                        # 현재가가 저항가를 돌파한 경우 매매주문 호출
                        if cu_price > item[7]:
                            
                            # 잔고정보 매수횟수, 매도횟수 차감 계산
                            if item[8] is None:
                                tr_count = 1
                            else:
                                tr_count = 1 if item[8] - item[9] <= 0 else item[8] - item[9]   
                                

                            # 잔고정보 매수 횟수 매도 횟수 차가 2보다 작은 경우 : 
                            if tr_count < 2:
                                trade_info = {
                                    "tr_tp": "B",
                                    "tr_state": "02",
                                    "id": item[0],
                                    "prd_nm": item[2],
                                    "tr_price": cu_price,
                                    "support_price": support_price,
                                    "regist_price": regist_price,
                                    "tr_count": tr_count,
                                    "buy_order_no": None,
                                    "plan_amt": plan_amt,  # 매매예정금액
                                    "trade_source": "PLAN"
                                }
                                trade_list.append(trade_info)
                    
                    else:
                        print("Ticker 데이터가 없습니다.")
                    
                    time.sleep(0.1)            

                if len(trade_list) > 0:
                    trade_list_json = json.dumps(trade_list, default=decimal_converter)
                    safe_trade_list_json = shlex.quote(trade_list_json)    
                
                    os.system(f"{python_executable} {script_path} order-chk {user} {market} {safe_trade_list_json} --work_mm=202503")

            cur031.close()

            cur034 = conn.cursor()
            result_34 = []

            # 매매신호정보 조회(주문정보 미처리 매수 대상)
            query34 = """
                        SELECT 
                            A.id, split_part(A.prd_nm, '/', 1), A.tr_price, ROUND(A.tr_price * 0.98, 1) AS support_price, ROUND(A.tr_price * 1.04, 1) AS regist_price, 
                            (SELECT CASE WHEN last_buy_count = 0 THEN 1 ELSE last_buy_count + 1 END FROM balance_info where cust_num = %s AND market_name = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '/', 1))
                            , (SELECT CASE WHEN last_sell_count = 0 THEN 1 ELSE last_sell_count + 1 END FROM balance_info where cust_num = %s AND market_name = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '/', 1))
                            , B.prd_nm, B.plan_price, B.plan_vol, B.plan_amt, B.support_price, B.regist_price, C.id
                        FROM TR_SIGNAL_INFO A
                        LEFT OUTER JOIN TRADE_PLAN B
                        ON B.cust_nm = %s AND split_part(A.prd_nm, '/', 1) = split_part(B.prd_nm, '-', 2) AND B.plan_tp = 'B1' AND B.plan_execute = 'N' AND B.market_name = %s
                        LEFT OUTER JOIN TRADE_PLAN C
                        ON C.cust_nm = %s AND split_part(A.prd_nm, '/', 1) = split_part(C.prd_nm, '-', 2) AND C.plan_tp = 'S1' AND B.plan_execute = 'N' AND B.market_name = %s
                        WHERE A.signal_name = %s
                        AND A.tr_tp = 'B'
                        AND A.tr_state = '02'
                        AND A.buy_order_no IS NULL
                        AND split_part(A.prd_nm, '/', 1) IN %s
                        ORDER BY A.tr_dtm DESC
                    """
            param4 = (cust_info['cust_num'], market, cust_info['cust_num'], market, cust_info['cust_nm'], market, cust_info['cust_nm'], market, f"TrendLine-{trend_type}", prd_list)
            cur034.execute(query34, param4)  
            result_34 = cur034.fetchall()

            trade_list = []
            plan_list = []

            if len(result_34) >  0:
                for item in result_34:
                    
                    # 잔고정보 매수횟수, 매도횟수 차감 계산
                    if item[5] is None:
                        tr_count = 1
                    else:
                        tr_count = 1 if item[5] - item[6] <= 0 else item[5] - item[6]   
                                    
                    # 잔고정보 매수 횟수 매도 횟수 차가 2보다 작은 경우
                    if tr_count < 2:
                        trade_info = {
                            "tr_tp": "B",
                            "tr_state": "02",
                            "id": item[0],
                            "prd_nm": item[1],
                            "tr_price": item[2],
                            "support_price": item[11] if item[11] is not None else item[3],
                            "regist_price": item[12] if item[12] is not None else item[4],
                            "tr_count": tr_count,
                            "buy_order_no": None,
                            "plan_amt": plan_amt,  # 매매예정금액
                            "trade_source": "SIGNAL"
                        }
                        trade_list.append(trade_info)

                        # 매도(S1) 매매예정정보 미존재한 경우 리스트 생성(S1, S2) : 현재가(plan_price), 저항가(현재가 기준 4% 수익 regist_price), 이탈가(현재가 기준 2% 손실 support_price)
                        if item[13] is None:
                            buy_division_amt_except_fee = (int(plan_amt) * Decimal('0.9995')).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
                            # 종목당 손실금액(매수예정금액의 -2%)
                            cut_amt = int(plan_amt * Decimal('0.98'))
                            # 안전마진 매도 수량
                            plan_vol = (Decimal(cut_amt) / (item[4] - item[3])).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
                            # 매매예정금액
                            plan_amt_f = int(item[2] * Decimal(plan_vol))

                            plan_param = {
                                "cust_nm": cust_info['cust_nm'],
                                "market_name": market,
                                "prd_nm": "KRW-"+item[1], 
                                "price": 0, 
                                "volume": 0,
                                "plan_tp": "S1",
                                "plan_price": item[2],
                                "plan_vol": plan_vol,
                                "plan_amt": plan_amt_f,
                                "support_price": item[3],
                                "regist_price": item[4],
                            }

                            plan_list.append(plan_param)

                            # 매도 수량
                            plan_vol = (buy_division_amt_except_fee / item[2]).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
                            # 매매예정금액
                            plan_amt_f = int(item[2] * Decimal(plan_vol))

                            plan_param = {
                                "cust_nm": cust_info['cust_nm'],
                                "market_name": market,
                                "prd_nm": "KRW-"+item[1], 
                                "price": 0, 
                                "volume": 0,
                                "plan_tp": "S2",
                                "plan_price": item[2],
                                "plan_vol": plan_vol,
                                "plan_amt": plan_amt_f,
                                "support_price": item[3],
                                "regist_price": item[4],
                            }

                            plan_list.append(plan_param)

                # 매매예정정보 백업 및 생성
                if len(plan_list) > 0:
                    create_trade_plan(plan_list, user_id, conn)

                if len(trade_list) > 0:
                    trade_list_json = json.dumps(trade_list, default=decimal_converter)
                    safe_trade_list_json = shlex.quote(trade_list_json)    
                    
                    os.system(f"{python_executable} {script_path} order-chk {user} {market} {safe_trade_list_json} --work_mm=202503")            

            cur034.close()

        cur02 = conn.cursor()
        upd_param1 = (
            user_id,   # chgr_id
            datetime.now(),  # chg_date
            cust_info['cust_num'],   
        )
        
        # 잔고정보 처리 시작
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
        
        for item in accounts:
            name = item['currency']
            price = float(item['avg_buy_price'])      # 평균단가    
            volume = float(item['balance']) + float(item['locked'])    # 보유수량 = 주문가능 수량 + 주문묶여있는 수량
            amt = int(price * volume)                 # 보유금액 
            
            cur03 = conn.cursor()
            
            if item['currency'] not in ["P", "KRW"]:
                params = {
                    "markets": "KRW-"+item['currency']
                }

                current_price = 0
                current_amt = 0
                loss_profit_amt = 0
                loss_profit_rate = 0
                
                try:
                    # 현재가 정보
                    res = requests.get(api_url + "/v1/ticker", params=params).json()

                    if isinstance(res, dict) and 'error' in res:
                        # 에러 메시지가 반환된 경우
                        error_name = res['error'].get('name', 'Unknown')
                        error_message = res['error'].get('message', 'Unknown')
                        print(f"[{user}-Ticker 조회 오류] {error_name}: {error_message}")
                        continue

                except Exception as e:
                    print(f"[{user}-Ticker 조회 예외] 오류 발생: {e}")
                    res = None 
                
                if len(res) > 0:    
                    current_price = float(res[0]['trade_price'])
                    
                    if current_price == 0:
                        continue

                    # 현재평가금액
                    current_amt = int(current_price * volume)
                    # 손실수익금
                    loss_profit_amt = current_amt - amt
                    # 손실수익률
                    loss_profit_rate = ((100 - Decimal(current_price / price) * 100) * -1).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

                    print(f"[{user}-{name}] 보유단가 : {format_number(price)}, 보유량 : {format_number(volume)}, 보유금액 : {format_number(amt)}원, 현재가 : {format_number(current_price)}, 현재금액 : {format_number(current_amt)}원, 손수익금액 : {format_number(loss_profit_amt)}원, 손수익율 : {format_number(loss_profit_rate)}%")    
                
                    cur032 = conn.cursor()
                    result_32 = []
                    cur033 = conn.cursor()
                    result_33 = []    

                    # 잔고정보 조회 : last_order_no, last_buy_count, last_sell_count, loss_price, target_price 설정
                    query33 = """
                        SELECT 
                            (SELECT ord_no FROM trade_mng WHERE cust_num = A.cust_num AND market_name = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '-', 2) AND ord_state = 'done' ORDER BY ord_dtm DESC LIMIT 1) AS last_order_no,
                            (SELECT count(*) FROM trade_mng WHERE cust_num = A.cust_num AND market_name = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '-', 2) AND ord_state = 'done' AND ord_tp = '01') AS last_buy_count,
                            (SELECT count(*) FROM trade_mng WHERE cust_num = A.cust_num AND market_name = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '-', 2) AND ord_state = 'done' AND ord_tp = '02') AS last_sell_count,
                            COALESCE(B.support_price, 0) AS loss_price,
                            COALESCE(B.regist_price, 0) AS target_price,
                            A.prd_nm, B.id
                        FROM balance_info A
                        LEFT OUTER JOIN TRADE_PLAN B
                        ON B.cust_nm = %s AND split_part(A.prd_nm, '-', 2) = split_part(B.prd_nm, '-', 2) AND B.plan_tp = 'S1' AND B.plan_execute = 'N'
                        WHERE A.cust_num = %s
                        AND split_part(A.prd_nm, '-', 2) = %s 
                    """
                    param3 = (market, market, market, cust_info['cust_nm'], cust_info['cust_num'], item['currency'],)
                    cur033.execute(query33, param3)  
                    result_33 = cur033.fetchall()
                    
                    last_order_no = None
                    last_buy_count = 0
                    last_sell_count = 0
                    loss_price = 0
                    target_price = 0

                    if len(result_33) > 0:

                        trade_list = []

                        for balance_info in result_33:
                            last_order_no = balance_info[0]
                            last_buy_count = balance_info[1]
                            last_sell_count = balance_info[2]
                            loss_price = balance_info[3]
                            target_price = balance_info[4]
                            product = balance_info[5]
                            plan_id = balance_info[6]
                            
                            product_symbol = product.split('-')[-1] if '-' in product else product

                            # 매도 대상의 보유 상품이 존재할 경우
                            if product_symbol in prd_list:
                            
                                # 매도가능수량 존재하는 경우
                                if name == product_symbol and float(item['balance']) > 0:
                                    
                                    # 매매예정정보(매도) 존재하는 경우
                                    if plan_id is not None:
                                        # 현재가가 이탈가 이탈하거나 저항가 돌파한 경우 매매주문 호출
                                        if current_price < float(loss_price) or current_price > float(target_price):

                                            trade_info = {
                                                "tr_tp": "S",
                                                "tr_state": "02",
                                                "id": plan_id,
                                                "prd_nm": product_symbol,
                                                "tr_price": current_price,
                                                "support_price": float(loss_price),
                                                "regist_price": float(target_price),
                                                "tr_count": last_sell_count + 1 if last_sell_count > 0 else 1,    # 매도주문건수
                                                "sell_order_no": None,
                                                "plan_amt": plan_amt,  # 매매예정금액
                                                "trade_source": "PLAN"
                                            }
                                            trade_list.append(trade_info)

                                    else:
                                        # 매도 완료 존재한 경우
                                        if last_sell_count > 0:
                                            if trend_type == "long":
                                                in_minutes = "240"
                                            elif trend_type == "mid":
                                                in_minutes = "60"
                                            else:
                                                in_minutes = "15"

                                            # 현재 분봉 종가가 이전 분봉 저가를 이탈했는지와 이전 분봉의 거래량보다 현재 분봉의 거래량이 큰 경우 체크
                                            if candle_minutes_info(product, in_minutes):
                                                trade_info = {
                                                    "tr_tp": "S",
                                                    "tr_state": "02",
                                                    "id": plan_id if plan_id is not None else None,
                                                    "prd_nm": product_symbol,
                                                    "tr_price": current_price,
                                                    "support_price": float(loss_price),
                                                    "regist_price": float(target_price),
                                                    "tr_count": last_sell_count + 1 if last_sell_count > 0 else 1,    # 매도주문건수
                                                    "sell_order_no": None,
                                                    "plan_amt": plan_amt,  # 매매예정금액
                                                    "trade_source": "PLAN"
                                                }
                                                trade_list.append(trade_info)

                                        else:

                                            # 매매신호정보 및 매매예정정보 조회(주문정보 미처리 매도 대상)
                                            query32 = """
                                                WITH signal_info AS (
                                                    SELECT id, prd_nm, tr_price, tr_dtm, support_price, regist_price, tr_count FROM TR_SIGNAL_INFO WHERE TR_TP = 'S' AND TR_STATE = '02' and signal_name = %s AND SELL_ORDER_NO IS null
                                                    UNION
                                                    SELECT id, prd_nm, tr_price, tr_dtm, support_price, regist_price, tr_count FROM TR_SIGNAL_INFO WHERE TR_TP = 'B' AND TR_STATE = '22' and signal_name = %s AND SELL_ORDER_NO IS null
                                                )
                                                SELECT 
                                                    A.id, split_part(A.prd_nm, '/', 1), A.tr_price, A.support_price, A.regist_price, 
                                                    (SELECT CASE WHEN last_sell_count = 0 THEN 1 ELSE last_sell_count + 1 END FROM balance_info where cust_num = %s AND market_name = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '-', 2))
                                                    , B.prd_nm, B.plan_price, B.plan_vol, B.plan_amt, B.support_price, B.regist_price
                                                FROM (
                                                    SELECT * 
                                                    FROM signal_info 
                                                    WHERE split_part(prd_nm, '/', 1) = %s 
                                                    ORDER BY tr_dtm DESC 
                                                    LIMIT 1  -- 최신 데이터 한 건만 가져옴
                                                ) A
                                                LEFT OUTER JOIN TRADE_PLAN B
                                                ON B.cust_nm = %s AND split_part(A.prd_nm, '/', 1) = split_part(B.prd_nm, '-', 2) AND B.plan_tp = 'S1' AND B.plan_execute = 'N' AND B.market_name = %s
                                            """
                                            param2 = (f"TrendLine-{trend_type}", f"TrendLine-{trend_type}", cust_info['cust_num'], market, item['currency'], cust_info['cust_nm'], market)
                                            cur032.execute(query32, param2)  
                                            result_32 = cur032.fetchall()

                                            support_price = 0
                                            regist_price = 0

                                            if len(result_32) > 0:
                                                for trade_signal in result_32:
                                                    support_price = trade_signal[10] if trade_signal[10] is not None else trade_signal[3]
                                                    regist_price = trade_signal[11] if trade_signal[11] is not None else trade_signal[4]
                                                    
                                                    trade_info = {
                                                        "tr_tp": "S",
                                                        "tr_state": "02",
                                                        "id": trade_signal[0],
                                                        "prd_nm": trade_signal[1],
                                                        "tr_price": trade_signal[2],
                                                        "support_price": float(support_price),
                                                        "regist_price": float(regist_price),
                                                        "tr_count": trade_signal[5],    # 매도주문건수
                                                        "sell_order_no": None,
                                                        "plan_amt": plan_amt,  # 매매예정금액
                                                        "trade_source": "SIGNAL"
                                                    }
                                                    trade_list.append(trade_info)

                            # 잔고정보 현행화
                            upd_param2 = (
                                price,
                                volume,
                                amt,
                                loss_profit_rate,
                                last_order_no,
                                last_buy_count,
                                last_sell_count,
                                current_price,
                                current_amt,
                                float(target_price),
                                float(loss_price),
                                user_id,
                                datetime.now(),
                                cust_info['cust_num'],
                                cust_info['market_name'],
                                "KRW-"+item['currency'],
                            )
                            
                            update2 = "update balance_info set hold_price = %s, hold_volume = %s, hold_amt = %s, loss_profit_rate = %s, last_order_no = %s, last_buy_count = %s, last_sell_count = %s, current_price = %s, current_amt = %s, target_price = %s, loss_price = %s, proc_yn = 'Y', chgr_id = %s, chg_date = %s where cust_num = %s and market_name = %s and prd_nm = %s"
                            cur03.execute(update2, upd_param2)
                            conn.commit()
                            cur03.close()            

                        if len(trade_list) > 0:
                            trade_list_json = json.dumps(trade_list, default=decimal_converter)
                            safe_trade_list_json = shlex.quote(trade_list_json)    
                        
                            os.system(f"{python_executable} {script_path} order-chk {user} {market} {safe_trade_list_json} --work_mm=202503")
                    
                    else:
                        # 잔고조회 기준 잔고정보 미존재 대상 생성 처리
                        ins_param1 = (
                            cust_info['acct_no'],
                            cust_info['cust_num'],
                            cust_info['market_name'],
                            "KRW-"+item['currency'],
                            price,
                            volume,
                            amt,
                            loss_profit_rate,
                            current_price,
                            current_amt,
                            'Y',
                            user_id,
                            datetime.now(),
                            user_id,
                            datetime.now(),
                        )
                        
                        insert1 = "insert into balance_info(acct_no, cust_num, market_name, prd_nm, hold_price, hold_volume, hold_amt, loss_profit_rate, current_price, current_amt, proc_yn, regr_id, reg_date, chgr_id, chg_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cur03.execute(insert1, ins_param1)
                        conn.commit()
                        cur03.close() 

                    cur032.close()
                    cur033.close()
                
                else:
                    print("Ticker 데이터가 없습니다.")
                
                time.sleep(0.1)
            
            else:
                
                ins_param1 = (
                    cust_info['acct_no'],
                    cust_info['cust_num'],
                    cust_info['market_name'],
                    "KRW-"+name,
                    0,
                    0,
                    volume,
                    0,
                    volume,
                    'Y',
                    user_id,
                    datetime.now(),
                    user_id,
                    datetime.now(),
                )

                insert1 = """
                    INSERT INTO balance_info (
                        acct_no, cust_num, market_name, prd_nm,
                        hold_price, hold_volume, hold_amt,
                        current_price, current_amt,
                        proc_yn, regr_id, reg_date, chgr_id, chg_date
                    )
                    VALUES (%s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s, %s, %s, %s)
                    ON CONFLICT (acct_no, cust_num, market_name, prd_nm)
                    DO UPDATE SET
                        hold_price = EXCLUDED.hold_price,
                        hold_volume = EXCLUDED.hold_volume,
                        hold_amt = EXCLUDED.hold_amt,
                        current_price = EXCLUDED.current_price,
                        current_amt = EXCLUDED.current_amt,
                        proc_yn = EXCLUDED.proc_yn,
                        chgr_id = EXCLUDED.chgr_id,
                        chg_date = EXCLUDED.chg_date;
                """

                cur03.execute(insert1, ins_param1)
                conn.commit()
                cur03.close()
            
                timezone = pytz.timezone('Asia/Seoul')
                end_time = datetime.now(timezone)
                print(f"[{user}-{name}] {format_number(volume)}원, 잔고정보 체크 시간 : {end_time}")
            
        cur04 = conn.cursor()
        del_param1 = (
            cust_info['cust_num'],   
        )
        
        # 잔고정보 치리여부(proc_yn = 'N') 대상 일괄 삭제 처리
        delete1 = """DELETE FROM balance_info WHERE proc_yn = 'N' AND cust_num = %s """
        cur04.execute(delete1, del_param1)
        conn.commit()
        cur04.close()

        # 잔고정보 미존재 대상 매매관리정보 백업 처리
        proc_trade_mng_hist(cust_info['cust_num'], cust_info['market_name'], conn)

        # 잔고정보 현행화 완료 후 : bit_fund_mng 실시간 자산정보/시장추세(market_ratio) 갱신
        update_bit_fund_mng_realtime(cust_info, market, conn)

    cur01.close()
    conn.close()

# 매수 대상 설정
# prd_list = ('XRP', 'BTC', 'ETH', 'SOL', 'ADA', 'ONDO', 'XLM', 'HBAR', 'SUI', 'LINK', 'STX', 'RENDER', 'ZETA', 'AVAX')
prd_list = ('ETH',)

# 매수예정금액
plan_amt = 100000000

users = ['phills2', 'mama', 'honey']
market = 'BITHUMB'
trend_type = 'long'    

# 실행
if __name__ == "__main__":
    # print("잔고정보 1분마다 분석 작업을 실행합니다...")

    for user in users:
        analyze_data(user, market, trend_type, prd_list, plan_amt)
    #     schedule.every(1).minutes.do(analyze_data, user, market, trend_type, prd_list, plan_amt)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)