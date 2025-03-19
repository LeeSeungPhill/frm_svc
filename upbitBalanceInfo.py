import jwt
import hashlib
import sys
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
import json
import shlex

api_url = os.getenv("UPBIT_API")

# 데이터베이스 연결 정보
DB_NAME = "universe"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "192.168.50.248"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

def decimal_converter(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # Decimal을 float으로 변환
    raise TypeError(f"Type {type(obj)} not serializable")

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

def open_order(access_key, secret_key, cust_num, market_name, user_id, conn):

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

    order_list = []

    for chk_ord in result_1 :
        # 주문 조회
        order_status = get_order(access_key, secret_key, chk_ord['ord_no'])
        ord_state = order_status['state']

        # 체결완료 상태인 경우
        if ord_state == 'done':
            if chk_ord['ord_no'] == order_status['uuid']:
                order_param = {
                    "ord_dtm": datetime.fromisoformat(order_status['trades'][0]['created_at']).strftime("%Y%m%d%H%M%S"),
                    "ord_no": order_status['trades'][0]['uuid'],
                    "prd_nm": order_status['trades'][0]['market'],
                    "ord_tp": '01' if order_status['trades'][0]['side'] == 'bid' else '02',
                    "ord_state": order_status['state'],
                    "ord_price": order_status['trades'][0]['price'],
                    "ord_vol": order_status['trades'][0]['volume'],
                    "executed_vol": order_status['executed_volume'],
                    "remaining_vol": order_status['remaining_volume']
                }
        
                order_list.append(order_param)

                # 매매관리정보 변경 처리
                cur02 = conn.cursor()
                upd_param1 = (
                    datetime.fromisoformat(order_status['trades'][0]['created_at']).strftime("%Y%m%d%H%M%S"),
                    order_status['trades'][0]['uuid'],
                    chk_ord['ord_no'],
                    order_status['state'],
                    Decimal(order_status['executed_volume']),
                    Decimal(order_status['remaining_volume']),
                    user_id,
                    datetime.now(),
                    chk_ord['id']
                )
                
                update1 = """UPDATE trade_mng SET 
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
                cur02.execute(update1, upd_param1)
                conn.commit()
                cur02.close()
        
        # 취소 상태인 경우
        elif ord_state == 'cancel':
            if chk_ord['ord_no'] == order_status['uuid']:
                order_param = {
                    "ord_dtm": datetime.fromisoformat(order_status['created_at']).strftime("%Y%m%d%H%M%S"),
                    "ord_no": order_status['uuid'],
                    "prd_nm": order_status['market'],
                    "ord_tp": '01' if order_status['side'] == 'bid' else '02',
                    "ord_state": order_status['state'],
                    "ord_price": order_status['price'],
                    "ord_vol": order_status['volume'],
                    "executed_vol": order_status['executed_volume'],
                    "remaining_vol": order_status['remaining_volume']
                }
        
                order_list.append(order_param)

                # 매매관리정보 변경 처리
                cur02 = conn.cursor()
                upd_param1 = (
                    order_status['state'],
                    Decimal(order_status['executed_volume']),
                    Decimal(order_status['remaining_volume']),
                    user_id,
                    datetime.now(),
                    chk_ord['id']
                )
                
                update1 = """UPDATE trade_mng SET 
                                ord_state = %s,
                                executed_vol = %s,
                                remaining_vol = %s,
                                chgr_id = %s,
                                chg_date = %s
                            WHERE id = %s
                            AND ord_state = 'wait'
                        """
                cur02.execute(update1, upd_param1)
                conn.commit()
                cur02.close()

        else:

            params = {
                'market': chk_ord['prd_nm'],        # 마켓 ID
                'states': chk_ord['ord_state'],     # 'wait', 'watch'
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
                for item in raw_order_list:
                    if chk_ord['ord_no'] == item['uuid']:

                        order_param = {
                                "ord_dtm": datetime.fromisoformat(item['created_at']).strftime("%Y%m%d%H%M%S"),
                                "ord_no": item['uuid'],
                                "prd_nm": item['market'],
                                "ord_tp": '01' if item['side'] == 'bid' else '02',
                                "ord_state": item['state'],
                                "ord_price": item['price'],
                                "ord_vol": item['volume'],
                                "executed_vol": item['executed_volume'],
                                "remaining_vol": item['remaining_volume']
                            }
                
                        order_list.append(order_param)

                        if chk_ord['remaining_vol'] != Decimal(item['remaining_volume']) or chk_ord['executed_vol'] != Decimal(item['executed_volume']):

                            # 매매관리정보 변경 처리
                            cur02 = conn.cursor()
                            upd_param1 = (
                                order_status['state'],
                                Decimal(order_status['executed_volume']),
                                Decimal(order_status['remaining_volume']),
                                user_id,
                                datetime.now(),
                                chk_ord['id']
                            )
                            
                            update1 = """UPDATE trade_mng SET 
                                            ord_state = %s,
                                            executed_vol = %s,
                                            remaining_vol = %s,
                                            chgr_id = %s,
                                            chg_date = %s
                                        WHERE id = %s
                                        AND ord_state = 'wait'
                                    """
                            cur02.execute(update1, upd_param1)
                            conn.commit()
                            cur02.close()
    cur01.close()                            

def proc_trade_mng_hist(cust_num, market_name, conn):

    try:
        cur01 = conn.cursor()
        cur02 = conn.cursor()

        param1 = (cust_num, market_name)

        # 기존 데이터 백업
        insert1 = """
            INSERT INTO trade_mng_hist (
                cust_num, market_name, ord_dtm, ord_no, orgn_ord_no, prd_nm, ord_tp, ord_state, ord_count, ord_expect_totamt, ord_price, ord_vol, ord_amt,
                cut_price, cut_rate, cut_amt, goal_price, goal_rate, goal_amt, margin_vol, executed_vol, remaining_vol, regr_id, reg_date, chgr_id, chg_date
            )
            SELECT 
                cust_num, market_name, ord_dtm, ord_no, orgn_ord_no, prd_nm, ord_tp, ord_state, ord_count, ord_expect_totamt, ord_price, ord_vol, ord_amt,
                cut_price, cut_rate, cut_amt, goal_price, goal_rate, goal_amt, margin_vol, executed_vol, remaining_vol, regr_id, reg_date, chgr_id, chg_date
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

def analyze_data(user, market, trend_type):
    
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
        script_path = "C:\\Project\\frm_batch_svc\\main.py"
    else:
        python_executable = "python3"
        script_path = "/Users/phillseungkorea/Documents/frm_batch_svc/main.py"

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
        
        cur031 = conn.cursor()
        result_31 = []

        # 매매신호정보 및 매매예정정보 조회(주문정보 미처리 매수 대상)
        query31 = """
                    SELECT A.id, split_part(A.prd_nm, '/', 1), A.tr_price, A.support_price, A.regist_price, 
                    (SELECT CASE WHEN count(*) = 0 THEN 1 ELSE count(*) END FROM trade_mng WHERE cust_num = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '/', 1) AND ord_state = 'done' AND ord_tp = '01')
                    , B.prd_nm, B.plan_price, B.plan_vol, B.plan_amt, B.support_price, B.regist_price
                    FROM TR_SIGNAL_INFO A
                    LEFT OUTER JOIN TRADE_PLAN B
                    ON split_part(A.prd_nm, '/', 1) = split_part(B.prd_nm, '-', 2) AND B.plan_tp = 'B1'
                    WHERE A.signal_name = %s
                    AND A.tr_tp = 'B'
                    AND A.tr_state = '02'
                    AND A.buy_order_no IS NULL
                    ORDER BY A.tr_dtm DESC
                """
        param1 = (cust_info['cust_num'], f"TrendLine-{trend_type}",)
        cur031.execute(query31, param1)  
        result_31 = cur031.fetchall()

        trade_list = []

        if result_31 is not None:
            for item in result_31:
                trade_info = {
                    "tr_tp": "B",
                    "tr_state": "02",
                    "id": item[0],
                    "prd_nm": item[1],
                    "tr_price": item[2],
                    "support_price": item[10] if item[10] is not None else item[3],
                    "regist_price": item[11] if item[11] is not None else item[4],
                    "tr_count": item[5],
                    "buy_order_no": None,
                    "plan_amt": 1000000  # 매매예정금액
                }
                trade_list.append(trade_info)

            trade_list_json = json.dumps(trade_list, default=decimal_converter)
            safe_trade_list_json = shlex.quote(trade_list_json)    
            
            os.system(f"{python_executable} {script_path} order-chk {user} {market} {safe_trade_list_json} --work_mm=202503")

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
                
                    cur032 = conn.cursor()
                    result_32 = []
                    
                    # 매매신호정보 및 매매예정정보 조회(주문정보 미처리 매도 대상)
                    query32 = """
                        WITH signal_info AS (
                            SELECT id, prd_nm, tr_price, tr_dtm, support_price, regist_price, tr_count FROM TR_SIGNAL_INFO WHERE TR_TP = 'S' AND TR_STATE = '02' and signal_name = %s AND SELL_ORDER_NO IS null
                            UNION
                            SELECT id, prd_nm, tr_price, tr_dtm, support_price, regist_price, tr_count FROM TR_SIGNAL_INFO WHERE TR_TP = 'B' AND TR_STATE = '22' and signal_name = %s AND SELL_ORDER_NO IS null
                        )
                        SELECT 
                            A.id, split_part(A.prd_nm, '/', 1), A.tr_price, A.support_price, A.regist_price, 
                            (SELECT count(*) FROM trade_mng WHERE cust_num = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '/', 1) AND ord_state = 'done' AND ord_tp = '01'),
                            (SELECT count(*) FROM trade_mng WHERE cust_num = %s AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '/', 1) AND ord_state = 'done' AND ord_tp = '02'),
                            B.prd_nm, B.plan_price, B.plan_vol, B.plan_amt, B.support_price, B.regist_price
                        FROM (
                            SELECT * 
                            FROM signal_info 
                            WHERE split_part(prd_nm, '/', 1) = %s 
                            ORDER BY tr_dtm DESC 
                            LIMIT 1  -- 최신 데이터 한 건만 가져옴
                        ) A
                        LEFT OUTER JOIN TRADE_PLAN B
                        ON split_part(A.prd_nm, '/', 1) = split_part(B.prd_nm, '-', 2) AND B.plan_tp = 'S1'
                    """
                    param2 = (f"TrendLine-{trend_type}", f"TrendLine-{trend_type}", cust_info['cust_num'], cust_info['cust_num'], item['currency'],)
                    cur032.execute(query32, param2)  
                    result_32 = cur032.fetchall()

                    trade_list = []
                    support_price = 0
                    regist_price = 0

                    if result_32 is not None:
                        for trade_signal in result_32:
                            support_price = trade_signal[10] if trade_signal[10] is not None else trade_signal[3]
                            regist_price = trade_signal[11] if trade_signal[11] is not None else trade_signal[4]
                            
                            trade_info = {
                                "tr_tp": "S",
                                "tr_state": "02",
                                "id": trade_signal[0],
                                "prd_nm": trade_signal[1],
                                "tr_price": trade_signal[2],
                                "support_price": support_price,
                                "regist_price": regist_price,
                                "tr_count": int(trade_signal[5]) - int(trade_signal[6]),    # 매수주문건수 - 매도주문건수
                                "sell_order_no": None,
                                "plan_amt": 1000000  # 매매예정금액
                            }
                            trade_list.append(trade_info)

                        trade_list_json = json.dumps(trade_list, default=decimal_converter)
                        safe_trade_list_json = shlex.quote(trade_list_json)    
                        
                        os.system(f"{python_executable} {script_path} order-chk {user} {market} {safe_trade_list_json} --work_mm=202503")

                    cur033 = conn.cursor()
                    result_33 = []    

                    # 잔고정보 조회 : last_order_no, last_buy_count, last_sell_count, loss_price, target_price 설정
                    query33 = """
                        SELECT 
                            (SELECT ord_no FROM trade_mng WHERE cust_num = A.cust_num AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '-', 2) AND ord_state = 'done' ORDER BY ord_dtm DESC LIMIT 1) AS last_order_no,
                            (SELECT count(*) FROM trade_mng WHERE cust_num = A.cust_num AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '-', 2) AND ord_state = 'done' AND ord_tp = '01') AS last_buy_count,
                            (SELECT count(*) FROM trade_mng WHERE cust_num = A.cust_num AND split_part(prd_nm, '-', 2) = split_part(A.prd_nm, '-', 2) AND ord_state = 'done' AND ord_tp = '02') AS last_sell_count,
                            COALESCE(B.support_price, 0) AS loss_price,
                            COALESCE(B.regist_price, 0) AS target_price
                        FROM balance_info A
                        LEFT OUTER JOIN TRADE_PLAN B
                        ON split_part(A.prd_nm, '-', 2) = split_part(B.prd_nm, '-', 2) AND B.plan_tp = 'S1'
                        WHERE A.cust_num = %s
                        AND split_part(A.prd_nm, '-', 2) = %s 
                    """
                    param3 = (cust_info['cust_num'], item['currency'],)
                    cur033.execute(query33, param3)  
                    result_33 = cur033.fetchall()
                    
                    last_order_no = None
                    last_buy_count = 0
                    last_sell_count = 0
                    loss_price = 0
                    target_price = 0

                    if result_33 is not None:
                        for balance_info in result_33:
                            last_order_no = balance_info[0]
                            last_buy_count = balance_info[1]
                            last_sell_count = balance_info[2]
                            loss_price = balance_info[3]
                            target_price = balance_info[4]

                    
                    # 최종 주문관리정보 매수 체결건 대상의 주문가 5% 이상 수익여부 체크 -> 조건해당시 해당 주문의 절반 매도 주문
                    # if Decimal(item["current_amt"]) >= Decimal(item["price"]) * Decimal("1.05"):


                    # 잔고정보 현행화
                    ins_param1 = (
                        price,
                        volume,
                        amt,
                        loss_profit_rate,
                        last_order_no,
                        last_buy_count,
                        last_sell_count,
                        current_price,
                        current_amt,
                        target_price,
                        loss_price,
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
                        last_order_no,
                        last_buy_count,
                        last_sell_count,
                        current_price,
                        current_amt,
                        target_price,
                        loss_price,
                        'Y',
                        user_id,
                        datetime.now(),
                        user_id,
                        datetime.now(),
                    )
                    
                    insert1 = "with upsert as (update balance_info set hold_price = %s, hold_volume = %s, hold_amt = %s, loss_profit_rate = %s, last_order_no = %s, last_buy_count = %s, last_sell_count = %s, current_price = %s, current_amt = %s, target_price = %s, loss_price = %s, proc_yn = 'Y', chgr_id = %s, chg_date = %s where cust_num = %s and market_name = %s and prd_nm = %s returning * ) insert into balance_info(acct_no, cust_num, market_name, prd_nm, hold_price, hold_volume, hold_amt, loss_profit_rate, last_order_no, last_buy_count, last_sell_count, current_price, current_amt, target_price, loss_price, proc_yn, regr_id, reg_date, chgr_id, chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
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

        # 잔고정보 미존재 대상 매매관리정보 백업 처리
        proc_trade_mng_hist(cust_info['cust_num'], cust_info['market_name'], conn)
    
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