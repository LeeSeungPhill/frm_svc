from datetime import datetime, timedelta
import psycopg2
import requests
import schedule
import time

UPBIT_API = "https://api.upbit.com"
BITHUMB_API = "https://api.bithumb.com"

# 데이터베이스 연결 정보
DB_NAME = "universe"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "localhost"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

# PostgreSQL 데이터베이스에 연결
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

def get_day_high_low(market_name, prd_nm, cache):
    # 수행일 일봉 기준 최고가, 최저가 조회 (거래소/종목별 1회만 호출)
    if not prd_nm or prd_nm == 'KRW-KRW':
        return None, None

    key = (market_name, prd_nm)
    if key in cache:
        return cache[key]

    base_url = UPBIT_API if market_name == 'UPBIT' else BITHUMB_API
    high_price, low_price = None, None
    try:
        res = requests.get(base_url + "/v1/candles/days", params={"market": prd_nm, "count": 1}, timeout=5).json()
        if isinstance(res, list) and len(res) > 0:
            high_price = float(res[0]['high_price'])
            low_price = float(res[0]['low_price'])
        else:
            print(f"[{market_name}-{prd_nm}] 일봉 조회 결과 없음: {res}")
    except Exception as e:
        print(f"[{market_name}-{prd_nm}] 일봉 조회 오류: {e}")

    time.sleep(0.1)  # API 호출 제한 대응
    cache[key] = (high_price, low_price)
    return high_price, low_price

def get_target_market(now):
    # 일봉 마감 직전 시간대에 해당 거래소만 처리
    # UPBIT : 09:00 일봉 마감 → 8시대 수행 (전일 09:00 ~ 당일 08:59 일봉, 기준일자 전일)
    # BITHUMB : 00:00 일봉 마감 → 23시대 수행 (당일 00:00 ~ 23:59 일봉, 기준일자 당일)
    if now.hour == 8:
        return 'UPBIT'
    if now.hour == 23:
        return 'BITHUMB'
    return None

def get_business_day(market_name, now):
    # UPBIT는 당일 09:00부터 익일 08:59까지, BITHUMB는 당일 00:00부터 23:59까지를 하나의 영업일로 취급 (bitTradingSet.py와 동일 규칙)
    if market_name == 'UPBIT' and now.hour < 9:
        now = now - timedelta(days=1)
    return now.strftime("%Y%m%d")

def analyze_data():
    now = datetime.now()
    target_market = get_target_market(now)
    if target_market is None:
        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} 처리 대상 시간대 아님 (UPBIT: 8시대, BITHUMB: 23시대)")
        return

    today = get_business_day(target_market, now)
    print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} [{target_market}] 기준일자 {today} 잔고정보 백업 시작")

    candle_cache = {}
    nickname_list = [{"cust_nm": "honey", "market_name": "UPBIT"}, {"cust_nm": "phills2", "market_name": "UPBIT"}, {"cust_nm": "mama", "market_name": "UPBIT"}, {"cust_nm": "honey", "market_name": "BITHUMB"}, {"cust_nm": "phills2", "market_name": "BITHUMB"}]
    nickname_list = [nick for nick in nickname_list if nick['market_name'] == target_market]

    for nick in nickname_list:
        try:
            cur1 = conn.cursor()
            result_1 = []

            select1 = """
                select 
                    A.acct_no, 
                    A.cust_num,
                    A.market_name,
                    A.prd_nm,
                    A.hold_price,
                    A.hold_volume,
                    A.hold_amt,
                    A.loss_profit_rate,
                    A.last_order_no,
                    A.last_buy_count,
                    A.last_sell_count,
                    A.current_price, 
                    A.current_amt,
                    A.loss_price,
                    A.target_price,
                    A.stop_price, 
                    A.action_price,
                    A.exit_price,
                    A.trading_plan,
                    A.proc_yn, 
                    A.regr_id,
                    A.reg_date, 
                    A.chgr_id, 
                    A.chg_date
                from balance_info A, cust_mng B
                where A.cust_num = B.cust_num
                and B.cust_nm = %s
                and B.market_name = %s
            """

            param1 = (nick['cust_nm'], nick['market_name'],)
            cur1.execute(select1, param1)  
            result_1 = cur1.fetchall()
            cur1.close()

            if not result_1:
                print(f"[{nick}] No balance data found.")

            cur2 = conn.cursor()

            insert_query1 = """
                INSERT INTO dly_balance_info (
                    sday,
                    acct_no, 
                    cust_num,
                    market_name,
                    prd_nm,
                    hold_price,
                    hold_volume,
                    hold_amt,
                    loss_profit_rate,
                    last_order_no,
                    last_buy_count,
                    last_sell_count,
                    current_price, 
                    current_amt,
                    loss_price,
                    target_price, 
                    stop_price,
                    action_price,
                    exit_price,
                    trading_plan,
                    proc_yn, 
                    regr_id,
                    reg_date, 
                    chgr_id, 
                    chg_date,
                    high_price,
                    low_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sday, acct_no, cust_num, market_name, prd_nm) DO NOTHING
            """

            for row in result_1:
                acct_no, cust_num, market_name, prd_nm, hold_price, hold_volume, old_amt, loss_profit_rate, last_order_no, last_buy_count, last_sell_count, current_price, current_amt, loss_price, target_price, stop_price, action_price, exit_price, trading_plan, proc_yn, regr_id, reg_date, chgr_id, chg_date = row
                high_price, low_price = get_day_high_low(market_name, prd_nm, candle_cache)
                try:
                    cur2.execute(insert_query1, (
                        today, acct_no, cust_num, market_name, prd_nm, hold_price, hold_volume, old_amt, loss_profit_rate, last_order_no, last_buy_count, last_sell_count, current_price, current_amt, loss_price, target_price, stop_price, action_price, exit_price, trading_plan, proc_yn, regr_id, reg_date, chgr_id, chg_date, high_price, low_price
                    ))
                except Exception as e:
                    print(f"{today}[{nick['cust_nm']}-{nick['market_name']}] Error dly_balance_info inserting row {row}: {e}")

                # 당일 고가가 action_price 초과시 고가로, 당일 저가가 stop_price/exit_price 이탈시 저가로 변경
                new_action_price = high_price if high_price is not None and action_price and high_price > action_price else action_price
                new_stop_price = low_price if low_price is not None and stop_price and low_price < stop_price else stop_price
                new_exit_price = low_price if low_price is not None and exit_price and low_price < exit_price else exit_price

                if (new_action_price, new_stop_price, new_exit_price) != (action_price, stop_price, exit_price):
                    try:
                        cur2.execute("""
                            UPDATE balance_info
                            SET action_price = %s, stop_price = %s, exit_price = %s, chgr_id = %s, chg_date = %s
                            WHERE acct_no = %s AND cust_num = %s AND market_name = %s AND prd_nm = %s
                        """, (new_action_price, new_stop_price, new_exit_price, nick['cust_nm'], datetime.now(), acct_no, cust_num, market_name, prd_nm))
                        print(f"{today}[{nick['cust_nm']}-{nick['market_name']}] {prd_nm} balance_info 변경 : "
                              f"action_price {action_price} → {new_action_price}, stop_price {stop_price} → {new_stop_price}, exit_price {exit_price} → {new_exit_price}")
                    except Exception as e:
                        print(f"{today}[{nick['cust_nm']}-{nick['market_name']}] Error balance_info updating {prd_nm}: {e}")

            conn.commit()
            cur2.close()
            print(f"{today}[{nick['cust_nm']}-{nick['market_name']}] Insert dly_balance_info completed. ({len(result_1)} rows processed)")
            

        except Exception as e:
            print(f"[dly_balance_info] 실행 중 예외 발생: {e}")
        
# 실행
if __name__ == "__main__":
    # print("잔고정보 백업 작업을 매일 실행합니다...")

    analyze_data()
    # 거래소별 일봉 마감 직전 실행되도록 스케줄 설정 (UPBIT: 08:50, BITHUMB: 23:50)
    # schedule.every().day.at("08:50").do(analyze_data)
    # schedule.every().day.at("23:50").do(analyze_data)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)