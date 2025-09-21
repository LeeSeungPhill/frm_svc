from datetime import datetime
import psycopg2
import schedule
import time

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

def analyze_data():
    today = datetime.now().strftime("%Y%m%d")
    nickname_list = [{"cust_nm": "honey", "market_name": "UPBIT"}, {"cust_nm": "phills2", "market_name": "UPBIT"}, {"cust_nm": "mama", "market_name": "UPBIT"}, {"cust_nm": "honey", "market_name": "BITHUMB"}, {"cust_nm": "phills2", "market_name": "BITHUMB"}]

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
                    proc_yn, 
                    regr_id,
                    reg_date, 
                    chgr_id, 
                    chg_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sday, acct_no, cust_num, market_name, prd_nm) DO NOTHING
            """

            for row in result_1:
                acct_no, cust_num, market_name, prd_nm, hold_price, hold_volume, old_amt, loss_profit_rate, last_order_no, last_buy_count, last_sell_count, current_price, current_amt, loss_price, target_price, proc_yn, regr_id, reg_date, chgr_id, chg_date = row
                try:
                    cur2.execute(insert_query1, (
                        today, acct_no, cust_num, market_name, prd_nm, hold_price, hold_volume, old_amt, loss_profit_rate, last_order_no, last_buy_count, last_sell_count, current_price, current_amt, loss_price, target_price, proc_yn, regr_id, reg_date, chgr_id, chg_date
                    ))
                except Exception as e:
                    print(f"{today}[{nick['cust_nm']}-{nick['market_name']}] Error dly_balance_info inserting row {row}: {e}")

            conn.commit()
            cur2.close()
            print(f"{today}[{nick['cust_nm']}-{nick['market_name']}] Insert dly_balance_info completed. ({len(result_1)} rows processed)")
            

        except Exception as e:
            print(f"[dly_balance_info] 실행 중 예외 발생: {e}")
        
# 실행
if __name__ == "__main__":
    # print("잔고정보 백업 작업을 매일 실행합니다...")

    analyze_data()
    # 매일 오전 9시에 실행되도록 스케줄 설정
    # schedule.every().day.at("09:00").do(analyze_data)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)