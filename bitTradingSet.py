import psycopg2
from datetime import datetime, timedelta

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

def create_bit_trading_trail():
    today = datetime.now().strftime("%Y%m%d")
    prev_day = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    now_time = datetime.now().strftime("%H%M%S")
    user_id = "TRAIL_AUTO"

    nickname_list = [
        {"cust_nm": "phills2", "market_name": "UPBIT"},
        {"cust_nm": "mama", "market_name": "UPBIT"},
        {"cust_nm": "honey", "market_name": "UPBIT"},
        {"cust_nm": "phills2", "market_name": "BITHUMB"},
        {"cust_nm": "mama", "market_name": "BITHUMB"},
        {"cust_nm": "honey", "market_name": "BITHUMB"},
    ]

    for nick in nickname_list:
        try:
            cur1 = conn.cursor()

            # 잔고정보 조회 (KRW 현금 제외, 보유수량 존재 대상) + 전일 매매추적정보(trail_tp) 승계
            # 전일 trail_tp IN ('1','L') 대상은 동일한 trail_tp로, trail_tp = '3' 대상은 'L'로 승격하여 오늘자 생성
            select1 = """
                SELECT
                    A.acct_no, A.cust_num, A.market_name, A.prd_nm,
                    A.hold_price, A.hold_volume, A.hold_amt,
                    A.loss_price, A.target_price, A.exit_price,
                    T.trail_tp AS prev_trail_tp
                FROM balance_info A
                JOIN cust_mng B ON A.cust_num = B.cust_num
                LEFT JOIN LATERAL (
                    SELECT trail_tp
                    FROM bit_trading_trail
                    WHERE cust_num = A.cust_num
                    AND market_name = A.market_name
                    AND prd_nm = A.prd_nm
                    AND trail_day = %s
                    AND trail_tp IN ('1','2','3','L','P','C','U')
                    ORDER BY trail_dtm DESC
                    LIMIT 1
                ) T ON true
                WHERE B.cust_nm = %s
                AND B.market_name = %s
                AND A.prd_nm != 'KRW-KRW'
                AND (A.trading_plan IS NULL OR A.trading_plan NOT IN ('i', 'h'))
                AND A.hold_volume > 0
            """
            cur1.execute(select1, (prev_day, nick['cust_nm'], nick['market_name']))
            result_1 = cur1.fetchall()
            cur1.close()

            if not result_1:
                print(f"[{today}-{nick['cust_nm']}-{nick['market_name']}] 보유잔고 없음, bit_trading_trail 생성 대상 없음.")
                continue

            cur2 = conn.cursor()

            # 매매추적정보 생성 : 동일 일자(trail_day)에 이미 생성된 종목은 재생성하지 않음
            insert_query1 = """
                INSERT INTO bit_trading_trail (
                    acct_no, cust_num, market_name, prd_nm,
                    trail_day, trail_dtm, trail_tp,
                    basic_price, basic_vol, basic_amt,
                    stop_price, target_price, exit_price, trade_tp, loss_amt,
                    regr_id, reg_date, chgr_id, chg_date
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, 
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (cust_num, market_name, prd_nm, trail_day, trail_tp) DO NOTHING
            """

            inserted_count = 0

            for row in result_1:
                acct_no, cust_num, market_name, prd_nm, hold_price, hold_volume, hold_amt, loss_price, target_price, exit_price, prev_trail_tp = row

                basic_price = float(hold_price) if hold_price else 0
                basic_vol = float(hold_volume) if hold_volume else 0
                stop_price = float(loss_price) if loss_price else 0
                trail_target_price = float(target_price) if target_price else 0
                trail_exit_price = float(exit_price) if exit_price else 0
                loss_amt = int((basic_price - stop_price) * basic_vol) if stop_price > 0 else 0

                trail_tp = 'L' if prev_trail_tp in ('3', 'L') else '1'

                try:
                    cur2.execute(insert_query1, (
                        acct_no, cust_num, market_name, prd_nm,
                        today, '090000' if market_name == 'UPBIT' else '000000', trail_tp,
                        basic_price, basic_vol, hold_amt,
                        stop_price, trail_target_price, trail_exit_price, 'M', loss_amt,
                        user_id, datetime.now(), user_id, datetime.now()
                    ))
                    if cur2.rowcount > 0:
                        inserted_count += 1
                except Exception as e:
                    print(f"[{today}-{nick['cust_nm']}-{nick['market_name']}] bit_trading_trail insert 오류 ({prd_nm}): {e}")

            conn.commit()
            cur2.close()

            print(f"[{today}-{nick['cust_nm']}-{nick['market_name']}] bit_trading_trail 생성 완료. (대상 {len(result_1)}건, 생성 {inserted_count}건)")

        except Exception as e:
            conn.rollback()
            print(f"[{today}-{nick['cust_nm']}-{nick['market_name']}] bit_trading_trail 처리 중 예외 발생: {e}")

# 실행
if __name__ == "__main__":
    create_bit_trading_trail()
    conn.close()
