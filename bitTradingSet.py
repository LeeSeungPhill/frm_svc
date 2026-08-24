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

def get_business_day(now=None):
    """trail_day 기준일 : 당일 09:00부터 익일 08:59까지를 하나의 영업일로 취급한다.
    (bitTradingTrail.py의 get_business_day()와 동일 규칙)"""
    now = now or datetime.now()
    if now.hour < 9:
        now = now - timedelta(days=1)
    return now

def update_bit_fund_mng(acct_no, cust_num, market_name, prev_day):
    """
    simul/kis_trading_set_simul.py 의 dly_acct_balance_simul 전일 집계 업데이트를 참고하되,
    시뮬레이션(가상 롤포워드 계산) 대신 dly_balance_info_backup.py 가 매일 적재하는 실제 잔고 스냅샷
    (dly_balance_info) 을 그대로 집계해 public.bit_fund_mng 에 전일자(prev_day) 자산현황을 반영한다.
    """
    cur1 = conn.cursor()
    cur1.execute("""
        SELECT prd_nm, hold_amt, current_amt, hold_volume
        FROM dly_balance_info
        WHERE acct_no = %s AND cust_num = %s AND market_name = %s AND sday = %s
    """, (acct_no, cust_num, market_name, prev_day))
    rows = cur1.fetchall()
    cur1.close()

    if not rows:
        print(f"[{prev_day}-{cust_num}-{market_name}] dly_balance_info 데이터 없음 → bit_fund_mng 스킵")
        return

    cash_amt = 0
    pchs_amt = 0
    evlu_amt = 0
    for prd_nm, hold_amt, current_amt, hold_volume in rows:
        if prd_nm == 'KRW-KRW':
            cash_amt = int(hold_amt or 0)
        elif float(hold_volume or 0) > 0:
            pchs_amt += int(hold_amt or 0)
            evlu_amt += int(current_amt or 0)

    evlu_pfls_amt = evlu_amt - pchs_amt        # 평가손익 합계
    user_evlu_amt = evlu_amt                   # 평가금액 합계 (= 보유원가 + 평가손익)
    tot_evlu_amt = cash_amt + evlu_amt          # 총자산 = 현금 + 평가금액
    nass_amt = tot_evlu_amt                     # 순자산(신용/대출 없음 → 총자산과 동일)

    # 직전 집계일(dt < prev_day) 대비 자산증감액
    cur2 = conn.cursor()
    cur2.execute("""
        SELECT tot_evlu_amt FROM public.bit_fund_mng
        WHERE acct_no = %s AND cust_num = %s AND market_name = %s AND dt < %s
        ORDER BY dt DESC LIMIT 1
    """, (acct_no, cust_num, market_name, prev_day))
    prev_row = cur2.fetchone()
    cur2.close()
    prev_tot_evlu_amt = int(prev_row[0]) if prev_row else tot_evlu_amt
    asst_icdc_amt = tot_evlu_amt - prev_tot_evlu_amt

    cur3 = conn.cursor()
    try:
        cur3.execute("""
            INSERT INTO public.bit_fund_mng (
                acct_no, cust_num, market_name, dt,
                dnca_tot_amt, prvs_excc_amt, user_evlu_amt, tot_evlu_amt, nass_amt,
                pchs_amt, evlu_amt, evlu_pfls_amt, asst_icdc_amt, last_chg_date
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
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
                last_chg_date = EXCLUDED.last_chg_date
        """, (
            acct_no, cust_num, market_name, prev_day,
            cash_amt, cash_amt, user_evlu_amt, tot_evlu_amt, nass_amt,
            pchs_amt, evlu_amt, evlu_pfls_amt, asst_icdc_amt, datetime.now()
        ))
        conn.commit()
        print(
            f"[{prev_day}-{cust_num}-{market_name}] bit_fund_mng 갱신 완료. "
            f"현금:{cash_amt:,} 보유원가:{pchs_amt:,} 평가금액:{evlu_amt:,} 평가손익:{evlu_pfls_amt:,} "
            f"총자산:{tot_evlu_amt:,} 증감:{asst_icdc_amt:,}"
        )
    except Exception as e:
        conn.rollback()
        print(f"[{prev_day}-{cust_num}-{market_name}] bit_fund_mng 갱신 오류: {e}")
    finally:
        cur3.close()

def create_bit_trading_trail():
    biz_dt = get_business_day()
    today = biz_dt.strftime("%Y%m%d")
    prev_day = (biz_dt - timedelta(days=1)).strftime("%Y%m%d")
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
            cur0 = conn.cursor()
            cur0.execute("SELECT cust_num, acct_no FROM cust_mng WHERE cust_nm = %s AND market_name = %s", (nick['cust_nm'], nick['market_name']))
            cust_row = cur0.fetchone()
            cur0.close()

            if not cust_row:
                print(f"[{today}-{nick['cust_nm']}-{nick['market_name']}] 고객정보 없음.")
                continue

            nick_cust_num, nick_acct_no = cust_row

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
            else:
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

            # 매매추적정보 생성 후 : 전일자(prev_day) 자산현황을 bit_fund_mng 에 집계 반영
            update_bit_fund_mng(nick_acct_no, nick_cust_num, nick['market_name'], prev_day)

        except Exception as e:
            conn.rollback()
            print(f"[{today}-{nick['cust_nm']}-{nick['market_name']}] bit_trading_trail 처리 중 예외 발생: {e}")

# 실행
if __name__ == "__main__":
    create_bit_trading_trail()
    conn.close()
