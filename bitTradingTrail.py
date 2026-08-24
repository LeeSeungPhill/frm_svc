import jwt
import hashlib
import os
import json
import time
import uuid
import requests
import psycopg2
import slack_sdk
from slack_sdk.errors import SlackApiError
from urllib.parse import urlencode, unquote
from decimal import Decimal
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

# ─────────────────────────────────────────
# kis_trading_trail_vol_state.py 의 trail_tp='1'/'2'/'L' 매매추적 로직을 업비트(24시간 시장)에 맞게 이식한다.
# - "영업일" 경계는 09:00 ~ 익일 08:59 (bitTradingSet.py의 UPBIT trail_dtm='090000' 관례와 동일)
# - KIS는 하루 1회 09:00부터 현재까지 1분봉을 리플레이하지만, 본 배치는 실제로 1분마다 외부(cron)에서
#   호출되므로 "가장 최근 완성봉만 처리 + 상태(기준봉/이탈대기/알림이력)는 bit_trading_trail에 지속 저장"하는
#   구조로 이식한다.
# - 상한가/호가단위 등 KRX 특화 로직은 대상이 아니므로 제외하고, 매도는 항상 시장가로 즉시 체결한다.
# - 코스피/코스닥 시장흐름 대신 KRW-BTC/KRW-ETH 단기추세(public.bit_fund_mng.btc_short/eth_short)를 사용한다.
#   코인이 BTC 자신이면 btc_short, 그 외(알트코인)는 eth_short를 단기하락(_short_market_down) 판단에 사용한다.
# - 업비트는 10분봉을 API에서 직접 제공하므로, KIS처럼 1분봉을 모아 10분봉을 직접 집계할 필요가 없다.
# ─────────────────────────────────────────

DRY_RUN = True  # True인 동안은 실제 매도 주문을 내지 않고 판단 결과만 로그/Slack으로 알린다. 운영 전환 시 False로 변경.

UPBIT_API = os.getenv("UPBIT_API")

# 데이터베이스 연결 정보
DB_NAME = "universe"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "localhost"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

USER_ID = "TRAIL_AUTO"

# Slack 메세지 연동
SLACK_BOT_TOKEN1 = os.environ['SLACK_BOT_TOKEN1']
SLACK_BOT_TOKEN2 = os.environ['SLACK_BOT_TOKEN2']
SLACK_BOT_TOKEN3 = os.environ['SLACK_BOT_TOKEN3']
SLACK_BOT_TOKEN4 = os.environ['SLACK_BOT_TOKEN4']
slack_client = slack_sdk.WebClient(token=SLACK_BOT_TOKEN1 + SLACK_BOT_TOKEN2 + SLACK_BOT_TOKEN3 + SLACK_BOT_TOKEN4)

# 안전마진(매입가 대비), 고점 되돌림 허용비율, 거래량서지 기준 — kis_trading_trail_vol_state.py 상수를 그대로 사용
SAFETY_MARGIN_RATE = 0.05
PROFIT_LOCK_FLOOR_RATE = 0.10          # trail_tp='L' 수익잠금 최소마진(매입가 대비 +10%)
PEAK_RETRACEMENT_RATE = 0.5
LATE_DAY_RETRACEMENT_RATE = 0.3
LATE_DAY_START = "0800"                # 08:00 이후: 되돌림 허용비율 강화
DOWNTREND_SELL_START = "0840"          # 08:40 이후: 하락추세 지속 시 강제 전량매도
DOWNTREND_WARN_START = "0910"
DOWNTREND_WARN_END = "0830"
PREMARKET_END = "0910"                 # 09:00~09:10 미처리
VOL_SURGE_N = 20
VOL_SURGE_MULT = 2.0
PROFIT_LOCK_GAIN_PCT = 15.0            # trail_tp='L' 당일 고가가 전일종가 대비 이 이상이면 수익잠금 트레일링 진입
TREND_LOOKBACK_DAYS = 30
DEFAULT_TP2_SELL_RATIO = 50.0          # trail_tp='2' 매도 트리거 기본 매도비율(trail_plan 미설정 시)
DEFAULT_FULL_SELL_RATIO = 100.0

REASON_LABEL = {
    'STOP': '이탈가 이탈',
    'EXIT': '최종이탈가 이탈',
    'BASE_BREAK_SAFETY': '기준봉 저가 이탈(안전마진 이하)',
    'PEAK_RETRACE': '고점 대비 되돌림',
    'BASE_BREAK': '기준봉 저가 이탈',
    'RETRACE': '고점 대비 되돌림(수익잠금)',
    'DOWNTREND': '하락추세 지속 강제매도',
}

JSONB_FIELDS = {'wait_state', 'last_alert_keys'}


def send_slack_message(channel, message):
    try:
        slack_client.chat_postMessage(channel=channel, text=message)
    except SlackApiError as e:
        print(f"Slack 메시지 전송 실패: {e.response['error']}")


def format_number(value):
    try:
        return f"{float(value):,.2f}" if isinstance(value, float) else f"{int(value):,}"
    except Exception:
        return str(value)


# ─────────────────────────────────────────
# 업비트 인증/시세/주문
# ─────────────────────────────────────────

def auth_headers(access_key, secret_key, params=None):
    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
    }
    if params:
        query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")
        m = hashlib.sha512()
        m.update(query_string)
        payload['query_hash'] = m.hexdigest()
        payload['query_hash_alg'] = 'SHA512'
    jwt_token = jwt.encode(payload, secret_key)
    return {'Authorization': 'Bearer {}'.format(jwt_token)}

def fetch_minute_candles(code, unit, count):
    try:
        res = requests.get(UPBIT_API + f"/v1/candles/minutes/{unit}", params={"market": "KRW-" + code, "count": count}).json()
        return res if isinstance(res, list) else []
    except Exception as e:
        print(f"[{code}] {unit}분봉 조회 오류: {e}")
        return []

def fetch_day_candles(code, count=2):
    try:
        res = requests.get(UPBIT_API + "/v1/candles/days", params={"market": "KRW-" + code, "count": count}).json()
        return res if isinstance(res, list) else []
    except Exception as e:
        print(f"[{code}] 일봉 조회 오류: {e}")
        return []

def fetch_10min_series(code, count=41):
    raw = fetch_minute_candles(code, 10, count)
    if not raw:
        return []
    raw_sorted = sorted(raw, key=lambda c: c['candle_date_time_kst'])
    now = datetime.now()
    series = []
    for c in raw_sorted:
        key = datetime.strptime(c['candle_date_time_kst'], "%Y-%m-%dT%H:%M:%S")
        if key + timedelta(minutes=10) > now:
            continue  # 아직 형성 중인 10분봉은 제외
        series.append({
            'key': key,
            'high': float(c['high_price']),
            'low': float(c['low_price']),
            'close': float(c['trade_price']),
            'volume': float(c['candle_acc_trade_volume']),
        })
    return series

def get_prev_day_info(code):
    days = fetch_day_candles(code, count=2)
    if len(days) < 2:
        return None
    days_sorted = sorted(days, key=lambda d: d['candle_date_time_kst'])
    prev = days_sorted[-2]
    return {'volume': float(prev['candle_acc_trade_volume']), 'close_price': float(prev['trade_price'])}

def calc_peak_trough_trend(highs, lows, dates):
    n = len(highs)
    if n < 3:
        return None
    high_pts = [None] * n
    low_pts = [None] * n
    for i in range(1, n - 1):
        if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
            high_pts[i] = highs[i]
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            low_pts[i] = lows[i]
    trends = []
    last_high, last_low = None, None
    for i in range(n):
        if high_pts[i] is not None:
            last_high = high_pts[i]
        if low_pts[i] is not None:
            last_low = low_pts[i]
        if last_high is not None and highs[i] > last_high:
            trends.append('Uptrend')
        elif last_low is not None and lows[i] < last_low:
            trends.append('Downtrend')
        else:
            trends.append('Sideways')
    cur_trend = trends[-1]
    start_idx = n - 1
    while start_idx > 0 and trends[start_idx - 1] == cur_trend:
        start_idx -= 1
    if cur_trend == 'Downtrend':
        ref_price = last_low
    elif cur_trend == 'Uptrend':
        ref_price = last_high
    else:
        ref_price = None
    return {'trend': cur_trend, 'start_date': dates[start_idx], 'ref_price': ref_price}

def get_coin_trend(code):
    days = fetch_day_candles(code, count=TREND_LOOKBACK_DAYS)
    if len(days) < 3:
        return None
    days_sorted = sorted(days, key=lambda d: d['candle_date_time_kst'])
    highs = [float(d['high_price']) for d in days_sorted]
    lows = [float(d['low_price']) for d in days_sorted]
    dates = [d['candle_date_time_kst'][:10] for d in days_sorted]
    return calc_peak_trough_trend(highs, lows, dates)

def is_tenmin_vol_surge(series, target_key, n=VOL_SURGE_N, mult=VOL_SURGE_MULT):
    idx = next((i for i, c in enumerate(series) if c['key'] == target_key), None)
    if idx is None:
        return False, 0, 0
    prev = series[max(0, idx - n):idx]
    if not prev:
        return False, 0, 0
    cur_vol = series[idx]['volume']
    avg_vol = sum(c['volume'] for c in prev) / len(prev)
    return cur_vol >= avg_vol * mult, cur_vol, avg_vol

def get_available_volume(access_key, secret_key, code):
    try:
        headers = auth_headers(access_key, secret_key)
        accounts = requests.get(UPBIT_API + '/v1/accounts', headers=headers).json()
        if isinstance(accounts, list):
            for item in accounts:
                if item.get('currency') == code:
                    return float(item['balance'])
    except Exception as e:
        print(f"[{code}] 잔고 조회 오류: {e}")
    return 0

def place_market_sell(access_key, secret_key, code, volume):
    params = {
        'market': "KRW-" + code,
        'side': 'ask',
        'ord_type': 'market',
        'volume': str(volume),
    }
    headers = auth_headers(access_key, secret_key, params)
    res = requests.post(UPBIT_API + '/v1/orders', json=params, headers=headers).json()
    return res

def get_order(access_key, secret_key, order_uuid):
    params = {"uuid": order_uuid}
    headers = auth_headers(access_key, secret_key, params)
    res = requests.get(UPBIT_API + "/v1/order", params=params, headers=headers).json()
    return res


# ─────────────────────────────────────────
# 영업일/시장흐름
# ─────────────────────────────────────────

def get_business_day(market_name, now=None):
    """trail_day 기준일 : UPBIT는 당일 09:00부터 익일 08:59까지, BITHUMB는 당일 00:00부터 23:59까지를
    하나의 영업일로 취급한다. (bitTradingSet.py의 get_business_day()와 동일 규칙)"""
    now = now or datetime.now()
    if market_name == 'UPBIT' and now.hour < 9:
        return (now - timedelta(days=1)).strftime("%Y%m%d")
    return now.strftime("%Y%m%d")

def get_10min_key(dt):
    return dt.replace(minute=(dt.minute // 10) * 10, second=0, microsecond=0)

def get_short_market_down(conn, acct_no, cust_num, market, code, trail_day):
    proxy_col = 'btc_short' if code == 'BTC' else 'eth_short'
    cur = conn.cursor()
    cur.execute(f"""
        SELECT {proxy_col} FROM public.bit_fund_mng
        WHERE acct_no = %s AND cust_num = %s AND market_name = %s AND dt <= %s
        ORDER BY dt DESC LIMIT 1
    """, (acct_no, cust_num, market, trail_day))
    row = cur.fetchone()
    cur.close()
    return bool(row and row[0] == '02')


# ─────────────────────────────────────────
# bit_trading_trail 상태 저장/조회
# ─────────────────────────────────────────

def update_trail_row(conn, trail_id, **fields):
    if not fields:
        return
    fields = dict(fields)
    fields['chg_date'] = datetime.now()
    fields.setdefault('chgr_id', USER_ID)
    set_parts = []
    values = []
    for k, v in fields.items():
        if k in JSONB_FIELDS:
            set_parts.append(f"{k} = %s::jsonb")
            values.append(json.dumps(v))
        else:
            set_parts.append(f"{k} = %s")
            values.append(v)
    values.append(trail_id)
    cur = conn.cursor()
    cur.execute(f"UPDATE bit_trading_trail SET {', '.join(set_parts)} WHERE id = %s", values)
    conn.commit()
    cur.close()

def dedup_trail_rows(conn, cust_num, market, trail_day):
    """동일 종목(prd_nm) 중복 활성건 중 최신 1건만 남기고 나머지는 trail_tp='Y' 처리."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE bit_trading_trail t
        SET trail_tp = 'Y', chgr_id = %s, chg_date = %s
        FROM (
            SELECT id, row_number() OVER (PARTITION BY prd_nm ORDER BY id DESC) AS rn
            FROM bit_trading_trail
            WHERE cust_num = %s AND market_name = %s AND trail_day = %s AND trail_tp IN ('1', '2', 'L')
        ) sub
        WHERE t.id = sub.id AND sub.rn > 1
    """, (USER_ID, datetime.now(), cust_num, market, trail_day))
    conn.commit()
    cur.close()

def refresh_basic_from_balance(conn, cust_num, market, trail_day):
    """잔고정보(balance_info) 기준으로 매매추적 활성건의 basic_price/basic_vol/basic_amt 현행화."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE bit_trading_trail t
        SET basic_price = b.hold_price, basic_vol = b.hold_volume, basic_amt = b.hold_amt, chg_date = %s
        FROM balance_info b
        WHERE b.cust_num = t.cust_num AND b.market_name = t.market_name AND b.prd_nm = t.prd_nm
        AND t.cust_num = %s AND t.market_name = %s AND t.trail_day = %s AND t.trail_tp IN ('1', '2', 'L')
        AND b.hold_volume > 0
    """, (datetime.now(), cust_num, market, trail_day))
    conn.commit()
    cur.close()


# ─────────────────────────────────────────
# 매도 실행 (공통) — 시장가 전량/부분 매도, trade_mng 생성, bit_trading_trail 결과 반영
# ─────────────────────────────────────────

def execute_sell(conn, ctx, ratio_pct, trade_result, reason, **extra_state):
    code = ctx['code']
    market = ctx['market']
    user = ctx['user']
    label = REASON_LABEL.get(trade_result, trade_result)

    if DRY_RUN:
        msg = f"[DRY-RUN] -{user}-[{market}] {code} {label} 매도 판단 : {reason} (매도비율:{ratio_pct:.0f}%)"
        print(msg)
        send_slack_message("#매매신호", msg)
        return

    access_key = ctx['access_key']
    secret_key = ctx['secret_key']
    basic_price = ctx['basic_price']
    basic_vol = ctx['basic_vol']
    trail_id = ctx['trail_id']
    cust_num = ctx['cust_num']

    ratio_pct = min(max(float(ratio_pct), 0), 100)
    intended_qty = basic_vol * (ratio_pct / 100.0)
    available_volume = get_available_volume(access_key, secret_key, code)
    sell_volume = min(intended_qty, available_volume) if available_volume > 0 else 0

    if sell_volume <= 0:
        print(f"[{user}-{market}-{code}] 매도가능 수량 없음, bit_trading_trail 종료 처리.")
        update_trail_row(conn, trail_id, trail_tp='Y', **extra_state)
        return

    order_response = place_market_sell(access_key, secret_key, code, sell_volume)

    if "uuid" not in order_response:
        msg = f"-{user}-[{market}] {code} {label} 매도 주문 실패 => {order_response.get('error', {}).get('message', order_response)}"
        print(msg)
        send_slack_message("#매매신호", msg)
        update_trail_row(conn, trail_id, **extra_state)
        return

    ord_no = order_response['uuid']
    time.sleep(1)
    order_status = get_order(access_key, secret_key, ord_no)

    trades_count = order_status.get('trades_count', 0)
    order_price = (
        Decimal(order_status['price']) if trades_count == 0 and order_status.get('price') else
        sum(Decimal(t['funds']) for t in order_status['trades']) / sum(Decimal(t['volume']) for t in order_status['trades'])
    )
    order_vol = Decimal(order_status['executed_volume'])
    order_amt = int(order_price * order_vol)
    trail_rate = float(((order_price / Decimal(str(basic_price)) - 1) * 100).quantize(Decimal('0.01'))) if basic_price and float(basic_price) > 0 else 0

    remaining_vol = max(Decimal(str(basic_vol)) - order_vol, Decimal('0'))
    is_full = remaining_vol <= Decimal('0.00000001')
    remaining_amt = int(Decimal(str(basic_price)) * remaining_vol) if not is_full else 0
    # 전량 매도 시 'Y'(완료), 부분 매도 시 '3'(안전마진, 당일 재추적 종료) — 다음날 bitTradingSet.py가 '3'을 'L'로 승계
    new_trail_tp = 'Y' if is_full else '3'

    cur3 = conn.cursor()
    cur3.execute("""
        INSERT INTO trade_mng (
            cust_num, market_name, ord_dtm, ord_no, prd_nm, ord_tp, ord_state, ord_count, ord_expect_totamt,
            ord_price, ord_vol, ord_amt, cut_price, cut_rate, cut_amt, goal_price, goal_rate, goal_amt, margin_vol,
            executed_vol, remaining_vol, hold_price, hold_vol, paid_fee, ord_type, regr_id, reg_date, chgr_id, chg_date
        ) VALUES (
            %s, %s, %s, %s, %s, '02', %s, 0, 0,
            %s, %s, %s, 0, 0, 0, 0, 0, 0, 0,
            %s, %s, %s, %s, %s, 'market', %s, %s, %s, %s
        )
    """, (
        cust_num, market, datetime.fromisoformat(order_status['created_at']).strftime("%Y%m%d%H%M%S"), ord_no, "KRW-" + code,
        order_status['state'], order_price, order_vol, order_amt,
        Decimal(order_status['executed_volume']), Decimal(order_status['remaining_volume']), basic_price, basic_vol, Decimal(order_status['paid_fee']),
        USER_ID, datetime.now(), USER_ID, datetime.now()
    ))
    conn.commit()
    cur3.close()

    update_fields = dict(extra_state)
    update_fields.update({
        'trail_tp': new_trail_tp, 'order_no': ord_no, 'order_price': order_price, 'order_vol': order_vol,
        'order_amt': order_amt, 'trail_rate': trail_rate, 'trade_result': trade_result,
        'basic_vol': remaining_vol, 'basic_amt': remaining_amt,
    })
    update_trail_row(conn, trail_id, **update_fields)

    qty_label = "전량" if is_full else f"{ratio_pct:.0f}%(부분)"
    msg = (
        f"-{user}-[{market}] [{label} 매도-{code}] {reason}\n"
        f"매도가 : {format_number(order_price)}원, 매도량({qty_label}) : {format_number(order_vol)}, 매도금액 : {format_number(order_amt)}원, "
        f"수익률 : {trail_rate}%, 주문번호 : {ord_no}"
        + ("" if is_full else f", 잔여수량 : {format_number(remaining_vol)}")
    )
    print(msg)
    send_slack_message("#매매신호", msg)


# ─────────────────────────────────────────
# trail_tp='1' : 목표가 미돌파, 이탈가 대기
# ─────────────────────────────────────────

def process_tp1(conn, ctx, tenmin_key):
    code = ctx['code']
    low, high, close, acml_vol = ctx['low'], ctx['high'], ctx['close'], ctx['acml_vol']
    stop_price, exit_price, target_price = ctx['stop_price'], ctx['exit_price'], ctx['target_price']
    chk_vol = ctx['volumn']

    wait = dict(ctx['wait_state'].get('bw1', {}))

    if wait.get('active'):
        wait_key = datetime.strptime(wait['tenmin_key'], "%Y%m%d%H%M") if wait.get('tenmin_key') else None

        if wait_key is not None and tenmin_key != wait_key and wait.get('tenmin_low') is None:
            if wait.get('sell_on_candle_close'):
                execute_sell(conn, ctx, DEFAULT_FULL_SELL_RATIO, wait['trade_result'],
                             f"[시장약세] {wait['sell_label']} 10분봉 완성 매도(매도가:현재가)",
                             wait_state={**ctx['wait_state'], 'bw1': {}}, last_alert_keys=ctx['last_alert_keys'])
                return
            series = fetch_10min_series(code)
            bar = next((c for c in series if c['key'] == wait_key), None)
            if bar is not None:
                surge_ok, cur_vol, avg_vol = is_tenmin_vol_surge(series, wait_key)
                wait['tenmin_low'] = bar['low']
                wait['tenmin_vol_ok'] = surge_ok
                if not surge_ok:
                    wait = {}

        if wait.get('tenmin_low') is not None and low < wait['tenmin_low']:
            execute_sell(conn, ctx, DEFAULT_FULL_SELL_RATIO, wait['trade_result'],
                         f"{wait['sell_label']} 10분봉저가({wait['tenmin_low']:,.4f}) 재이탈",
                         wait_state={**ctx['wait_state'], 'bw1': {}}, last_alert_keys=ctx['last_alert_keys'])
            return

    else:
        breach_price, breach_label, trade_result = None, None, None
        if exit_price > 0 and low <= exit_price and acml_vol > chk_vol:
            breach_price, breach_label, trade_result = exit_price, "최종이탈가 매도", 'EXIT'
        elif stop_price > 0 and low <= stop_price and acml_vol > chk_vol:
            breach_price, breach_label, trade_result = stop_price, "이탈가 매도", 'STOP'

        if breach_price is not None:
            key_str = tenmin_key.strftime("%Y%m%d%H%M")
            wait = {
                'active': True, 'tenmin_key': key_str, 'tenmin_low': None,
                'sell_label': breach_label, 'trade_result': trade_result,
                'sell_on_candle_close': ctx['short_market_down'],
            }
            if ctx['last_alert_keys'].get('1') is None:
                ctx['last_alert_keys']['1'] = key_str
                suffix = "[시장약세] 10분봉 완성 후 매도 대기" if ctx['short_market_down'] else "10분봉 저가 재이탈 대기"
                msg = f"-{ctx['user']}-[{ctx['market']}] {code} {breach_label.replace(' 매도','')}({format_number(breach_price)}) 이탈 → {suffix}"
                print(msg)
                send_slack_message("#매매신호", msg)

    # 목표가 돌파 → 기준봉 생성 후 트레일링(2) 전환
    if target_price > 0 and high >= target_price:
        series = fetch_10min_series(code)
        bar = next((c for c in series if c['key'] == tenmin_key), None)
        if bar is not None:
            update_trail_row(
                conn, ctx['trail_id'], trail_tp='2', base_low=bar['low'], base_high=bar['high'],
                base_vol=bar['volume'], peak_price=bar['high'],
                wait_state={'last_min_key': ctx['wait_state'].get('last_min_key')},
                last_alert_keys=ctx['last_alert_keys'],
            )
            msg = (
                f"-{ctx['user']}-[{ctx['market']}] {code} 목표가({format_number(target_price)}) 돌파 → "
                f"기준봉 생성(고가:{format_number(bar['high'])}/저가:{format_number(bar['low'])}) → 트레일링(2) 전환"
            )
            print(msg)
            send_slack_message("#매매신호", msg)
            return

    update_trail_row(conn, ctx['trail_id'], wait_state={**ctx['wait_state'], 'bw1': wait}, last_alert_keys=ctx['last_alert_keys'])


# ─────────────────────────────────────────
# trail_tp='2' : 목표가 돌파 후 기준봉 트레일링
# ─────────────────────────────────────────

def process_tp2(conn, ctx, tenmin_key, is_last_of_tenmin):
    code = ctx['code']
    low, close = ctx['low'], ctx['close']
    stop_price, exit_price, basic_price = ctx['stop_price'], ctx['exit_price'], ctx['basic_price']
    base_low, base_high, base_vol, peak_price = ctx['base_low'], ctx['base_high'], ctx['base_vol'], ctx['peak_price']

    wait2 = dict(ctx['wait_state'].get('bw2', {}))

    # 분봉 단위 이탈가/최종이탈가 감지 → 해당 10분봉 완성 시 매도(조건 D)
    if not wait2.get('active'):
        is_exit = exit_price > 0 and low <= exit_price
        is_stop = stop_price > 0 and low <= stop_price
        if is_exit or is_stop:
            key_str = tenmin_key.strftime("%Y%m%d%H%M")
            wait2 = {
                'active': True,
                'breach_price': exit_price if is_exit else stop_price,
                'breach_label': '최종이탈가' if is_exit else '이탈가',
                'trade_result': 'EXIT' if is_exit else 'STOP',
                'tenmin_key': key_str,
            }
            last_alert = ctx['last_alert_keys'].get('2')
            if last_alert is None or key_str > last_alert:
                ctx['last_alert_keys']['2'] = key_str
                msg = f"-{ctx['user']}-[{ctx['market']}] {code} {wait2['breach_label']}({format_number(wait2['breach_price'])}) 분봉 저가 이탈 → 10분봉 완성 후 매도 대기"
                print(msg)
                send_slack_message("#매매신호", msg)

    if not is_last_of_tenmin or ctx['proc_min'] == tenmin_key.strftime("%H%M00"):
        update_trail_row(conn, ctx['trail_id'], wait_state={**ctx['wait_state'], 'bw2': wait2}, last_alert_keys=ctx['last_alert_keys'])
        return

    series = fetch_10min_series(code)
    bar = next((c for c in series if c['key'] == tenmin_key), None)
    if bar is None:
        update_trail_row(conn, ctx['trail_id'], wait_state={**ctx['wait_state'], 'bw2': wait2}, last_alert_keys=ctx['last_alert_keys'])
        return

    tenmin_close, tenmin_low, tenmin_high, tenmin_vol = bar['close'], bar['low'], bar['high'], bar['volume']
    prev_bar = next((c for c in series if c['key'] == tenmin_key - timedelta(minutes=10)), None)
    prev_close = prev_bar['close'] if prev_bar else None

    safety_margin = basic_price * (1 + SAFETY_MARGIN_RATE)
    is_late_day = LATE_DAY_START <= ctx['now_hhmm'] < '0900'
    retrace_rate = LATE_DAY_RETRACEMENT_RATE if is_late_day else PEAK_RETRACEMENT_RATE

    sell_trigger, trade_result, reason = False, None, None

    # 조건 D
    if wait2.get('active') and wait2.get('tenmin_key'):
        wait2_key = datetime.strptime(wait2['tenmin_key'], "%Y%m%d%H%M")
        if tenmin_key >= wait2_key:
            sell_trigger = True
            trade_result = wait2['trade_result']
            reason = f"{wait2['breach_label']}({format_number(wait2['breach_price'])}) 분봉 저가 이탈 10분봉 완성 매도"

    # 조건 A : 기준봉 저가를 종가로 이탈 + 안전마진 이하
    if not sell_trigger and tenmin_close < base_low and tenmin_close <= safety_margin:
        sell_trigger = True
        trade_result = 'BASE_BREAK_SAFETY'
        gap_rate = (safety_margin - tenmin_close) / safety_margin * 100 if safety_margin > 0 else 0
        reason = f"안전마진({format_number(safety_margin)}) 이하 기준봉저가({format_number(base_low)}) 종가 이탈 (이탈폭:{gap_rate:.1f}%)"

    # 조건 B : 고점 대비 되돌림(수익구간)
    if not sell_trigger:
        peak_to_safety = peak_price - safety_margin
        if peak_price > safety_margin and peak_to_safety >= safety_margin * 0.05:
            threshold = peak_price - peak_to_safety * retrace_rate
            if tenmin_close < threshold:
                sell_trigger = True
                trade_result = 'PEAK_RETRACE'
                reason = f"고점({format_number(peak_price)}) 대비 되돌림 임계({format_number(threshold)}) 종가 이탈"

    # 조건 C-1 : 안전마진 이상 구간에서도 이탈가/최종이탈가 이탈 시 즉시 매도
    if not sell_trigger and tenmin_close > safety_margin:
        is_exit_breach = exit_price > 0 and tenmin_close <= exit_price
        is_stop_breach = stop_price > 0 and tenmin_close <= stop_price
        if is_exit_breach or is_stop_breach:
            sell_trigger = True
            trade_result = 'EXIT' if is_exit_breach else 'STOP'
            reason = f"{'최종이탈가' if is_exit_breach else '이탈가'} 이탈 (안전마진 이상 구간)"

    # 조건 C-2 : 기준봉 저가를 안전마진 이상에서 이탈 + 거래량서지/연속이탈
    if not sell_trigger and tenmin_close < base_low and tenmin_close > safety_margin:
        consecutive_breaks = (1 if tenmin_close < base_low else 0) + (1 if (prev_close is not None and prev_close < base_low) else 0)
        threshold_breaks = 1 if is_late_day else 2
        if tenmin_vol > base_vol or consecutive_breaks >= threshold_breaks:
            sell_trigger = True
            trade_result = 'BASE_BREAK'
            reason = f"기준봉저가({format_number(base_low)}) 종가 이탈 (거래량:{format_number(tenmin_vol)}/기준:{format_number(base_vol)})"

    if sell_trigger:
        ratio = ctx['trail_plan'] if ctx['trail_plan'] is not None else DEFAULT_TP2_SELL_RATIO
        execute_sell(
            conn, ctx, float(ratio), trade_result, reason,
            proc_min=tenmin_key.strftime("%H%M00"), wait_state={}, last_alert_keys=ctx['last_alert_keys'],
        )
        return

    # 매도 미발생 → 기준봉 갱신(고가/거래량 갱신 시 base_low는 위로만 이동)
    new_base_low, new_base_high, new_base_vol, new_peak = base_low, base_high, base_vol, peak_price
    if tenmin_high > base_high or tenmin_vol > base_vol:
        new_base_low = max(tenmin_low, base_low)
        new_base_high = tenmin_high
        new_base_vol = tenmin_vol
        new_peak = max(peak_price, tenmin_high)

    update_trail_row(
        conn, ctx['trail_id'], base_low=new_base_low, base_high=new_base_high, base_vol=new_base_vol,
        peak_price=new_peak, proc_min=tenmin_key.strftime("%H%M00"),
        wait_state={**ctx['wait_state'], 'bw2': {}}, last_alert_keys=ctx['last_alert_keys'],
    )


# ─────────────────────────────────────────
# trail_tp='L' : 추세 기반 동적 트레일링(장기추적)
# ─────────────────────────────────────────

def process_tpL(conn, ctx, tenmin_key, is_last_of_tenmin):
    code = ctx['code']
    low, close, high = ctx['low'], ctx['close'], ctx['high']
    basic_price, stop_price, exit_price, peak_price = ctx['basic_price'], ctx['stop_price'], ctx['exit_price'], ctx['peak_price']

    trend = get_coin_trend(code)
    trend_up = bool(trend and trend['trend'] == 'Uptrend')
    trend_down = bool(trend and trend['trend'] == 'Downtrend')
    trend_ref = (trend.get('ref_price') if trend else None) or 0

    # [최우선] 상승추세가 아니면서 최종이탈가 이탈 → 즉시 전량매도
    if not trend_up and exit_price > 0 and close <= exit_price:
        execute_sell(
            conn, ctx, DEFAULT_FULL_SELL_RATIO, 'EXIT', f"추세이탈가({format_number(exit_price)}) 이탈 즉시 매도",
            wait_state={}, last_alert_keys=ctx['last_alert_keys'],
        )
        return

    wait = dict(ctx['wait_state'].get('bwL', {}))

    if not trend_up:
        if wait.get('active'):
            wait_key = datetime.strptime(wait['tenmin_key'], "%Y%m%d%H%M") if wait.get('tenmin_key') else None
            if wait_key is not None and tenmin_key != wait_key and wait.get('tenmin_low') is None:
                series = fetch_10min_series(code)
                bar = next((c for c in series if c['key'] == wait_key), None)
                if bar is not None:
                    surge_ok, _, _ = is_tenmin_vol_surge(series, wait_key)
                    wait['tenmin_low'] = bar['low']
                    if not surge_ok:
                        wait = {}
            if wait.get('tenmin_low') is not None and low < wait['tenmin_low']:
                execute_sell(
                    conn, ctx, DEFAULT_FULL_SELL_RATIO, 'STOP',
                    f"이탈가({format_number(stop_price)}) 10분봉저가({format_number(wait['tenmin_low'])}) 재이탈",
                    wait_state={**ctx['wait_state'], 'bwL': {}}, last_alert_keys=ctx['last_alert_keys'],
                )
                return
        elif stop_price > 0 and low <= stop_price:
            key_str = tenmin_key.strftime("%Y%m%d%H%M")
            wait = {'active': True, 'tenmin_key': key_str, 'tenmin_low': None}
            if ctx['last_alert_keys'].get('L') is None:
                ctx['last_alert_keys']['L'] = key_str
                msg = f"-{ctx['user']}-[{ctx['market']}] {code} 이탈가({format_number(stop_price)}) 이탈 대기"
                print(msg)
                send_slack_message("#매매신호", msg)

    # 10분봉 완성 시점에만 되돌림/수익잠금 평가
    if is_last_of_tenmin and ctx['proc_min'] != tenmin_key.strftime("%H%M00"):
        prev_info = get_prev_day_info(code)
        prev_close = prev_info['close_price'] if prev_info else 0
        has_15pct = prev_close > 0 and high >= prev_close * (1 + PROFIT_LOCK_GAIN_PCT / 100)
        new_peak = max(peak_price, high)

        if has_15pct:
            safety_margin = basic_price * (1 + PROFIT_LOCK_FLOOR_RATE)
            is_late_day = LATE_DAY_START <= ctx['now_hhmm'] < '0900'
            retrace_rate = LATE_DAY_RETRACEMENT_RATE if is_late_day else PEAK_RETRACEMENT_RATE
            peak_to_safety = new_peak - safety_margin
            if new_peak > safety_margin and peak_to_safety >= safety_margin * 0.05:
                threshold = new_peak - peak_to_safety * retrace_rate
                if close < threshold:
                    execute_sell(
                        conn, ctx, DEFAULT_FULL_SELL_RATIO, 'RETRACE',
                        f"고점({format_number(new_peak)}) 대비 되돌림 임계({format_number(threshold)}) 이탈, 당일 15%+ 달성",
                        wait_state={}, last_alert_keys=ctx['last_alert_keys'],
                    )
                    return

        update_trail_row(
            conn, ctx['trail_id'], peak_price=new_peak, proc_min=tenmin_key.strftime("%H%M00"),
            wait_state={**ctx['wait_state'], 'bwL': wait}, last_alert_keys=ctx['last_alert_keys'],
        )
    else:
        update_trail_row(conn, ctx['trail_id'], wait_state={**ctx['wait_state'], 'bwL': wait}, last_alert_keys=ctx['last_alert_keys'])

    # 하락추세 사전경고 (09:10~08:30, 텍스트 알림 1일 1회)
    in_warn_window = ctx['now_hhmm'] >= DOWNTREND_WARN_START or ctx['now_hhmm'] <= DOWNTREND_WARN_END
    if in_warn_window and trend_down and close < trend_ref:
        if ctx['last_alert_keys'].get('downtrend_warn') != ctx['trail_day']:
            ctx['last_alert_keys']['downtrend_warn'] = ctx['trail_day']
            msg = f"-{ctx['user']}-[{ctx['market']}] {code} [사전경고] 종가:{format_number(close)}원, 하락추세 기준가({format_number(trend_ref)}) 이탈"
            print(msg)
            send_slack_message("#매매신호", msg)
            update_trail_row(conn, ctx['trail_id'], last_alert_keys=ctx['last_alert_keys'])

    # 08:40 이후 하락추세 지속 시 강제 전량매도
    if DOWNTREND_SELL_START <= ctx['now_hhmm'] < '0900' and trend_down and close < trend_ref:
        execute_sell(
            conn, ctx, DEFAULT_FULL_SELL_RATIO, 'DOWNTREND',
            f"[장마감전] 종가:{format_number(close)}원, 하락추세 기준가({format_number(trend_ref)}) 이탈",
            wait_state={}, last_alert_keys=ctx['last_alert_keys'],
        )


# ─────────────────────────────────────────
# 종목 처리 진입점 : 최근 완성 1분봉 로드 → trail_tp 별 분기
# ─────────────────────────────────────────

def process_row(conn, user, market, cust_num, acct_no, access_key, secret_key, trail_day, row):
    (trail_id, prd_nm, trail_tp, basic_price, basic_vol, stop_price, target_price, exit_price,
     peak_price, base_low, base_high, base_vol, volumn, trail_plan, proc_min, wait_state, last_alert_keys) = row

    code = prd_nm.split('-')[-1] if '-' in prd_nm else prd_nm

    now = datetime.now()
    now_hhmm = now.strftime("%H%M")

    # 09:00~09:10 미처리
    if '0900' <= now_hhmm < PREMARKET_END:
        return

    candles = fetch_minute_candles(code, 1, 2)
    if not candles:
        return
    candles_sorted = sorted(candles, key=lambda c: c['candle_date_time_kst'])
    latest = candles_sorted[-1]
    latest_start = datetime.strptime(latest['candle_date_time_kst'], "%Y-%m-%dT%H:%M:%S")

    if latest_start + timedelta(minutes=1) > now:
        # 아직 형성 중인 분봉 → 직전 완성봉 사용
        if len(candles_sorted) < 2:
            return
        latest = candles_sorted[-2]
        latest_start = datetime.strptime(latest['candle_date_time_kst'], "%Y-%m-%dT%H:%M:%S")

    wait_state = dict(wait_state or {})
    minute_key = latest_start.strftime("%Y%m%d%H%M")
    if wait_state.get('last_min_key') == minute_key:
        return  # 이미 처리한 분봉
    wait_state['last_min_key'] = minute_key

    short_market_down = get_short_market_down(conn, acct_no, cust_num, market, code, trail_day)

    ctx = {
        'trail_id': trail_id, 'code': code,
        'basic_price': float(basic_price or 0), 'basic_vol': float(basic_vol or 0),
        'stop_price': float(stop_price or 0), 'target_price': float(target_price or 0), 'exit_price': float(exit_price or 0),
        'peak_price': float(peak_price or 0), 'base_low': float(base_low or 0), 'base_high': float(base_high or 0),
        'base_vol': float(base_vol or 0), 'volumn': float(volumn or 0), 'trail_plan': trail_plan, 'proc_min': proc_min,
        'wait_state': wait_state, 'last_alert_keys': dict(last_alert_keys or {}),
        'high': float(latest['high_price']), 'low': float(latest['low_price']),
        'close': float(latest['trade_price']), 'acml_vol': float(latest['candle_acc_trade_volume']),
        'now_hhmm': now_hhmm, 'user': user, 'market': market, 'cust_num': cust_num, 'acct_no': acct_no,
        'access_key': access_key, 'secret_key': secret_key, 'trail_day': trail_day,
        'short_market_down': short_market_down,
    }

    tenmin_key = get_10min_key(latest_start)
    is_last_of_tenmin = (latest_start.minute % 10 == 9)

    if trail_tp == '1':
        process_tp1(conn, ctx, tenmin_key)
    elif trail_tp == '2':
        process_tp2(conn, ctx, tenmin_key, is_last_of_tenmin)
    elif trail_tp == 'L':
        process_tpL(conn, ctx, tenmin_key, is_last_of_tenmin)


def analyze_trail(user, market):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    trail_day = get_business_day(market)

    try:
        cur0 = conn.cursor()
        cur0.execute("""
            SELECT cust_num, acct_no, access_key, secret_key
            FROM cust_mng
            WHERE cust_nm = %s AND market_name = %s
        """, (user, market))
        cust_row = cur0.fetchone()
        cur0.close()

        if not cust_row:
            print(f"[{trail_day}-{user}-{market}] 고객정보 없음.")
            return

        cust_num, acct_no, access_key, secret_key = cust_row

        # [공통 사전 처리] 동일종목 중복건 정리 및 잔고 현행화
        dedup_trail_rows(conn, cust_num, market, trail_day)
        refresh_basic_from_balance(conn, cust_num, market, trail_day)

        cur1 = conn.cursor()
        cur1.execute("""
            SELECT id, prd_nm, trail_tp, basic_price, basic_vol, stop_price, target_price, exit_price,
                   peak_price, base_low, base_high, base_vol, volumn, trail_plan, proc_min, wait_state, last_alert_keys
            FROM bit_trading_trail
            WHERE cust_num = %s AND market_name = %s AND trail_day = %s
            AND trail_tp IN ('1', '2', 'L') AND basic_vol > 0
        """, (cust_num, market, trail_day))
        rows = cur1.fetchall()
        cur1.close()

        if not rows:
            print(f"[{trail_day}-{user}-{market}] 매매추적 대상 없음.")
            return

        for row in rows:
            prd_nm = row[1]
            try:
                process_row(conn, user, market, cust_num, acct_no, access_key, secret_key, trail_day, row)
            except Exception as e:
                print(f"[{trail_day}-{user}-{market}] 종목 처리 오류 ({prd_nm}): {e}")
            time.sleep(0.2)

    except Exception as e:
        conn.rollback()
        print(f"[{trail_day}-{user}-{market}] bitTradingTrail 처리 중 예외 발생: {e}")

    finally:
        conn.close()


# 실행 (market_name = 'UPBIT' 우선 구현 — BITHUMB는 추후 확장)
if __name__ == "__main__":
    nickname_list = [
        {"cust_nm": "phills2", "market_name": "UPBIT"},
        {"cust_nm": "mama", "market_name": "UPBIT"},
        {"cust_nm": "honey", "market_name": "UPBIT"},
    ]

    for nick in nickname_list:
        analyze_trail(nick['cust_nm'], nick['market_name'])

    # 운영 환경에서는 OS 스케줄러(cron/작업 스케줄러)로 본 스크립트를 1분 주기로 호출하는 방식을 권장한다.
    # import schedule
    # for nick in nickname_list:
    #     schedule.every(1).minutes.do(analyze_trail, nick['cust_nm'], nick['market_name'])
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
