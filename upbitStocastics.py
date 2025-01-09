import requests
import pandas as pd
from datetime import date, datetime, timedelta
import time

# 업비트 시세 캔들 데이터 가져오기
def fetch_candles(market):
    today = datetime.now()
    datetime_with_time = datetime.combine(today, datetime.strptime('00:00:00', '%H:%M:%S').time())
    start_dt = datetime_with_time.isoformat() + "+09:00"

    url = "https://api.upbit.com/v1/candles/days"
    params = {  
        'market': market,  
        'count': 50,
        'to': start_dt
    }  
    headers = {"accept": "application/json"}

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        # DataFrame으로 변환 및 정렬 (최신 데이터가 맨 위로 오므로 뒤집기)
        df = pd.DataFrame(data)
        df = df[["candle_date_time_kst", "opening_price", "high_price", "low_price", "trade_price"]]
        df.columns = ["date", "open", "high", "low", "close"]
        return df.iloc[::-1].reset_index(drop=True)
    else:
        print(f"Error fetching candles: {response.status_code}")
        return None

# Stochastic Slow 계산
def calculate_stochastic_slow(df, period, k_slow, d_slow):
    df["low_min"] = df["low"].rolling(window=period).min()
    df["high_max"] = df["high"].rolling(window=period).max()
    df["%K"] = ((df["close"] - df["low_min"]) / (df["high_max"] - df["low_min"])) * 100
    df["%K_slow"] = df["%K"].rolling(window=k_slow).mean()
    df["%D"] = df["%K_slow"].rolling(window=d_slow).mean()
    return df

# 돌파 이탈 조건 확인 함수
def check_crossover(df):
    df["upward_crossover"] = False
    df["downward_crossover"] = False

    for i in range(1, len(df)):
        # 현재와 이전 행 데이터
        current_row = df.iloc[i]
        prev_row = df.iloc[i - 1]

        # 상향 돌파 조건: %K_slow < %D 이고 이전에는 %K_slow >= %D
        if (
            current_row["%K_slow"] > 75 and
            current_row["%K_slow"] < current_row["%D"] and
            prev_row["%K_slow"] >= prev_row["%D"]
        ):
            df.at[i, "upward_crossover"] = True

        # 하향 이탈 조건: %K_slow < %D 이고 이전에는 %K_slow <= %D
        if (
            current_row["%K_slow"] < 25 and
            current_row["%K_slow"] < current_row["%D"] and
            prev_row["%K_slow"] <= prev_row["%D"]
        ):
            df.at[i, "downward_crossover"] = True

    return df

# 메인 실행
if __name__ == "__main__":
    # 감시할 코인
    params = [
        "KRW-BTC","KRW-XRP","KRW-ETH","KRW-GAS","KRW-ATOM","KRW-STX","KRW-SOL","KRW-SUI","KRW-ZETA","KRW-HBAR","KRW-ONDO","KRW-LINK"
    ]
    period = 9  # 기본 기간
    k_slow = 3  # %K의 슬로우 이동평균 기간
    d_slow = 3  # %D의 슬로우 이동평균 기간

    for i in params:
        # 캔들 데이터 가져오기
        candles = fetch_candles(market=i)
        if candles is not None:
            # Stochastic Slow 계산
            result = calculate_stochastic_slow(candles, period=period, k_slow=k_slow, d_slow=d_slow)
            
            # 돌파 이탈 조건 확인
            result = check_crossover(result)

            # 상향 돌파와 하향 이탈 각각 출력
            upward_crossovers = result[result["upward_crossover"]]
            downward_crossovers = result[result["downward_crossover"]]

            if not upward_crossovers.empty:
                print("과매수(상향 돌파) 발생:",i)
                print(upward_crossovers[["date", "close", "%K", "%K_slow", "%D"]])

            if not downward_crossovers.empty:
                print("과매도(하향 이탈) 발생:",i)
                print(downward_crossovers[["date", "close", "%K", "%K_slow", "%D"]])

            if upward_crossovers.empty and downward_crossovers.empty:
                print("돌파 이탈 조건이 충족되지 않았습니다.")
        time.sleep(0.1)        
