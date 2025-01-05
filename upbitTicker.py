import requests

server_url = "https://api.upbit.com"

params = {
    "markets": "KRW-BTC,KRW-XRP,KRW-ETH,KRW-GAS,KRW-ATOM,KRW-STX,KRW-SOL,KRW-SUI,KRW-ZETA,KRW-HBAR,KRW-ONDO,KRW-LINK"
}

# 시세 현재가 조회
res = requests.get(server_url + "/v1/ticker", params=params).json()

for item in res:
    name = item['market']
    trade_price = float(item['trade_price'])
    low_price = float(item['low_price'])
    high_price = float(item['high_price'])
    print(name,", trade_price : ",trade_price,", low_price : ",low_price,", high_price : ",high_price)