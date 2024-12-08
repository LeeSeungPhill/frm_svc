import requests

server_url = "https://api.upbit.com"

params = {
    "markets": "KRW-BTC,KRW-ETH"
}

# 시세 현재가 조회
res = requests.get(server_url + "/v1/ticker", params=params)

print(res.json())