import requests

url = "https://api.upbit.com/v1/market/all?is_details=true"

headers = {"accept": "application/json"}

res = requests.get(url, headers=headers)

# res.json()
market = res.json()
print(len(market))
market_dict = {}

for data in market: 
    if data['market'].startswith('KRW-'):
        print("market_id : "+data['market']+", name :  "+data['korean_name'])
        market_dict[data['korean_name']] = data['market']

korean_name = input("마켓 한글 이름을 입력하세요: ")  # 사용자 입력
market_id = market_dict.get(korean_name, "해당하는 마켓을 찾을 수 없습니다.")
print("market_id : ",market_id)