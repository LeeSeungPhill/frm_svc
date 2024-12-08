import jwt
import hashlib
import os
import requests
import uuid
from urllib.parse import urlencode, unquote
from dotenv import load_dotenv
load_dotenv()

# access_key = os.environ['UPBIT_0825_ACCESS_KEY']
# secret_key = os.environ['UPBIT_0825_SECRET_KEY']
# api_url = os.environ['UPBIT_API']
access_key = os.getenv("UPBIT_0825_ACCESS_KEY")
secret_key = os.getenv("UPBIT_0825_SECRET_KEY")
api_url = os.getenv("UPBIT_API")

params = {
  'market': 'KRW-BTC'
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

res = requests.get(api_url + '/v1/orders/chance', params=params, headers=headers)
print(res.json())