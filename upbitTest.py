import jwt
import hashlib
import os
import requests
import uuid
from urllib.parse import urlencode, unquote

access_key = os.environ['UPBIT_0825_ACCESS_KEY']
secret_key = os.environ['UPBIT_0825_SECRET_KEY']
api_url = os.environ['UPBIT_API']

print("access_key : ", access_key)
print("secret_key : ", secret_key)
print("api_url : ", api_url)

payload = {
    'access_key': access_key,
    'nonce': str(uuid.uuid4()),
}

jwt_token = jwt.encode(payload, secret_key)
authorization = 'Bearer {}'.format(jwt_token)
headers = {
  'Authorization': authorization,
}

print("jwt_token : ", jwt_token)
print("authorization : ", authorization)

print("headers : ", headers)

res = requests.get(api_url + '/v1/accounts',headers=headers)
print(res.json())