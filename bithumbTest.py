# Python 3
# pip3 installl pyJwt
import jwt 
import uuid
import time
import requests
import os
from dotenv import load_dotenv
load_dotenv()

# Set API parameters
print("BITHUMB_ACCESS_KEY : ", os.getenv("BITHUMB_ACCESS_KEY"))
print("BITHUMB_SECRET_KEY : ", os.getenv("BITHUMB_SECRET_KEY"))
accessKey = os.getenv("BITHUMB_ACCESS_KEY")
secretKey = os.getenv("BITHUMB_SECRET_KEY")
apiUrl = os.getenv("BITHUMB_API")

# Generate access token
payload = {
    'access_key': accessKey,
    'nonce': str(uuid.uuid4()),
    'timestamp': round(time.time() * 1000)
}
jwt_token = jwt.encode(payload, secretKey)
authorization_token = 'Bearer {}'.format(jwt_token)
headers = {
  'Authorization': authorization_token
}

try:
    # Call API : 잔고조회
    response = requests.get(apiUrl + '/v1/accounts', headers=headers)
    # handle to success or fail
    print(response.status_code)
    print(response.json())
except Exception as err:
    # handle exception
    print(err)
