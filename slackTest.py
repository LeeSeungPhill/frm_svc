import os
import slack_sdk
from slack_sdk.errors import SlackApiError
from scipy.stats import linregress
from dotenv import load_dotenv

load_dotenv()

# Slack 메세지 연동
SLACK_BOT_TOKEN1 = os.environ['SLACK_BOT_TOKEN1']
SLACK_BOT_TOKEN2 = os.environ['SLACK_BOT_TOKEN2']
SLACK_BOT_TOKEN3 = os.environ['SLACK_BOT_TOKEN3']
SLACK_BOT_TOKEN4 = os.environ['SLACK_BOT_TOKEN4']
client = slack_sdk.WebClient(token=SLACK_BOT_TOKEN1+SLACK_BOT_TOKEN2+SLACK_BOT_TOKEN3+SLACK_BOT_TOKEN4)

sent_messages = set()

def send_slack_message(channel, message):
    try:
        if message not in sent_messages:  # 이전에 전송되지 않은 메시지만 전송
            response = client.chat_postMessage(channel=channel, text=message)
            print(f"Slack 메시지 전송 성공: {response['message']['text']}")
            sent_messages.add(message)  # 메시지를 기록
        # else:
        #     print("중복 메시지, 전송 생략:", message)
    except SlackApiError as e:
        print(f"Slack 메시지 전송 실패: {e.response['error']}")

message = f"매도 신호 발생"
send_slack_message("#매매신호", message)        