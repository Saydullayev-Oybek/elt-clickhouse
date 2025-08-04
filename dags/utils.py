import os
import json
import base64
import requests
import clickhouse_connect
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AUTH_PATH = os.path.join(BASE_DIR, "auth.json")

with open(AUTH_PATH) as f:
    auth = json.load(f)
    username = auth["username"]
    password = auth["password"]

# endcode credentials 
endcode_credentials = base64.b64encode(f"{username}:{password}".encode()).decode()

# Headers
headers = {
    'Authorization': f"Basic {endcode_credentials}",
    'project_code': 'trade',
    'filial_id': '6091241'
}

## Download object function ##
def download_object(url: str, obj_name: str, begin_date: datetime = None, end_date: datetime = None):
    
    payload = {
        "begin_modified_on": begin_date.strftime("%d-%m-%Y"),
        "end_modified_on": end_date.strftime("%d-%m-%Y")
    }

    response = requests.post(url, headers=headers, data=payload)
    
    all_records = []
    if response.status_code == 200:
        data = response.json()
        if data[f"{obj_name}"]:
            all_records.extend(data[f"{obj_name}"])
        print(response.text)
    else:
        print(f"❌ Xatolik: {response.status_code}")
        print(response.text)
        print(url)
    
    return all_records


## begin_date and end_date functions ## 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SYNC_FILE_PATH = os.path.join(BASE_DIR, "last_sync.json")

if __name__ == "__main__":
    # only used for local testing
    print("Running utils.py directly")

def get_date_range():
    """ Fayldan oxirgi sync sanani o‘qish, bo‘lmasa 1 kun oldingi vaqtni olish """
    if os.path.exists(SYNC_FILE_PATH):
        with open(SYNC_FILE_PATH, "r") as f:
            last_sync_str = json.load(f).get("last_sync")
            begin_date = datetime.fromisoformat(last_sync_str) + timedelta(days=1)
    else:
        begin_date = datetime(2024, 4, 1)

    end_date = datetime.utcnow()
    return begin_date, end_date


def save_sync_date(sync_dt):
    """ Faylga oxirgi sync sanani saqlash """
    with open(SYNC_FILE_PATH, "w") as f:
        json.dump({"last_sync": sync_dt.isoformat()}, f)


def client_connect():
    client = clickhouse_connect.get_client(
            host='kz1-a-0qptbicmd7nfc07f.mdb.yandexcloud.kz', 
            port=8443,
            username='admin',
            password='admin123',
            secure=True,
            verify=False
        )
    return client