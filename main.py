import requests
import math
from tqdm import tqdm
from datetime import datetime
from config import usertoken_frwp


headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer u8xzkdpkgfc73uektn64, User {usertoken_frwp}',
    'Accept': 'application/vnd.yclients.v2+json'
}

recdata = []

def getallrec(page):
    url = f"https://api.yclients.com/api/v1/records/630091?end_date=2023-06-13&start_date=2023-01-01&count=1000&page={page}"
    payload = {}
    response = requests.request("GET", url, headers=headers, data=payload).json()
    total_count = response["meta"]["total_count"]
    iterations = math.ceil(total_count / 1000)
    for item in tqdm(response["data"]):
        if item["paid_full"] != 1:
            recdata.append(item)
    return iterations


def parserec():
    page = 1
    global recdata
    it = getallrec(page)
    for i in range(2,it+1):
        getallrec(i)
    parrec = parsedata(recdata)
    print(parrec)
    # findabon(parrec)


def parsedata(data):
    parse = []
    for item in data:
        parse.append({
            "rec_id" : item["id"],
            "client_id" : item["client"]["id"],
            "client_phone": item["client"]["phone"],
            "date" : item["date"],
            "link" : f"https://yclients.com/timetable/630091#main_date=2023-06-14&open_modal_by_record_id={item['id']}"
        })
    return parse


def findabon(data):
    for item in data:
        url = f"https://api.yclients.com/api/v1/loyalty/abonements/?company_id=630091&phone={item['client_phone']}"
        response = requests.request("GET", url, headers=headers).json()
        target_datetime = datetime.strptime(item["date"], "%Y-%m-%d %H:%M:%S")
        for abons in response["data"]:
            created_datetime = datetime.strptime(abons["created_date"], "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
            if created_datetime < target_datetime:
                print(abons["number"], item["rec_id"], f"https://yclients.com/timetable/630091#main_date=2023-06-14&open_modal_by_record_id={item['rec_id']}", item["date"])




parserec()