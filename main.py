import json
import sqlite3
import logging

import requests
import math
import datetime

from tqdm import tqdm
from openpyxl import Workbook
from config import usertoken_frwp



logging.basicConfig(filename='frwp.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')



salon = 630091
chain = 613336
now = datetime.datetime.now()

headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer u8xzkdpkgfc73uektn64, User {usertoken_frwp}',
    'Accept': 'application/vnd.yclients.v2+json'
}

recdata = []  # массив со всеми вообще записями
allabons = []
alltransact = []
allgoods = []


recstart = "2023-01-01"
# enddate = now.strftime('%Y-%m-%d')
enddate = "2023-06-19"


def getallrec(page):
    url = f"https://api.yclients.com/api/v1/records/{salon}?end_date={enddate}&start_date={recstart}&count=1000&page={page}"
    payload = {}
    response = requests.request("GET", url, headers=headers, data=payload).json()
    total_count = response["meta"]["total_count"]
    iterations = math.ceil(total_count / 1000)
    for item in response["data"]:
        if item["paid_full"] != 1:
            date_string = item["date"]
            date_object = datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
            formatted_date = date_object.strftime('%Y-%m-%d')
            recdata.append({
                "id": item["id"],
                "client_id": item["client"]["id"],
                "client_phone": item["client"]["phone"],
                "date": item["date"],
                "link": f"https://yclients.com/timetable/{salon}#main_date={formatted_date}&open_modal_by_record_id={item['id']}"
            })
    return iterations

def parserec():
    page = 1
    it = getallrec(page)
    for i in tqdm(range(2, it + 1), desc="Searching for unpaid records for the entire period"):
        getallrec(i)

def parseabon():
    page = 1
    while True:
        url = f"https://api.yclients.com/api/v1/chain/{chain}/loyalty/abonements"
        payload = json.dumps({
            "created_after": "2000-01-01",
            "created_before": enddate,
            "count": "1000",
            "page": page
        })
        response = requests.request("GET", url, headers=headers, data=payload).json()
        if response["meta"]["count"] == 0:
            break
        for item in tqdm(response["data"], desc=f"Collect abonements, page {page}"):
            allabons.append({
                "id": item["id"],
                "number": item["number"],
                "createddate": item["created_date"],
                "activated_date": item["activated_date"],
                "expiration_date": item["expiration_date"],
                "status": item["status"]["id"]
            })
        page += 1

def parseLoyaltyTransaction():
    page = 1
    while True:
        url = f"https://api.yclients.com/api/v1/chain/{chain}/loyalty/transactions"
        payload = json.dumps({
            "created_after": "2000-01-01",
            "created_before": enddate,
            "count": "1000",
            "types": ["9"],
            "page": page
        })
        response = requests.request("GET", url, headers=headers, data=payload).json()
        if response["meta"]["count"] == 0:
            break
        for item in tqdm(response["data"], desc=f"Сollect loyalty transactions, page {page}"):
            alltransact.append({
                "id": item.get("id", "['']"),
                "created_date": item.get("created_date", ''),
                "amount": item.get("amount", ''),
                "abonement_id": item.get("abonement_id", ''),
                "visit_id": item.get("visit_id", ''),
                "record_id": item.get("item_record_id", '')
            })
        page += 1

def parseAllGoodtrans():
    page = 1
    while True:
        url = f"https://api.yclients.com/api/v1/storages/transactions/{salon}"
        payload = json.dumps({
            "count": "1000",
            "page": page
        })
        response = requests.request("GET", url, headers=headers, data=payload).json()
        if response["data"] == []:
            break
        for item in tqdm(response["data"], desc=f"Collect all goods transactions, page {page}"):
            client = item.get("client", {})
            if isinstance(client, dict):
                client_id = client.get("id", '')
                client_phone = client.get("phone", '')
            else:
                client_id = ''
                client_phone = ''
            allgoods.append({
                "id": item.get("id", ''),
                "loyalty_abonement_id": item.get("loyalty_abonement_id", ''),
                "client_id": client_id,
                "client_phone": client_phone,
            })
        page += 1





def insertabon(dataset):
    conn = sqlite3.connect('frwp.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS abonements
                      (id INTEGER PRIMARY KEY, number VARCHAR(100), cdate TIMESTAMP, activated TIMESTAMP, expiration TIMESTAMP, status INTEGER)''')
    for item in tqdm(dataset, desc=f"Insert data into the table abonements"):
        try:
            cursor.execute("INSERT INTO abonements VALUES (?, ?, ?, ?, ?, ?)", (
                item["id"], item["number"], item["createddate"], item["activated_date"], item["expiration_date"],
                item["status"]))
            conn.commit()
        except sqlite3.IntegrityError:
            logging.warning(f"Ошибка: запись с id {item['id']} уже существует и не может быть добавлена в таблицу abonements.")

def inserttransactions(dataset):
    conn = sqlite3.connect('frwp.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS loyalty_transactions
                      (id INTEGER PRIMARY KEY, cdate TIMESTAMP, amount DECIMAL(16,2), abonement_id INTEGER, visit_id INTEGER, record_id INTEGER)''')

    for item in tqdm(dataset, desc=f"Insert data into the table loyalty_transactions"):
        try:
            cursor.execute("INSERT INTO loyalty_transactions VALUES (?, ?, ?, ?, ?, ?)", (
                item["id"], item["created_date"], item["amount"], item["abonement_id"], item["visit_id"],
                item["record_id"]))
            conn.commit()
        except sqlite3.IntegrityError:
            logging.warning(f"Ошибка: запись с id {item['id']} уже существует и не может быть добавлена в таблицу loyalty_transactions.")

def insertGoodTransactions(dataset):
    conn = sqlite3.connect('frwp.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS goods_transactions
                      (id INTEGER PRIMARY KEY, loyalty_abonement_id INTEGER, client_id INTEGER, client_phone INTEGER)''')

    for item in tqdm(dataset, desc=f"Insert data into the table goods_transactions"):
        try:
            cursor.execute("INSERT INTO goods_transactions VALUES (?, ?, ?, ?)",
                           (item["id"], item["loyalty_abonement_id"], item["client_id"], item["client_phone"]))
            conn.commit()
        except sqlite3.IntegrityError:
            logging.warning(f"Ошибка: запись с id {item['id']} уже существует и не может быть добавлена в таблицу goods_transactions.")

def insertRecords(dataset):
    conn = sqlite3.connect('frwp.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS records
                      (id INTEGER PRIMARY KEY, client INTEGER, date TIMESTAMP, link VARCHAR(250))''')

    for item in tqdm(dataset, desc=f"Insert data into the table records"):
        try:
            cursor.execute("INSERT INTO records VALUES (?, ?, ?, ?)", (item["id"], item["client_id"], item["date"], item["link"]))
            conn.commit()
        except sqlite3.IntegrityError:
            logging.warning(f"Ошибка: запись с id {item['id']} уже существует и не может быть добавлена в таблицу records.")

def saveresult():
    conn = sqlite3.connect('frwp.db')
    cursor = conn.cursor()

    # Выполнение SQL-запроса
    query = '''
       SELECT
       gt.client_id AS "ID клиента",
       gt.client_phone AS "Телефон клиента",
       a.id AS "ID абонемента",
       a.number as "Номер абонемента",
       strftime('%Y-%m-%d %H:%M:%S', datetime(a.activated, 'localtime', '+1 hours')) AS "Начало действия абонемента",
       CASE WHEN a.status = 4 THEN (SELECT strftime('%Y-%m-%d %H:%M:%S', datetime(MAX(cdate), 'localtime', '+1 hours')) FROM loyalty_transactions lt WHERE lt.abonement_id  = a.id)
           ELSE strftime('%Y-%m-%d %H:%M:%S', datetime(a.expiration, 'localtime', '+1 hours')) END AS "Окончание действия абонемента (или дата последнего списания)",
       CASE a.status
           WHEN 1 THEN 'Выпущен'
           WHEN 2 THEN 'Активен'
           WHEN 3 THEN 'Просрочен'
           WHEN 4 THEN 'Исчерпан'
       END AS "Статус абонемента",
       r.id AS "ID записи",
       r.date AS "Дата записи",
       r.link AS "Ссылка на визит"        
        FROM goods_transactions gt
        JOIN abonements a ON gt.loyalty_abonement_id = a.id
        JOIN records r ON r.client = gt.client_id
        WHERE gt.client_id IN (SELECT DISTINCT client FROM records)
          AND r.date BETWEEN a.cdate AND a.expiration;
    '''
    cursor.execute(query)
    column_headers = [description[0] for description in cursor.description]
    results = cursor.fetchall()

    # Вывод в xls
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(column_headers)
    for row_data in results:
        sheet.append(row_data)
    workbook.save("results.xlsx")


    conn.close()



def job():
    #collect data
    parserec()
    parseabon()
    parseLoyaltyTransaction()
    parseAllGoodtrans()
    #insert into table
    insertabon(allabons)
    insertGoodTransactions(allgoods)
    inserttransactions(alltransact)
    insertRecords(recdata)
    saveresult()

job()

