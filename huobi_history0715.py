import traceback

from pymongo import MongoClient
from websocket import create_connection
import gzip
import time
import json
import csv
from datetime import datetime

# import pymysql

con = MongoClient('IP', 27017)
zecusdt = con.exchange.zecusdt

global globalTime
# 获取历史数据起始时间
globalTime = 1523109600

##合约名字
global contractName
contractName = 'btc'

##k线时间单位 1分钟k线
global period
period = 60


##时间跨度
global timeLen
timeLen = 300*60*period

# 1506787200

def loop_data(o, k=''):
    global json_ob, c_line
    if isinstance(o, dict):
        for key, value in o.items():
            if (k == ''):
                loop_data(value, key)
            else:
                loop_data(value, k + '.' + key)
    elif isinstance(o, list):
        for ov in o:
            loop_data(ov, k)
    else:
        if not k in json_ob:
            json_ob[k] = {}
        json_ob[k][c_line] = o


def get_title_rows(json_ob):
    title = ['time']##默认加个时间
    row_num = 0
    # global rows
    rows = []
    for key in json_ob:
        title.append(key)
        v = json_ob[key]
        if len(v) > row_num:
            row_num = len(v)
        continue
    for i in range(row_num):
        row = {}
        for k in json_ob:
            v = json_ob[k]
            if i in v.keys():
                row[k] = v[i]
            else:
                row[k] = ''
        rows.append(row)

    # for dbRow in rows:
    #     sql = """insert into zecusdt_min(id,open,close,low,high,amount,vol,count) VALUES (""" + str(
    #         dbRow.get('id')) + """,""" + str(dbRow.get('open')) + """,""" + str(dbRow.get('close')) + """,""" + str(
    #         dbRow.get('low')) + """,""" + str(dbRow.get('high')) + """,""" + str(
    #         dbRow.get('amount')) + """,""" + str(dbRow.get('vol')) + """,""" + str(dbRow.get('count')) + """)"""
    #     try:
    #         cursor.execute(sql)
    #         db.commit()
    #     except:
    #         db.rollback()
    return title, rows


def write_csv(title, rows, csv_file_name):
    with open(csv_file_name, 'a+', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=title)
        writer.writerows(rows)


def json_to_csv(object_list):
    global json_ob, c_line
    json_ob = {}
    c_line = 0
    for ov in object_list:
        loop_data(ov)
        c_line += 1

    titleArr, rows = get_title_rows(json_ob)

    with open("/Users/Chandler/Downloads/"+contractName+"_data.csv", 'a') as csvfile:
        writer = csv.writer(csvfile)
        if(count == 1):##只有第一次打印标题
            writer.writerow(titleArr)
        int = 0
        for row in rows:
            rowdata = []


            for title in titleArr:
                if title == 'time':
                    timeStr = time.localtime(row['id'])
                    rowdata.append(time.strftime('%Y-%m-%d %H:%M:%S', timeStr))
                else :
                    rowdata.append(row[title])
            writer.writerow(rowdata)
            int = int+1


global count
count = 1
global x
x = globalTime

if __name__ == '__main__':
    while True:
        try:
            ws = create_connection("wss://api.huobipro.com/ws", http_proxy_host="127.0.0.1", http_proxy_port=1080)
            # print("""{"req": "market.zecusdt.kline.1min","id": "id10", "from": """ + str(globalTime) + """, "to":""" + str(globalTime + 18000) + """ }""")
            # ws = create_connection("wss://api.hadax.com/ws")
            # ws = create_connection("wss://api.huobipro.com/ws")
            while True:

                # tradeStr = """{"sub": "market.ethusdt.kline.1min","id": "id10"}"""
                tradeStr = """{"req": "market."""+contractName+"""usdt.kline."""+str(period)+"""min","id": "id10", "from": """ + str(globalTime) + """, "to":""" + str(globalTime + timeLen) + """ }"""
                print("获取第" + str(count) + "次数据，时间范围" +
                      time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(globalTime)) +
                                    ">>>" + time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(globalTime + timeLen)))
                ws.send(tradeStr)
                compressData = ws.recv()
                if compressData != '':
                    # print(compressData)
                    result = gzip.decompress(compressData).decode('utf-8')
                else:
                    print("正在重新连接")
                    ws.connect("wss://api.huobi.pro/ws")
                    # ws.connect("wss://api.huobipro.com/ws")

                    globalTime = x + (18000 * (count + 1))
                    tradeStr2 = """{"req": "market."""+contractName+"""usdt.kline."""+period+"""min","id": "id10", "from": """ + str(
                        globalTime) + """, "to":""" + str((globalTime + timeLen)) + """ }"""
                    ws.send(tradeStr2)
                    compressData = ws.recv()
                    result = gzip.decompress(compressData).decode('utf-8')

                temp = result[0:7]
                if temp == '{"ping"':
                    ts = result[8:21]
                    pong = '{"pong":' + ts + '}'
                    ws.send(pong)
                else:
                    resutlJson = json.loads(result)
                    print(resutlJson)
                    data = resutlJson['data']

                    # cursor = None
                    # db = None
                    # db = pymysql.connect("192.168.1.167", "root", "123", "test3")
                    # cursor = db.cursor()
                    # print(data)
                    json_to_csv(data)
                    # db.close()
                    globalTime += timeLen
                    count +=1
                    if count == 5:
                        exit(1)
                    # print(data)
                    #zecusdt.insert_many(data)
        except Exception as e:
            print('connect ws error,retry...')
            exstr = traceback.format_exc()
            print(exstr)
            break