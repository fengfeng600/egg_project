import pandas as pd

area = pd.read_csv("畜牧用地面積.csv")
farms = pd.read_csv("家禽飼養場數.csv")
count = pd.read_csv("家禽飼養隻數.csv")

#篩選出108-113年[蛋雞]
layer_area = area.loc[(area["date"]>=108)&(area["date"]<=113)&(area["dname2"]=="蛋雞")]
layer_farms = farms.loc[(farms["date"]>=108)&(farms["date"]<=113)&(farms["dname2"]=="蛋雞")]
layer_count = count.loc[(count["date"]>=108)&(count["date"]<=113)&(count["dname2"]=="蛋雞")]

#重新設定index
layer_area = layer_area.reset_index(drop=True)
layer_farms = layer_farms.reset_index(drop=True)
layer_count = layer_count.reset_index(drop=True)

#轉換為西元年
layer_area['西元年'] = layer_area['date'] + 1911
layer_farms['西元年'] = layer_farms['date'] + 1911
layer_count['西元年'] = layer_count['date'] + 1911

#讓科學記號轉為整數 四捨五入
layer_area['平方公尺'] = layer_area['value'].apply(lambda x: round(x))
layer_farms['場數數量'] = layer_farms['value'].apply(lambda x: round(x))
layer_count['隻數數量'] = layer_count['value'].apply(lambda x: round(x))

#縣市號碼轉為縣市名
city_code = {
    '09007': '連江縣',
    '09020': '金門縣',
    '68000': '桃園市',
    '67000': '臺南市',
    '66000': '臺中市',
    '65000': '新北市',
    '64000': '高雄市',
    '63000': '臺北市',
    '10020': '嘉義市',
    '10018': '新竹市',
    '10017': '基隆市',
    '10016': '澎湖縣',
    '10015': '花蓮縣',
    '10014': '臺東縣',
    '10013': '屏東縣',
    '10010': '嘉義縣',
    '10009': '雲林縣',
    '10008': '南投縣',
    '10007': '彰化縣',
    '10005': '苗栗縣',
    '10004': '新竹縣',
    '10002': '宜蘭縣'}

layer_farms['dname1'] = layer_farms['dname1'].apply(lambda x: city_code[x] if x in city_code else x)
layer_count['dname1'] = layer_count['dname1'].apply(lambda x: city_code[x] if x in city_code else x)

#刪除不要的欄位和合計\台灣省
for df in [layer_area, layer_farms, layer_count]:
    df.drop(["date", "value", "unit"], axis=1, inplace=True)

layer_area = layer_area.drop(range(108, 118), axis=0)
for newdata in [layer_farms, layer_count]:
    newdata.drop(range(136,148),axis=0, inplace=True)

#合併資料
layer_cf = pd.merge(layer_count,layer_farms,how="left")
layer_alldata = pd.merge(layer_cf,layer_area,how="left")

#因為layer_area 只記錄到2023年，部分區域也沒有，所以nan以0為取代
layer_alldata = layer_alldata.fillna(0)
layer_alldata['平方公尺'] = layer_alldata['平方公尺'].apply(lambda x: round(x))

#刪除金馬地區，因為已經有金門\連江地區
layer_alldata = layer_alldata.drop(range(10,16),axis=0)
layer_alldata = layer_alldata.reset_index(drop=True)
layer_alldata = layer_alldata.rename(columns={"dname1":"地區","dname2":"種類"})
#寫入SQL
import mysql.connector
from sqlalchemy import create_engine
conn = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "123456789",
    database = "egg_data",
    )
cursor = conn.cursor()
create_table = '''CREATE TABLE IF NOT EXISTS layer(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    region VARCHAR(10),
                    category VARCHAR(10),
                    year INT,
                    count INT,
                    farm INT,
                    area INT
                    );'''

cursor.execute(create_table)
conn.commit() 
print("Create Table successfully")                      

engine = create_engine("mysql+mysqlconnector://root:123456789@localhost/egg_data")
layer_alldata.to_sql(name='layer_alldata', con=engine, if_exists='replace', index=False)
print("Data inserted into MySQL successfully!")




































