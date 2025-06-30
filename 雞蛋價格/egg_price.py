import pandas as pd

egg_data = pd.read_csv("家禽交易行情(白肉雞雞蛋).csv")[["日期","雞蛋(產地)"]]

egg_data["日期"]=pd.to_datetime(egg_data["日期"])
egg_data["年"] = egg_data["日期"].dt.year
egg_data["月"] = egg_data["日期"].dt.month

egg_data = egg_data.rename(columns={"雞蛋(產地)": "國產蛋價格"})
egg_data["國產蛋價格"] = pd.to_numeric(egg_data["國產蛋價格"], errors='coerce') #將資料轉為數值，如果資料轉換失敗（例如 '無資料'、'-'、空值），就自動變成 NaN（缺值）而不報錯

#篩選出2019-2024年資料
egg_newdata = egg_data.loc[(egg_data["年"] >= 2019) & (egg_data["年"] <= 2024)].copy()

#蛋價有缺值，每月的中位數填補
egg_newdata["該月中位數"] = egg_newdata.groupby(["年","月"])["國產蛋價格"].transform("median")
egg_newdata["國產蛋價格"] = egg_newdata["國產蛋價格"].fillna(egg_newdata["該月中位數"])

#算出每年每月的蛋價平均四捨五入到小數點後2位
domestic_eggs = egg_newdata.groupby(["年","月"])["國產蛋價格"].mean().round(2)
domestic_eggs = domestic_eggs.reset_index()
domestic_eggs["來源"] = "國產"

#寫入MYSQL
import mysql.connector
from sqlalchemy import create_engine
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456789"
)

cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS egg_data")
cursor.execute("USE egg_data")
create_table_sql = '''CREATE TABLE IF NOT EXISTS egg_price (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year INT,
    month INT,
    price DECIMAL(10, 2),
    source VARCHAR(10)
);'''
print("Create Table successufully")
engine = create_engine("mysql+mysqlconnector://root:123456789@localhost/egg_data")
domestic_eggs.to_sql(name='egg_price', con=engine, if_exists='replace', index=False)
print("Data inserted into MySQL successfully!")
























