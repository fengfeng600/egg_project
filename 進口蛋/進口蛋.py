import pandas as pd
egg_=pd.read_csv("海關進口蛋.csv")
#egg_.info()

#日期改為西元年
egg_[["年", "月"]] = egg_["日期"].str.extract(r'(\d+)年(\d+)月')
egg_["年"] = egg_["年"].astype(int) + 1911
egg_["月"] = egg_["月"].astype(int)

egg_= egg_.drop(["進出口別","貨品號列","中文貨名","英文貨名"],axis=1)

egg_ = egg_.rename(columns={
    '重量(公噸)': '公噸',
    '重量(公斤)': '公斤'})

#公斤換算成台斤，因為雞蛋價格是每一台斤多錢 1 公斤 = 1.6667 台斤（常用四捨五入成 1.67）
egg_["台斤"] = egg_["公斤"]*1.67
egg_["台斤"]= egg_["台斤"].apply(lambda x: round(x)) 
egg_["進口蛋價格"] = ((egg_["新臺幣(千元)"] * 1000) / egg_["台斤"]).round(2)

#找出缺少的（年, 月）組合
existing = set(zip(egg_["年"], egg_["月"]))
years = [2019,2020,2021,2022, 2023, 2024]
months = list(range(1, 13)) 
full = set((y, m) for y in years for m in months)
missing = sorted(full - existing)
print("進口蛋缺少年份月")
for y, m in missing:
    print(f"缺少：{y} 年 {m} 月")
print("缺少",len(missing),"筆")
egg_["來源"] = "進口"
imported_eggs = egg_[["年","月","台斤","新臺幣(千元)","進口蛋價格","來源"]]


#不是每個月都紀錄，所以將填補為0
all_years_months = pd.DataFrame([(y, m) for y in years for m in months], columns=["年", "月"])
imported_eggs = pd.merge(all_years_months,imported_eggs, on=["年", "月"], how="left")
columns_to_fill = ["台斤", "新臺幣(千元)", "進口蛋價格"]
for col in columns_to_fill:
    imported_eggs[col] = imported_eggs[col].fillna(0)
    
imported_eggs["來源"] = imported_eggs["來源"].fillna("進口")    
imported_eggs["台斤"] = imported_eggs["台斤"].astype(int)
#寫入SQL
import mysql.connector
from sqlalchemy import create_engine

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456789",
    database="egg_data"
)
cursor = conn.cursor()
create_table = '''CREATE TABLE IF NOT EXISTS imported_eggs (
    `年` INT,
    `月` INT,
    `台斤` INT,
    `新臺幣(千元)` INT,
    `進口蛋價格` FLOAT,
    `來源` VARCHAR(10)
);'''

cursor.execute(create_table)
conn.commit()
print("Create Table successfully")


engine = create_engine("mysql+mysqlconnector://root:123456789@localhost/egg_data")
imported_eggs.to_sql(name='imported_eggs', con=engine, if_exists='replace', index=False)
print("Data inserted into MySQL successfully!")





















































