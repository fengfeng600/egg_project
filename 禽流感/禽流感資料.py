import pandas as pd

years = [108, 109, 110, 111, 112, 113]

df_list = []

# 逐年讀取檔案
for year in years:
    filename = f"{year}年確診高病原性禽流感防疫處置表.csv"
    df = pd.read_csv(filename)
    df_list.append(df)

# 合併所有年度資料
combined_df = pd.concat(df_list, ignore_index=True)

#取出蛋雞/蛋中雞
filtered_df_1 = combined_df.loc[combined_df["禽種"]=="蛋雞"]
filtered_df_2 = combined_df.loc[combined_df["禽種"]=="蛋中雞"]
new_data = pd.concat([filtered_df_1,filtered_df_2],ignore_index=True)
new_data = new_data[["縣市","鄉鎮","禽種","撲殺日期","撲殺隻數"]]

#日期轉換
def convert_to_gregorian(x):
    if pd.notna(x):
        parts = x.split(".")
        year = int(parts[0])+1911
        month = parts[1]
        day = parts[2]
        return f"{year}.{month}.{day}"
    return x
new_data["撲殺日期"]=new_data["撲殺日期"].apply(convert_to_gregorian)
new_data["撲殺日期"] = pd.to_datetime(new_data["撲殺日期"],errors='coerce')
new_data["年"] = new_data["撲殺日期"].dt.year
new_data["月"] = new_data["撲殺日期"].dt.month

#欄位調整
new_data = new_data[["縣市","鄉鎮","禽種","年","月","撲殺隻數"]]
#每年每月的禽流感加總
new_data["撲殺隻數"] = new_data["撲殺隻數"].astype(str)
# 清除所有非數字字元（逗號、中文、空格等）
new_data["撲殺隻數"] = new_data["撲殺隻數"].str.replace(r"[^\d]", "", regex=True)
new_data["撲殺隻數"] = new_data["撲殺隻數"].astype(int)
new_data_sum = new_data.groupby(["年","月"])["撲殺隻數"].sum().reset_index()
#紀錄缺少的年月
existing = set(zip(new_data_sum["年"], new_data_sum["月"]))
years = [2019,2020,2021,2022,2023,2024]
months = list(range(1, 13)) 
full = set((y, m) for y in years for m in months)
missing = sorted(full - existing)
print("進口蛋缺少年份月")
for y, m in missing:
    print(f"缺少：{y} 年 {m} 月")
print("缺少",len(missing),"筆")
#不是每個月都紀錄禽流感，所以將填補為0
all_years_months = pd.DataFrame([(y, m) for y in years for m in months], columns=["年", "月"])
full_data = pd.merge(all_years_months,new_data_sum, on=["年", "月"], how="left")
full_data["撲殺隻數"] = full_data["撲殺隻數"].fillna(0).astype(int)

import mysql.connector
from sqlalchemy import create_engine

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456789",
    database="egg_data"
)
cursor = conn.cursor()
create_table = '''CREATE TABLE IF NOT EXISTS avian_area_data (
    `縣市` VARCHAR(10),
    `鄉鎮` VARCHAR(10),
    `禽種` VARCHAR(10),
    `年` INT,
    `月` INT,
    `撲殺隻數` INT
);'''
create_table1 = '''CREATE TABLE IF NOT EXISTS avian_data (
    `年` INT,
    `月` INT,
    `撲殺隻數` INT
);'''
cursor.execute(create_table)
cursor.execute(create_table1)
conn.commit()
print("Create Table successfully")


engine = create_engine("mysql+mysqlconnector://root:123456789@localhost/egg_data")
new_data.to_sql(name='avian_area_data', con=engine, if_exists='append', index=False)
full_data.to_sql(name='avian_data', con=engine, if_exists='replace', index=False)
print("Data inserted into MySQL successfully!")

















