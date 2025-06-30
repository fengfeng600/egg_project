import xml.etree.ElementTree as ET
import pandas as pd
import os

all_data = []
folder = r"C:\Users\liaow\OneDrive\桌面\天氣"

# 命名空間處理：加上這行讓後續 find 可以正確使用 tag 名稱
ns = {'ns': 'urn:cwa:gov:tw:cwacommon:0.1'}

for filename in os.listdir(folder):
    if filename.endswith(".xml"):
        filepath = os.path.join(folder, filename)
        tree = ET.parse(filepath)
        root = tree.getroot()

       
        resources = root.find("ns:resources", ns)
        resource = resources.find("ns:resource", ns)
        data = resource.find("ns:data", ns)
        surfaceObs = data.find("ns:surfaceObs", ns)

        for location in surfaceObs.findall("ns:location", ns):
            station = location.find("ns:station", ns)
            station_name = station.find("ns:StationName", ns).text

            stats = location.find("ns:stationObsStatistics", ns)
            year_month = stats.find("ns:YearMonth", ns).text
            mean_temperature = stats.find("ns:AirTemperature", ns).find("ns:monthly", ns).find("ns:Mean", ns).text
            mean_humidity = stats.find("ns:RelativeHumidity", ns).find("ns:monthly", ns).find("ns:Mean", ns).text

            all_data.append({
                "地區": station_name,
                "年月": year_month,
                "平均溫度": mean_temperature,
                "平均濕度": mean_humidity
            })


all_data_files = pd.DataFrame(all_data)
all_data_files["年月"] = pd.to_datetime(all_data_files["年月"], errors='coerce') 
all_data_files["年"]=all_data_files["年月"].dt.year
all_data_files["月"]=all_data_files["年月"].dt.month
all_data_files = all_data_files.drop(["年月"],axis=1)
all_data_files["平均溫度"] = all_data_files["平均溫度"].astype(float)
all_data_files["平均濕度"] = all_data_files["平均濕度"].astype(float)
#更改欄位位置
all_data_files = all_data_files[["地區","年","月","平均溫度","平均濕度"]]
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
create_table = '''CREATE TABLE IF NOT EXISTS weather_data (
    `地區` VARCHAR(10),
    `年` INT,
    `月` INT,
    `平均溫度` FLOAT,
    `平均濕度` FLOAT
);'''

cursor.execute(create_table)
conn.commit()
print("Create Table successfully")


engine = create_engine("mysql+mysqlconnector://root:123456789@localhost/egg_data")
all_data_files.to_sql(name='weather_data', con=engine, if_exists='replace', index=False)
print("Data inserted into MySQL successfully!")




























