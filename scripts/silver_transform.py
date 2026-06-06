import json
import pandas as pd 
from pathlib import Path

def run_silver_transform(**context):
    execution_date = context['ds_nodash']

    bronze_file = context['ti'].xcom_pull(key='bronze_file' , task_ids='bronze_ingest')

    if not bronze_file:
        raise ValueError("No bronze file found in XCom for the current execution date.")

    silver_path = Path(f'/opt/airflow/data/silver')
    
    # Create the silver directory if it doesn't exist
    silver_path.mkdir(parents=True, exist_ok=True)

    with open(bronze_file) as f:
        #“อ่านข้อมูล JSON จากไฟล์” แล้วแปลงให้เป็น object ของ Python
        raw = json.load(f) 
    
    df_raw = pd.DataFrame(raw['states'])
    
    # กำหนดชื่อคอลัมน์ให้กับ DataFrame โดยอ้างอิงจากโครงสร้างของข้อมูลที่ได้รับมา ซึ่งในที่นี้เราจะตั้งชื่อคอลัมน์ตามลำดับของข้อมูลใน JSON
    df_raw.columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude",
        "latitude", "baro_altitude", "on_ground", 
        "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk",
        "spi", "position_source"
    ]

    # clean data - เลือกเฉพาะคอลัมน์ที่ต้องการ
    # ถ้าจะเอา colume เดียว df["A"] แต่ถ้าจะเอาหลสย colume df[["A","B"]]
    df = df_raw[
        [
            "icao24",
            "origin_country",
            "velocity",
            "on_ground"
        ]
    ]
    # / ใน Path คือ operator ของ pathlib = เอา folder + file มาต่อ path กัน
    #  path ของ silver_path + flights_{execution_date}.csv จะได้
    # /opt/airflow/data/silver/{execution_date}/flights_{execution_date}.csv
    output_file = silver_path / f'flights_{execution_date}.csv'
    df.to_csv(output_file, index=False) # ignore index column when saving to csv

    context['ti'].xcom_push(key='silver_file', value=str(output_file))

