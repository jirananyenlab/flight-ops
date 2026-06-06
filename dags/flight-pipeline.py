import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# การสร้างตัวแปรที่อ้างถึงโฟลเดอร์หลักของ Airflow ในรูปแบบ Path object เพื่อให้สามารถจัดการ path และไฟล์ต่าง ๆ 
# ได้สะดวกและปลอดภัยกว่าการใช้ String ธรรมดา เช่น การต่อ path, สร้างโฟลเดอร์, อ่าน/เขียนไฟล์ เป็นต้น
AIRFLOW_HOME = Path("/opt/airflow")

# เพิ่ม AIRFLOW_HOME ลงใน sys.path เพื่อให้ Python สามารถนำเข้าโมดูลจากโฟลเดอร์นี้ได้
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.append(str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingest
from scripts.silver_transform import run_silver_transform
from scripts.gold_aggregate import run_gold_aggregate
from scripts.load_gold_to_snowflake import load_gold_to_snowflake

default_args = {
    'owner': 'airflow',
    'retries': 1, # กำหนดจำนวนครั้งที่ task จะถูก retry เมื่อเกิดความล้มเหลว
    'retry_delay': timedelta(minutes=5) # กำหนดเวลาที่จะรอก่อนที่จะ retry task -> อีกครั้ง 5 นาที
}

with DAG(
    dag_id='flight_ops_medallion_pipe', # ชื่อของ DAG
    default_args=default_args,
    description='A simple flight data pipeline',
    schedule_interval="*/30 * * * *", # กำหนดให้ DAG นี้ทำงานทุก ๆ 30 นาที
    start_date=datetime(2026, 6, 1), # กำหนดวันที่เริ่มต้นของ DAG
    catchup=False, # ปิดการทำงานย้อนหลัง (ไม่ต้องรัน DAG สำหรับช่วงเวลาที่ผ่านมา)
    tags=['flight'] # เพิ่ม tag เพื่อช่วยในการจัดกลุ่มและค้นหา DAG ใน Airflow UI
) as dag:
    bronze = PythonOperator(
        task_id='bronze_ingest', # ชื่อของ task 
        python_callable=run_bronze_ingest, # ฟังก์ชันที่จะถูกเรียกเมื่อ task นี้ทำงาน
    )
    silver = PythonOperator(
        task_id='silver_transform', 
        python_callable=run_silver_transform, 
    )
    gold = PythonOperator(
        task_id='gold_aggregate', 
        python_callable=run_gold_aggregate, 
    )

    load_to_snowflake = PythonOperator(
        task_id='load_to_snowflake', 
        python_callable=load_gold_to_snowflake, 
    )

    # กำหนดลำดับการทำงานของ task โดยให้ task bronze ทำงานก่อนแล้วค่อยตามด้วย task silver
    bronze >> silver >> gold >> load_to_snowflake