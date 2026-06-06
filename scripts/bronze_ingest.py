import requests
import json
from datetime import datetime
from pathlib import Path

URL = "https://opensky-network.org/api/states/all"

# **context เป็นพารามิเตอร์ที่ Airflow จะส่งเข้ามาเมื่อฟังก์ชันนี้ถูกเรียกใช้ใน task ของ Airflow ซึ่ง context นี้จะมีข้อมูลต่าง ๆ
#  เกี่ยวกับการทำงานของ task และ DAG ที่กำลังรันอยู่ เช่น execution date, task instance, และอื่น ๆ 
# ที่สามารถนำมาใช้ในการประมวลผลหรือส่งข้อมูลระหว่าง task ได้

# เครื่องหมาย ** หมายถึงรับ Keyword Arguments ทั้งหมด เป็น Dictionary 
# ซึ่งในที่นี้ context จะเป็น Dictionary ที่มีข้อมูลต่าง ๆ ที่ Airflow ส่งมาให้
def run_bronze_ingest(**context):
    response = requests.get(URL, timeout=30) 
    response.raise_for_status()  # Raise an exception for HTTP errors
    
    data = response.json()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # Define the output path
    # /opt/airflow จาก volume ที่เราแมปไว้ใน docker-compose.yml
    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")
    
    # สร้างไฟล์ ตาม path ที่กำหนดไว้
    with open(path, "w") as f:
        # แปลง Python Object เป็น JSON แล้วเขียนลงไฟล์
        json.dump(data, f)

    # ส่ง path ของไฟล์ที่สร้างขึ้นไปยังขั้นตอนถัดไปใน Airflow โดยใช้ XCom
    # แต่ละ task ใน Airflow สามารถส่งข้อมูลระหว่างกันได้ผ่าน XCom (Cross-Communication)
    context['ti'].xcom_push(key='bronze_file', value=str(path))