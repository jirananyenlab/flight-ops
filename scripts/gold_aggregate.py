from pathlib import Path
import pandas as pd

def run_gold_aggregate(**context):
    silver_file = context["ti"].xcom_pull(key="silver_file" ,task_ids="silver_transform")
    if not silver_file:
        raise ValueError("No silver file found in XCom for the current execution date.")
    
    df = pd.read_csv(silver_file)

    # จัดกลุ่มตามประเทศ แล้วสรุปข้อมูลเที่ยวบิน
    agg = (
        df.groupby("origin_country")
        .agg(
            total_flights=("icao24", "count"),
            avg_velocity=("velocity", "mean"),
            on_ground=("on_ground", "sum")
        )
        .reset_index()
    )
    # หลัง groupby origin_country จะกลายเป็น index , reset_index ทำให้กลับมาเป็น column ปกติ

    # silver_file = /opt/airflow/data/silver/flights_{execution_date}.csv
    # gold_path = /opt/airflow/data/gold/flights_{execution_date}.csv  เพราะ แทนที่ silver ด้วย gold
    gold_path = Path(silver_file.replace("silver", "gold"))
    
    # ไม่ต้องมี บรรทัดนี้ ถ้าไม่ทำ process ต่อไป เช่น ส่งdata ไป Snowflake หรือส่งไป dashboard อื่น ๆ 
    # แต่ถ้าจะทำต่อก็ต้องมีบรรทัดนี้ เพื่อให้ขั้นตอนถัดไปรู้ว่าไฟล์ gold อยู่ที่ไหน
    context["ti"].xcom_push(key="gold_file", value=str(gold_path))
    
    # create output as new CSV file
    agg.to_csv(gold_path, index=False)


    # gold_path.mkdir() สร้างโฟลเดอร์จาก path นี้โดยตรง
    # gold_path.parent.mkdir() สร้างโฟลเดอร์ของไฟล์ (folder เท่านั้น)