import pandas as pd
import snowflake.connector

# Hook = ตัวกลางสำหรับคุยกับระบบภายนอก  connect to the external service 
# แทนที่จะเขียน connect เองทุกครั้ง Airflow จะให้ Hook มาจัดการให้
# BaseHook = ตัวพื้นฐานของการดึง connection ใน Airflow
# ใช้เพื่อ: ดึง connection จาก Airflow UI -> เชื่อม database / API / cloud ->  สร้าง custom hook เอง
from airflow.hooks.base import BaseHook 

def load_gold_to_snowflake(**context):
    gold_file = context["ti"].xcom_pull(key="gold_file", task_ids="gold_aggregate")
    
    if not gold_file:
        raise ValueError("No gold file found in XCom")
    
    #ดึง “ช่วงเวลาเริ่มต้นของรอบการรัน DAG” (start of data interval) จาก context ของ task
    # ตั้ง 30 นาที เช่น DAG run 8.30 จะประมวลผลข้อมูลของ 8.00
    # data_interval_start = 8.00 :for DAG run 8.30
    execution_date = context['data_interval_start'].strftime('%Y-%m-%d %H:%M:%S')
    # data_interval_end = 8.30 

    df = pd.read_csv(gold_file)

    # สร้าง connection ไปยัง Snowflake ใน aireflow
    conn = BaseHook.get_connection("flight_snowflake") # flight_snowflake คือ connection id ที่ตั้งไว้ใน Airflow UI
    
    sf_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        private_key_file = "/opt/airflow/secrets/rsa_key.p8",
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.extra_dejson.get("database"),
        schema=conn.schema,
        role=conn.extra_dejson.get("role")
    )

    # sql  merge data 
    # tgt = Target Table (ตารางปลายทาง)
    # src = Source Data (ข้อมูลใหม่ที่จะเอาเข้า) 

    # USING (...) คือ สร้าง source table ชั่วคราวให้ MERGE ใช้  ,เป็นข้อมูลใหม่ที่ Python ส่งเข้ามา 
    # USING (...) src สร้างขึ้นเพื่อให้ MERGE มี ข้อมูลฝั่ง Source สำหรับนำไปเปรียบเทียบกับข้อมูลในตารางปลายทาง (tgt)

    # โดยใช้ %s + cursor.execute(sql, params) เป็น parameterized query 
    # เพื่อป้องกัน SQL Injection และจัดการกับข้อมูลที่ส่งเข้ามาอย่างปลอดภัย
    merge_sql = """ 
    MERGE INTO FLIGHT_KPI tgt
    USING (
        SELECT
            TO_TIMESTAMP(%s) AS WINDOW_START,
            %s AS ORIGINAL_COUNTRY,
            %s AS TOTAL_FLIGHTS,
            %s AS AVG_VELOCITY,
            %s AS ON_GROUND
    ) src
    ON tgt.WINDOW_START = src.WINDOW_START AND 
    tgt.ORIGINAL_COUNTRY = src.ORIGINAL_COUNTRY
    WHEN MATCHED THEN UPDATE SET
        TOTAL_FLIGHTS = src.TOTAL_FLIGHTS,
        AVG_VELOCITY = src.AVG_VELOCITY,
        ON_GROUND = src.ON_GROUND,
        LOAD_TIME = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (WINDOW_START, ORIGINAL_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, ON_GROUND)
        VALUES
        (src.WINDOW_START, src.ORIGINAL_COUNTRY, src.TOTAL_FLIGHTS, src.AVG_VELOCITY, src.ON_GROUND);

"""

    # Cursor เปรียบเสมือน มือที่ใช้ส่ง SQL เข้า Database
    # Parameterized Query (%s) ค่าจริงจะถูกส่งมาจากตรงนี้
    with sf_conn.cursor() as cursor:
            for _, row in df.iterrows():
                cursor.execute(
                    merge_sql,
                    (
                        execution_date,
                        row["origin_country"],
                        int(row["total_flights"]),
                        float(row["avg_velocity"]),
                        int(row["on_ground"]),
                    ),
                )

    sf_conn.close()