from datetime import datetime
import json
import os
from airflow.sdk import DAG, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.smtp.operators.smtp import EmailOperator
import pandas as pd

with DAG(
    dag_id="etl_bmkg",
    start_date=datetime(2025, 6, 1),
    schedule="@daily",
    tags=["geocourse"]
) as dag:
    
    EXPORT_DIR_PATH = "/tmp/bmkg/"
    CURRENT_DATE = datetime.today().strftime("%Y-%m-%d")
    
    get_bmkg_data = HttpOperator(
        task_id="get_bmkg_cuaca_data",
        http_conn_id="bmkg_http",
        endpoint="publik/prakiraan-cuaca",
        data={"adm4": "34.71.04.1001"},
        method= "GET",
        headers={"Content-Type": "application/json"},
        log_response=True
    )

    @task
    def data_transformation(bmkg_response_str):
        data = json.loads(bmkg_response_str)
        result = []

        provinsi = data["lokasi"]["provinsi"],
        kabkot = data["lokasi"]["kotkab"],
        kecamatan = data["lokasi"]["kecamatan"],
        desa = data["lokasi"]["desa"],
        longitude = data["lokasi"]["lon"],
        latitude = data["lokasi"]["lat"],

        info_cuaca = data["data"][0]["cuaca"]
        for chunk_cuaca in info_cuaca:
            for cuaca in chunk_cuaca:
                waktu = cuaca["datetime"]
                suhu = cuaca["t"]
                deskripsi = cuaca["weather_desc"]
                angin = cuaca["ws"]

                result.append([
                    str(provinsi[0]), 
                    str(kabkot[0]), 
                    str(kecamatan[0]), 
                    str(desa[0]), 
                    float(longitude[0]), 
                    float(latitude[0]), 
                    str(deskripsi), 
                    float(suhu), 
                    float(angin),
                    str(waktu),
                ])

        return {
            "columns": [
                "provinsi",
                "kabkot",
                "kecamatan",
                "desa",
                "longitude",
                "latitude",
                "deskripsi_cuaca",
                "suhu_celcius",
                "kecepatan_angin_km_j",
                "waktu",
            ], 
            "data": result
        }
    
    @task
    def save_data_to_csv(data):
        os.makedirs(EXPORT_DIR_PATH, exist_ok=True)
        result = data.get("data", [])
        columns = data.get("columns", [])
        df = pd.DataFrame(result, columns=columns)
        df.to_csv(EXPORT_DIR_PATH + CURRENT_DATE + ".csv", index=False)

    bmkg_csv_file = FileSensor(
        task_id="bmkg_csv_file_waiting", 
        fs_conn_id="bmkg_fs",
        filepath=CURRENT_DATE + ".csv",
        poke_interval=10,
        timeout=60,
        mode="reschedule",
        soft_fail=True
    )

    send_email_with_csv = EmailOperator(
        task_id="send_bmkg_data_email",
        conn_id="natural_earth_email",
        to="azfaiz2022@gmail.com",
        from_email="ertim.geoportal@gmail.com",
        subject="Airflow: Daily BMKG Data CSV",
        html_content="""
            <h3>Hello Team,</h3>
            <p>The daily BMKG Weather Forcast data CSV file has been generated and is attached.</p>
            <p>Pipeline: <b>{{ dag.dag_id }}</b><br>
            Run ID: <b>{{ run_id }}</b></p>
            <p>Best regards,<br>
            Faiz Airflow Automation</p>
        """,
        files=[EXPORT_DIR_PATH + CURRENT_DATE + ".csv"],
    )
    
    transform = data_transformation(bmkg_response_str=get_bmkg_data.output)
    get_bmkg_data >> transform
    save_data_to_csv(data=transform)
    bmkg_csv_file >> send_email_with_csv