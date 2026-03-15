import io
import requests
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def fetch_reviews_page(app_id, params):
    """
    Один GET-запрос к Steam Reviews API.
    Возвращает сырой JSON payload.
    """
    url = f"https://store.steampowered.com/appreviews/{app_id}"

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if payload.get("success") != 1:
        raise ValueError(f"Steam API вернул не success=1, а success={payload.get('success')}")

    return payload


def collect_reviews_for_date(app_id, target_date, params):
    params = params.copy()
    all_pages = []  # Список под датафреймы с отзывами

    while True:
        payload = fetch_reviews_page(app_id=app_id, params=params)
        reviews = payload.get("reviews", [])

        # Останавливаемся, если API больше не отдает отзывы
        if len(reviews) == 0:
            print("Отзывы закончились, остановка")
            break

        page_df = pd.DataFrame(reviews)

        author_df = pd.json_normalize(page_df["author"])
        author_df.columns = [f"author_{col}" for col in author_df.columns]
        page_df = pd.concat([page_df.drop(columns=["author"]), author_df], axis=1)

        page_df["timestamp_created_dt"] = pd.to_datetime(
            page_df["timestamp_created"], unit="s", utc=True
        )
        page_df["created_date"] = page_df["timestamp_created_dt"].dt.date

        page_df["timestamp_updated_dt"] = pd.to_datetime(
            page_df["timestamp_updated"], unit="s", utc=True
        )
        page_df["updated_date"] = page_df["timestamp_updated_dt"].dt.date

        filtered_page_df = page_df[page_df["created_date"] == target_date].copy()

        # Оставляем как есть: даже пустые куски можно складывать, concat их переживет
        all_pages.append(filtered_page_df)

        print(
            f"Итерация: всего строк={len(page_df)}, "
            f"за нужную дату={len(filtered_page_df)}, "
            f"min_date={page_df['created_date'].min()}, "
            f"max_date={page_df['created_date'].max()}"
        )

        # Если вся страница уже старше target_date, дальше искать бессмысленно
        if (page_df["created_date"] < target_date).all():
            print(f"Текущая страница уже старше {target_date}, остановка")
            break

        params["cursor"] = payload["cursor"]


    if len(all_pages) == 0:
        return pd.DataFrame()

    result_df = pd.concat(all_pages, ignore_index=True)
    return result_df


def run_reviews_job(target_date, app_id=730):
    """
    Основной job:
    1. Проверяем, есть ли parquet за target_date в MinIO
    2. Если есть — пропускаем загрузку
    3. Если нет — собираем отзывы из Steam API
    4. Пишем parquet в память
    5. Грузим parquet в MinIO
    """
    target_date = datetime.strptime(target_date, "%Y-%m-%d").date()

    params = {
        "json": 1,
        "language": "all",
        "filter": "recent",
        "num_per_page": 100,
        "cursor": "*"
    }

    hook = S3Hook(aws_conn_id="minios3_conn")

    key = f"EvgSergeev_steam_reviews/app_id={app_id}/dt={target_date}/reviews.parquet"

    # Проверка идемпотентности: если файл уже есть, второй раз дату не собираем
    if hook.check_for_key(key=key, bucket_name="dev"):
        print(f"Файл за дату {target_date} уже существует в MinIO, загрузку пропускаем")
        return {
            "status": "skipped_already_exists",
            "app_id": app_id,
            "target_date": str(target_date),
            "bucket": "dev",
            "key": key
        }

    df = collect_reviews_for_date(
        app_id=app_id,
        target_date=target_date,
        params=params
    )

    # Даже если df пустой — все равно пишем parquet
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    hook.load_bytes(
        bytes_data=buffer.read(),
        key=key,
        bucket_name="dev",
        replace=True
    )

    print(f"Файл успешно загружен в s3://dev/{key}")

    return {
        "status": "uploaded",
        "app_id": app_id,
        "target_date": str(target_date),
        "rows": int(len(df)),
        "bucket": "dev",
        "key": key
    }


default_args = {
    "owner": "evgeny",
    "start_date": datetime(2026, 3, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="steam_reviews_api_to_s3",
    default_args=default_args,
    schedule="0 10 * * *",
    catchup=True,
    description="Steam reviews API to MinIO parquet",
    tags=["steam", "reviews", "api", "s3", "minio"],
)

upload_reviews_to_s3 = PythonOperator(
    task_id="upload_reviews_to_s3",
    python_callable=run_reviews_job,
    op_kwargs={
        "target_date": "{{ ds }}",
        "app_id": 730
    },
    dag=dag,
)