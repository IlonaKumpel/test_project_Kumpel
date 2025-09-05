import os
import json
import time
import hashlib
import datetime as dt
import requests
from dotenv import load_dotenv
import clickhouse_connect

load_dotenv()

URL = os.getenv("SOURCE_URL", "http://api.open-notify.org/astros.json")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "demo")

MAX_RETRIES = 5
BACKOFF_BASE = 1  


def fetch_json_with_retry(url: str, max_retries=MAX_RETRIES, backoff_base=BACKOFF_BASE) -> dict:
    """
    Получаем JSON с API и повторными попытками.
    Если сервер отвечает ошибкой или не отвечает вообще — пробуем снова,
    каждый раз увеличивая время ожидания.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "astros-ingestor/1.0"})

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=10)
            if resp.status_code == 429:
                wait = backoff_base ** attempt
                print(f"[{attempt}/{max_retries}] Сервер выдает ошибку 429 (Too Many Requests). Ждём {wait} секунд")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            print(f"[{attempt}/{max_retries}] JSON успешно получен")
            return resp.json()

        except Exception as e:
            if attempt == max_retries:
                raise RuntimeError(f"Все {max_retries} попытки неудачны. Ошибка: {e}") from e
            wait = backoff_base ** attempt
            print(f"[{attempt}/{max_retries}] Ошибка: {e}. Пробуем еще раз через {wait} секунд")
            time.sleep(wait)


def canonical_json_str(obj: dict) -> str:
    """
    Приводим JSON к каноническому виду, для хэширования:
    - убираем лишние пробелы,
    - сортируем ключи.
    """
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_hex(s: str) -> str:
    """Считаем SHA256-хеш от строки и возвращаем его в hex-формате."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def insert_raw_to_clickhouse(json_str: str, payload_hash: str, inserted_at: dt.datetime) -> None:
    """
    Подключаемся к ClickHouse и вставляем запись в таблицу demo.raw_astros.
    """
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database="demo"
    )
    client.insert(
        "demo.raw_astros",
        [[json_str, payload_hash, inserted_at]],
        column_names=["data", "payload_hash", "_inserted_at"],
    )
    print("→ Запись успешно вставлена в demo.raw_astros.")


def optimize_tables() -> None:
    """
    Ручная дедупликация данных 
    Выполняем OPTIMIZE для таблиц raw_astros и people
    """
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database="demo"
    )
    client.query("OPTIMIZE TABLE demo.raw_astros FINAL DEDUPLICATE")
    client.query("OPTIMIZE TABLE demo.people FINAL DEDUPLICATE")
    print("✓ Таблицы demo.raw_astros и demo.people оптимизированы, а дубликаты удалены.")


def main():
    print("Старт загрузки данных с API")
    payload = fetch_json_with_retry(URL)

    jstr = canonical_json_str(payload)
    phash = sha256_hex(jstr)
    now = dt.datetime.utcnow()

    print(f"Подготовка данных: hash={phash[:10]}… | timestamp={now.isoformat()}Z")
    insert_raw_to_clickhouse(jstr, phash, now)

    print("Готово: данные в ClickHouse. Materialized View автоматически обновил таблицу demo.people.")
    print("Для удаления дубликатов можно вручную запустить optimize_tables().")


if __name__ == "__main__":
    main()
