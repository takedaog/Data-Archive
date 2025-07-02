import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
import urllib


def get_cookies_from_browser(url):
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    input("🌐 Зайдите на сайт и нажмите Enter после авторизации...")
    cookies = driver.get_cookies()
    driver.quit()
    return {cookie['name']: cookie['value'] for cookie in cookies}


def fetch_and_flatten(data_url):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")
        print("⬇️ Загружаем данные...")
        response = requests.get(data_url, cookies=cookies)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    data = value
                    break
            else:
                raise ValueError("❌ Не найден список в структуре JSON")
        elif not isinstance(data, list):
            raise ValueError("❌ Формат ответа неизвестен")

        # Основная таблица — input без вложенных полей
        input_df = pd.json_normalize(
            data,
            sep="_",
            max_level=1
        ).drop(columns=["input_items", "supplier_codes"], errors="ignore")

        # Отдельно — input_items
        input_items_list = []
        suppliers_list = []

        for row in data:
            input_id = row.get("input_id")
            supplier_codes = row.get("supplier_codes", [])

            # Если список есть, но даже пустой — всё равно сохраняем input_id
            if supplier_codes:
                for sup in supplier_codes:
                    sup = sup or {}  # если None, превращаем в пустой dict
                    sup["input_id"] = input_id
                    suppliers_list.append(sup)

        input_items_df = pd.DataFrame(input_items_list)
        suppliers_df = pd.DataFrame(suppliers_list)

        print(f"✅ Получено: {len(input_df)} записей, {len(input_items_df)} товаров, {len(suppliers_df)} поставщиков")

        return {
            "ReceiptsWH_inputs": input_df,
            "ReceiptsWH_input_items": input_items_df,
            "ReceiptsWH_suppliers": suppliers_df
        }

    except Exception as e:
        print(f"❌ Ошибка при загрузке: {e}")
        return None


def upload_to_sql(df_dict):
    try:
        print("🔌 Подключение к SQL Server...")
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=TAKEDA;"
            "DATABASE=DealDB;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        for table_name, df in df_dict.items():
            if df.empty or df.columns.empty:
                print(f"⏭ Таблица {table_name} пуста или без столбцов — пропущено.")
                continue
            print(f"📥 Загрузка в таблицу: {table_name} ({len(df)} строк)")
            df.to_sql(table_name, con=engine, index=False, if_exists="replace")
        print("✅ Все данные успешно записаны в SQL Server.")

    except Exception as e:
        print(f"❌ Ошибка при записи в SQL: {e}")
        for table_name, df in df_dict.items():
            if df.empty or df.columns.empty:
                print(f"⏭ Таблица {table_name} пуста — пропущено.")


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/anor/mxsx/mkw/input$export"
    df_dict = fetch_and_flatten(DATA_URL)
    if df_dict:
        upload_to_sql(df_dict)
