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


def fetch_and_flatten(data_url_inventory, data_url_groups):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")
        print("⬇️ Загружаем данные...")

        # Получаем только нужные данные
        inventory_data = requests.get(data_url_inventory, cookies=cookies).json()
        group_data = requests.get(data_url_groups, cookies=cookies).json()

        # Извлекаем только нужное из inventory
        flat_rows = []
        for item in inventory_data.get("inventory", []):
            product_id = item.get("product_id")
            code = item.get("code")
            groups = item.get("groups", [])
            for group in groups:
                flat_rows.append({
                    "product_id": product_id,
                    "code": code,
                    "group_code": group.get("group_code"),
                    "type_id": group.get("type_id")
                })
        inventory_groups_df = pd.DataFrame(flat_rows)

        # Расплющиваем product_groups
        product_group_df = pd.json_normalize(group_data.get("product_groups", []))[
            ["product_type_id", "product_group_code", "product_group_name"]
        ]

        # Объединение по type_id
        merged_df = pd.merge(
            inventory_groups_df,
            product_group_df,
            how="left",
            left_on="type_id",
            right_on="product_type_id"
        )

        print(f"✅ Объединено: {len(merged_df)} строк")
        return {"ProductType_Merged": merged_df}

    except Exception as e:
        print(f"❌ Ошибка при загрузке и объединении: {e}")
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
            print(f"📥 Загрузка в таблицу: {table_name} ({len(df)} строк)")
            df.to_sql(table_name, con=engine, index=False, if_exists="replace")

        print("✅ Все данные успешно записаны в SQL Server.")

    except Exception as e:
        print(f"❌ Ошибка при записи в SQL: {e}")
        for table_name, df in df_dict.items():
            if df.empty or df.columns.empty:
                print(f"⏭ Таблица {table_name} пуста — пропущено.")


if __name__ == "__main__":
    DATA_URL_INVENTORY = "https://smartup.online/b/anor/mxsx/mr/inventory$export"
    DATA_URL_GROUPS = "https://smartup.online/b/anor/mxsx/mr/product_group$export"

    df_dict = fetch_and_flatten(DATA_URL_INVENTORY, DATA_URL_GROUPS)
    if df_dict:
        upload_to_sql(df_dict)
