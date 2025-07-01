import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
import urllib


def get_cookies_from_browser(url):
    """Открывает браузер и получает cookies после ручного входа"""
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

        # Если пришёл dict, но внутри есть список возвратов
        if isinstance(data, dict):
            # ищем первый список
            for key, value in data.items():
                if isinstance(value, list):
                    data = value
                    break
            else:
                raise ValueError("❌ Не найден список в структуре JSON")
        elif not isinstance(data, list):
            raise ValueError("❌ Формат ответа неизвестен")



        # Проверь, что это список
        if not isinstance(data, list):
            print("📦 Ответ от сервера:")
            print(json.dumps(data, indent=2, ensure_ascii=False))
            raise ValueError("❌ Ожидался список объектов (возвраты), но получен другой формат")

        # Основная таблица: возвраты
        order_df = pd.json_normalize(data, sep="_", max_level=1)

        # Подтаблица: return_products
        order_products_list = []
        for order in data:
            order_id = order.get("deal_id")
            for product in order.get("return_products", []):
                product["order_id"] = order_id
                order_products_list.append(product)
        order_products_df = pd.DataFrame(order_products_list)

        # Подтаблица: details (если они есть внутри return_products)
        details_list = []
        for product in order_products_list:
            product_id = product.get("product_unit_id")
            order_id = product.get("order_id")
            for detail in product.get("details", []):
                detail["product_id"] = product_id
                detail["order_id"] = order_id
                details_list.append(detail)
        details_df = pd.DataFrame(details_list)

        print(f"✅ Получено: {len(order_df)} возвратов, {len(order_products_df)} товаров, {len(details_df)} деталей")
        return {
            "orders": order_df,
            "order_products": order_products_df,
            "order_details": details_df
        }

    except Exception as e:
        print(f"❌ Ошибка при загрузке: {e}")
        return None

def upload_to_sql(df_dict):
    try:
        print("🔌 Подключение к SQL Server...")
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=Epco;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        for table_name, df in df_dict.items():
            if df.empty or df.columns.empty:
                print(f"⏭ Таблица {table_name} пуста или не содержит столбцов — пропущено.")
                continue
            print(f"📥 Загрузка в таблицу: {table_name} ({len(df)} строк)")
            df.to_sql(table_name, con=engine, index=False, if_exists="replace")

        print("✅ Все таблицы успешно записаны в SQL Server.")
    except Exception as e:
        print(f"❌ Ошибка при записи в SQL: {e}")


        for table_name, df in df_dict.items():
         if df.empty:
          print(f"⏭ Таблица {table_name} пуста — пропущено.")
          continue
    print(f"📥 Загрузка в таблицу: {table_name} ({len(df)} строк)")
    df.to_sql(table_name, con=engine, index=False, if_exists="replace")


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/trade/txs/tdeal/order$export"  # твой исходный URL
    df_dict = fetch_and_flatten(DATA_URL)
    if df_dict:
        upload_to_sql(df_dict)
