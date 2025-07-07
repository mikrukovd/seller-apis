import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров магазина Ozon через Seller API.

    Args:
        last_id (str): Идентификатор последнего полученного элемента
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): Токен продавца для авторизации API

    Returns:
        dict: Ответ API со списком товаров магазина
    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает список всех артикулов товаров магазина Ozon.

    Args:
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): Токен продавца для авторизации API

    Returns:
        list: Список артикулов (offer_id) всех товаров магазина
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров в магазине Ozon.

    Args:
        prices (list): Список словарей с ценами товаров
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): Токен продавца для авторизации API

    Returns:
        dict: Ответ API Ozon с результатами обновления цен
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров в магазине Ozon.

    Args:
        stocks (list): Список словарей с остатками
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): Токен продавца для авторизации API

    Returns:
        dict: Ответ API Ozon с результатами обновления остатков
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает и обрабатывает файл с остатками товаров с сайта Casio.

    Returns:
        list: Список словарей с информацией о товарах
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Формирует список остатков для обновления в Ozon.

    Args:
        watch_remnants (list): Список остатков из файла Casio
        offer_ids (list): Список артикулов товаров в магазине Ozon

    Returns:
        list: Список словарей для обновления остатков, где каждый словарь содержит:
            - offer_id (str): Артикул товара
            - stock (int): Количество товара (0 если "1", 100 если ">10", иначе число)
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует список цен для обновления в Ozon.

    Args:
        watch_remnants (list): Список остатков из файла Casio
        offer_ids (list): Список артикулов товаров в магазине Ozon

    Returns:
        list: Список словарей для обновления цен, где каждый словарь содержит:
            - offer_id (str): Артикул товара
            - price (str): Цена товара (после преобразования)
            - old_price (str): Старая цена ("0" по умолчанию)
            - currency_code (str): Валюта ("RUB")
            - auto_action_enabled (str): Флаг автодействий ("UNKNOWN")
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Удаляет все нецифрованные символы из строки с ценой.

    Функция разбивает строку по точке (если она есть) и оставляет только цифры
    в левой части (целая часть цены), удаляя все остальные символы.

    Args:
        price: Строка с ценой. Может содержать цифры, разделители тысяч и
        точку как разделитель дробной части.

    Returns:
        Строка из цифр, или пустая если нет цифр и целой части цены.

    Examples:
        >>> price_conversion("12345.67")
        '12345'
        >>> price_conversion(".12")
        ''
        >>> price_conversion("price")
        ''
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части фиксированного размера.

    Args:
        lst (list): Исходный список для разделения
        n (int): Размер одного подсписка

    Yields:
        list: Подсписок из n элементов

    Examples:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет цены товаров в Ozon.

    Args:
        watch_remnants (list): Список остатков из файла Casio
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): Токен продавца для авторизации API

    Returns:
        list: Сформированный список всех цен для обновления
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет остатки товаров в Ozon.

    Args:
        watch_remnants (list): Список остатков из файла Casio
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): Токен продавца для авторизации API

    Returns:
        tuple: Кортеж из двух элементов:
            - list: Товары с ненулевыми остатками
            - list: Все товары с обновленными остатками
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для выполнения скрипта обновления цен и остатков.

    Workflow:
    1. Получает учетные данные из переменных окружения
    2. Загружает список товаров магазина Ozon
    3. Скачивает актуальные остатки с сайта Casio
    4. Обновляет остатки в Ozon
    5. Обновляет цены в Ozon
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
