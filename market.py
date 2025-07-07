import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров магазина через API Яндекс Маркета.

    Args:
        page (str): Токен страницы
        campaign_id (str): Идентификатор кампании продавца
        access_token (str): Токен для доступа к API

    Returns:
        dict: Ответ API со списком товаров магазина
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет остатки товаров в Яндекс Маркете.

    Args:
        stocks (list): Список словарей с остатками
        campaign_id (str): Идентификатор кампании продавца
        access_token (str): Токен для доступа к API

    Returns:
        dict: Ответ API с результатами обновления
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров в Яндекс Маркете.

    Args:
        prices (list): Список словарей с ценами
        campaign_id (str): Идентификатор кампании продавца
        access_token (str): Токен для доступа к API

    Returns:
        dict: Ответ API с результатами обновления цен
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает список всех артикулов товаров магазина в Яндекс Маркете.

    Args:
        campaign_id (str): Идентификатор кампании продавца
        market_token (str): Токен для доступа к API

    Returns:
        list: Список артикулов всех товаров магазина
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Формирует список остатков для обновления в Яндекс Маркете.

    Args:
        watch_remnants (list): Список остатков из файла поставщика
        offer_ids (list): Список артикулов товаров в магазине
        warehouse_id (str): Идентификатор склада в Яндекс Маркете

    Returns:
        list: Список словарей в формате для API:
            - sku (str): Артикул товара
            - warehouseId (str): ID склада
            - items (list): Информация об остатках:
                - count (int): Количество (0 если "1", 100 если ">10")
                - type (str): "FIT"
                - updatedAt (str): Время обновления в ISO формате
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует список цен для обновления в Яндекс Маркете.

    Args:
        watch_remnants (list): Список остатков из файла поставщика
        offer_ids (list): Список артикулов товаров в магазине

    Returns:
        list: Список словарей в формате для API:
            - id (str): Артикул товара
            - price (dict): Информация о цене:
                - value (int): Цена в копейках
                - currencyId (str): "RUR"
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно обновляет цены товаров в Яндекс Маркете.

    Args:
        watch_remnants (list): Список остатков из файла поставщика
        campaign_id (str): Идентификатор кампании продавца
        market_token (str): Токен для доступа к API

    Returns:
        list: Сформированный список всех цен для обновления
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Асинхронно обновляет остатки товаров в Яндекс Маркете.

    Args:
        watch_remnants (list): Список остатков из файла поставщика
        campaign_id (str): Идентификатор кампании продавца
        market_token (str): Токен для доступа к API
        warehouse_id (str): Идентификатор склада

    Returns:
        tuple: Кортеж из:
            - list: Товары с ненулевыми остатками
            - list: Все товары с обновленными остатками
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Основная функция для выполнения скрипта обновления цен и остатков.

    Workflow:
    1. Получает учетные данные из переменных окружения
    2. Загружает актуальные остатки с сайта поставщика
    3. Для каждой кампании (FBS и DBS):
        - Получает список товаров
        - Обновляет остатки
        - Обновляет цены

    Переменные окружения:
        - MARKET_TOKEN: Токен API
        - FBS_ID: ID кампании FBS
        - DBS_ID: ID кампании DBS
        - WAREHOUSE_FBS_ID: ID склада FBS
        - WAREHOUSE_DBS_ID: ID склада DBS
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
