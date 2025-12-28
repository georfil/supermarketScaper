# scrapers/supermarkets/ab.py

import requests
from bs4 import BeautifulSoup
import json


def fetch_categories():
    url = "https://www.ab.gr/"
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    return [
        "https://www.ab.gr/" + a["href"]
        for a in soup.find_all("a")
        if "/c/" in a.get("href", "")
    ]


def fetch_products(category_code, page):
    url = "https://www.ab.gr/api/v1/"

    params = {
        "operationName": "GetCategoryProductSearch",
        "variables": json.dumps({
            "lang": "gr",
            "category": category_code,
            "pageNumber": page,
            "pageSize": 50,
            "fields": "PRODUCT_TILE"
        }),
        "extensions": json.dumps({
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "afce78bc1a2f0fe85f8592403dd44fae5dd8dce455b6eeeb1fd6857cc61b00a2"
            }
        })
    }

    response = requests.get(url, params=params)
    return response.json()


def scrape():
    products = []
    categories = fetch_categories()

    for category_url in categories:
        category_code = category_url.split("c/")[1]
        first_page = fetch_products(category_code, 0)

        pagination = first_page["data"]["categoryProductSearch"]["pagination"]
        total_pages = pagination["totalPages"]

        products.extend(
            first_page["data"]["categoryProductSearch"]["products"]
        )

        for page in range(1, total_pages):
            data = fetch_products(category_code, page)
            products.extend(
                data["data"]["categoryProductSearch"]["products"]
            )

    return products
