# transformers/ab_transformer.py

import pandas as pd
import numpy as np
import datetime as dt
import json 
import requests
from scrapers.ab.ab import HEADERS

TODAY = dt.date.today().strftime("%d-%m-%Y")


PRIVATE_LABELS = {
    "ΑΒ",
    "ΑΒ ΒΙΟ",
    "ΑΒ ΕΠΙΛΟΓΗ",
    "ΑΒ ΚΟΝΤΑ ΣΤΗΝ ΕΛΛΗΝΙΚΗ ΓΗ",
    "ΑΒ THINK BIO",
    "ΑΒ FRESH TO GO",
    "ΑΒ THINK NUTRI",
    "ΑΒ ΕΤΟΙΜΑ ΓΕΥΜΑΤΑ",
}


def fetchUniqueCategories():
    url = "https://www.ab.gr/api/v1/"

    params = {
        "operationName": "LeftHandNavigationBar",
        "variables": json.dumps({"rootCategoryCode":"","cutOffLevel":"4","lang":"gr"}, separators=(',', ':')),
        "extensions": json.dumps(
        {"persistedQuery":{"version":1,"sha256Hash":"29a05b50daa7ab7686d28bf2340457e2a31e1a9e4d79db611fcee435536ee01c"}}
        , separators=(',', ':'))
    }

    response = requests.request("GET", url, headers=HEADERS, params=params)
    return response.json()


def getCategoryMappings():
    data = fetchUniqueCategories()['data']['leftHandNavigationBar']['categoryTreeList']
    cat_mappings = {}
    for lvl in data:
        cats = lvl['categoriesInfo']
        for cat in cats:
            for sub in cat['levelInfo']:
                cat_mappings.update({sub['url'].split('/c')[0].split('/')[-1]:sub['name']})
    return cat_mappings


def transform(raw_products):
    df = pd.DataFrame(raw_products)
    cat_mappings = getCategoryMappings()

    df['product_type'] = df['firstLevelCategory'].apply(lambda x: x['name'])
    df['product_subtype_level1'] = df['url'].apply(lambda url: url.split('eshop/')[1].split('/')[1].split('/')[0])
    df['product_subtype_level1'] = df['product_subtype_level1'].map(cat_mappings)
    df['product_subtype_level2'] = df['url'].apply(lambda url: url.split('eshop/')[1].split('/')[2].split('/')[0])
    df['product_subtype_level2'] = df['product_subtype_level2'].map(cat_mappings)

    df["original_product_code"] = df["code"]
    df["brand"] = df["manufacturerName"]
    df["supermarket_code"] = "ab"

    df["pricePerKilo"] = df["price"].apply(
        lambda p: float(p["supplementaryPriceLabel1"]
                        .split("€/")[0]
                        .replace(",", "."))
        if p.get("supplementaryPriceLabel1") and "€/" in p["supplementaryPriceLabel1"]
        else np.nan
    )

    df["unit"] = df["price"].apply(lambda x: x["unit"])
    df["price"] = df["price"].apply(lambda x: x["value"])


    df["product_url"] = "https://www.ab.gr" + df["url"]
    df["main_image"] = df["images"].apply(
        lambda imgs: "https://www.ab.gr" + imgs[0]["url"]
        if isinstance(imgs, list) and imgs else np.nan
    )

    df["privateLabel"] = df["brand"].isin(PRIVATE_LABELS)
    df['date'] = TODAY
    df['product_id'] = df['supermarket_code'] + df['original_product_code']

    df.rename(columns = {
        "name":"product_name",
        "product_type":"original_product_type_level1",
        "product_subtype_level1":"original_product_type_level2",
        "product_subtype_level2":"original_product_type_level3"
        }, inplace=True)

    df.drop_duplicates(subset="original_product_code", inplace=True)
    return df[
        [
            'product_id',
            "original_product_code",
            "brand",
            "product_name",
            "price",
            "pricePerKilo",
            "unit",
            "product_url",
            "main_image",
            "supermarket_code",
            "privateLabel",
            "date",
            "original_product_type_level1",
            "original_product_type_level2",
            "original_product_type_level3"
        ]
    ]
