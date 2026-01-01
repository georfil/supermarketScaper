# transformers/ab_transformer.py

import pandas as pd
import numpy as np
import datetime as dt

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


def transform(raw_products):
    df = pd.DataFrame(raw_products)

    df["original_code"] = df["code"]
    df["brand"] = df["manufacturerName"]
    df["supermarket"] = "ab"

    df["value"] = df["price"].apply(lambda x: x["value"])
    df["unit"] = df["price"].apply(lambda x: x["unit"])

    df["price_per_kilo"] = df["price"].apply(
        lambda p: float(p["supplementaryPriceLabel1"]
                        .split("€/")[0]
                        .replace(",", "."))
        if p.get("supplementaryPriceLabel1") and "€/" in p["supplementaryPriceLabel1"]
        else np.nan
    )

    df["url"] = "https://www.ab.gr" + df["url"]
    df["image"] = df["images"].apply(
        lambda imgs: "https://www.ab.gr" + imgs[0]["url"]
        if isinstance(imgs, list) and imgs else np.nan
    )

    df["privateLabel"] = df["brand"].isin(PRIVATE_LABELS)
    df['date'] = TODAY

    return df[
        [
            "original_code",
            "brand",
            "name",
            "value",
            "price_per_kilo",
            "unit",
            "url",
            "image",
            "supermarket",
            "privateLabel",
            "date",
        ]
    ]
