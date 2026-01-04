import pandas as pd
import requests

from database.db_loadElements import getIDSforDetailsSearch
from database.prepareTables import prepareTablesForDetails
from database.load_to_db import load_details_to_db

from scrapers.ab.ab_product_details import fetchproductDetails as scrapeAB


def scrapeDetailsFromSupermarkets(product_to_scrape):
    data = []
    session = requests.Session()
    for id, original_code, supermarket_code in product_to_scrape:
        if supermarket_code == "ab":
            output = scrapeAB(original_code,session)
        
        if output=="error":
            continue
        data.append({
            **{"id":id},
            **output}
        )

    session.close()
    return data


def main():
    product_to_scrape = getIDSforDetailsSearch()
   
    output = scrapeDetailsFromSupermarkets(product_to_scrape)

    df_images, df_ingredients, df_nutrients = prepareTablesForDetails(output)
    

    load_details_to_db({
        "dbo.productImages":df_images,
        "dbo.nutrients":df_nutrients,
        "dbo.products" : df_ingredients
    })


if __name__ == "__main__":
    main()



