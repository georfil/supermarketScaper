# run_pipeline.py

import pandas as pd

from scrapers.ab.ab import scrape as scrape_ab
from processing.ab_transformer import transform as transform_ab




def main():
    
    # Scraping
    ab_raw = scrape_ab()
    print("Finished Scarping")

    # Transforming
    ab_df = transform_ab(ab_raw)



    final_df = pd.concat(
        [
            ab_df,
            # skl_df
        ],
        ignore_index=True
    )

   
    # Database
    from database.prepareTables import prepareTables
    from database.load_to_db import load_to_db


    # final_df = final_df.head(100)  <- for testing
    df_products, df_prices = prepareTables(final_df)


    load_to_db(
        {
            "dbo.products":df_products,
            "dbo.productPrices":df_prices
        }
    )



if __name__ == "__main__":
    main()
