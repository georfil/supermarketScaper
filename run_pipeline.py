# run_pipeline.py

import pandas as pd

from scrapers.ab.ab import scrape as scrape_ab
from processing.ab_transformer import transform as transform_ab
from database.prepareTables import prepareTables
from database.load_to_db import load_to_db



def main():

    ab_raw = scrape_ab()
    ab_df = transform_ab(ab_raw)

    print("Finished Scarping")


    final_df = pd.concat(
        [
            ab_df,
            # skl_df
        ],
        ignore_index=True
    )

   
    df_products = prepareTables(final_df)

    df_products = df_products.head(2).copy()
    load_to_db(
        {
            "dbo.products":df_products
        }
    )



if __name__ == "__main__":
    main()
