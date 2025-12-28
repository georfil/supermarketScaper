# run_pipeline.py

import pandas as pd

from scrapers.ab.ab import scrape as scrape_ab
from processing.ab_transformer import transform as transform_ab


def main():

    ab_raw = scrape_ab()
    ab_df = transform_ab(ab_raw)

    final_df = pd.concat(
        [
            ab_df,
            # skl_df
        ],
        ignore_index=True
    )

    final_df.to_csv("output/products.csv", index=False)


if __name__ == "__main__":
    main()
