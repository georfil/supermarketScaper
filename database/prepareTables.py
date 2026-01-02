import pandas as pd

def prepareTables(df):

    df_products = df[['product_id', 
                      'original_product_code',
                      'product_name',
                      'brand',
                    #   'product_type_code'
                      'supermarket_code',
                      "original_product_type_level1",
                      "original_product_type_level2",
                      "original_product_type_level3",
                      "unit",
                      "product_url",
                      "main_image",
                      "privateLabel",
                      "date"]]
    
    df_products.rename(columns = {"date":"created_at"}, inplace=True)
    return df_products
    