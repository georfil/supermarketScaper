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
                      "date",
                      ]].copy()
    
    df_products.rename(columns = {"date":"created_at"}, inplace=True)


    df_prices = df[['product_id',
                    'price',
                    'pricePerKilo',
                    'date']].copy()
    df_prices.rename(columns = {"date":"priceDate"}, inplace=True)
    

                    
    return df_products, df_prices
    

def prepareTablesForDetails(scraped_data):

  products_cols = ["product_id", "ingredients", "searchedDetails"]
  images_cols = ["product_id", "image_url"]
  nutrients_cols = ["product_id", "portion", "energy_kj", "energy_kcal", "fat",
                  "saturated_fat", "carbs", "sugars", "proteins", "salt"]

  products_rows = []
  images_rows = []
  nutrients_rows = []

  for row in scraped_data:
      product_id = row["id"]
      images = row["productImages"]
      ingredients = row["ingredients"]
      nutrients = row["nutrients"]
      
      products_rows.append([product_id, ingredients, True])
    
      for image in (images or []):
          images_rows.append([product_id, image])
      for n in (nutrients or []):
        
          nutrients_rows.append([
              product_id,
              n.get("portion"),
              n.get("energy_kj"),
              n.get("energy_kcal"),
              n.get("fat"),
              n.get("saturated_fat"),
              n.get("carbs"),
              n.get("sugars"),
              n.get("proteins"),
              n.get("salt"),
          ])

  df_products = pd.DataFrame(products_rows, columns=products_cols)
  df_images = pd.DataFrame(images_rows, columns=images_cols)
  df_nutrients = pd.DataFrame(nutrients_rows, columns=nutrients_cols)

  return df_images, df_products, df_nutrients

