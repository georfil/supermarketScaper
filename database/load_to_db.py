import pandas as pd
from .db_init import initialiseDB


def load_to_db(tables: dict[str, pd.DataFrame]={}, chunk_size=1000):

    cursor, conn = initialiseDB()

    try:
        for table_name, df in tables.items():

            if df.empty:
                print(f"⚠️ Skipping empty table: {table_name}")
                continue
            
            df.to_excel(r"C:\Users\georf\OneDrive - aueb.gr\Desktop\Projects\supermarketScaper\database\files\test.xlsx")
            print(f"📦 Inserting into {table_name} ({len(df)} rows)")

            cols = ", ".join(f"[{c}]" for c in df.columns)
            placeholders = ", ".join(["?"] * len(df.columns))

            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            # NaN → NULL
            data = df.where(pd.notna(df), None).values.tolist()
            for i in range(0, len(data), chunk_size):
                cursor.executemany(sql, data[i:i + chunk_size])

            conn.commit()
            print(f"✅ {table_name} done")


            if table_name == "[staging].[products]":
                transfer_products(cursor, conn)

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


def transfer_products(cursor, conn):
    transfer_sql = """
    INSERT INTO [dbo].[products] (
        product_id,
        original_product_code,
        product_name,
        brand,
        supermarket_code,
        original_product_type_level1,
        original_product_type_level2,
        original_product_type_level3,
        unit,
        product_url,
        main_image,
        privateLabel,
        created_at
    )
    SELECT 
        s.product_id,
        s.original_product_code,
        s.product_name,
        s.brand,
        s.supermarket_code,
        s.original_product_type_level1,
        s.original_product_type_level2,
        s.original_product_type_level3,
        s.unit,
        s.product_url,
        s.main_image,
        s.privateLabel,
        s.created_at
    FROM [staging].[products] s
    WHERE NOT EXISTS (
        SELECT 1 FROM [dbo].[products] p WHERE p.product_id = s.product_id
    );
    """
    cursor.execute(transfer_sql)
    conn.commit()
    print("✅ Transferred new products to dbo.products")

    # 2️⃣ Truncate the staging table to prepare for next run
    truncate_sql = "TRUNCATE TABLE [staging].[products];"
    cursor.execute(truncate_sql)
    conn.commit()
    print("🗑️ Truncated staging.products for next run")

def updated_db(tables: dict[str, pd.DataFrame] = {}, chunk_size=1000):
    cursor, conn = initialiseDB()

    try:
        for table_name, df in tables.items():

            if df.empty:
                print(f"⚠️ Skipping empty table: {table_name}")
                continue

            print(f"📦 Updating {table_name} ({len(df)} rows)")

            # Prepare columns, excluding the primary key
            cols = [c for c in df.columns if c != "product_id"]
            set_clause = ", ".join(f"[{c}] = ?" for c in cols)

            # Build UPDATE statement
            sql_update = f"""
                UPDATE {table_name}
                SET {set_clause}
                WHERE [product_id] = ?
            """

            # Prepare data for update
            data_update = []
            for _, row in df.iterrows():
                # Replace NaN with None
                row_data = [row[c] if pd.notna(row[c]) else None for c in cols]
                row_data.append(row["product_id"])  # product_id for WHERE
                data_update.append(row_data)

            for i in range(0, len(data_update), chunk_size):
                cursor.executemany(sql_update, data_update[i:i + chunk_size])

            conn.commit()
            print(f"✅ {table_name} done")

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()
