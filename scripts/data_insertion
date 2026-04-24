# ============================================================
# MSBA 305 — Data Insertion Script
# Olist E-Commerce PostgreSQL Pipeline
# ============================================================

import os
import pandas as pd
from sqlalchemy import create_engine, text

# ------------------------------------------------------------
# Database connection
# ------------------------------------------------------------

# Use environment variable for security:
# export SUPABASE_DB_URL="postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DATABASE"
SUPABASE_DB_URL = os.getenv(
    "SUPABASE_DB_URL",
    "postgresql+psycopg2://USERNAME:PASSWORD@HOST:PORT/DATABASE"
)

engine = create_engine(SUPABASE_DB_URL)

# ------------------------------------------------------------
# File paths
# ------------------------------------------------------------

DATA_PATH = "../data"

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

def insert_dataframe_to_sql(df, table_name, engine, if_exists="append"):
    try:
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method="multi"
        )
        print(f"Successfully inserted {len(df):,} rows into '{table_name}'.")

        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};")).scalar()
            print(f"Current row count in '{table_name}': {count:,}")

    except Exception as e:
        print(f"Error inserting data into '{table_name}': {e}")


def verify_all_tables(engine):
    tables = [
        "geolocation",
        "product_categories",
        "products",
        "sellers",
        "customers",
        "orders",
        "order_items",
        "order_payments",
        "order_reviews",
        "countries_gdp",
        "holidays"
    ]

    print("=" * 55)
    print("POSTGRESQL DATABASE TABLE ROW COUNTS")
    print("=" * 55)

    with engine.connect() as conn:
        for table in tables:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table};")).scalar()
                print(f"{table:<25} {count:>10,} rows")
            except Exception as e:
                print(f"{table:<25} ERROR: {e}")

    print("=" * 55)


# ------------------------------------------------------------
# Main loading logic
# ------------------------------------------------------------

def main():
    print("Starting PostgreSQL data insertion pipeline...")

    # Load datasets
    ols_customers = pd.read_csv(f"{DATA_PATH}/olist_customers_dataset.csv")
    ols_geo = pd.read_csv(f"{DATA_PATH}/olist_geolocation_dataset.csv")
    ols_order_items = pd.read_csv(f"{DATA_PATH}/olist_order_items_dataset.csv")
    ols_order_payments = pd.read_csv(f"{DATA_PATH}/olist_order_payments_dataset.csv")
    ols_order_reviews = pd.read_csv(f"{DATA_PATH}/olist_order_reviews_dataset.csv")
    ols_orders = pd.read_csv(f"{DATA_PATH}/olist_orders_dataset.csv")
    ols_products = pd.read_csv(f"{DATA_PATH}/olist_products_dataset.csv")
    ols_sellers = pd.read_csv(f"{DATA_PATH}/olist_sellers_dataset.csv")
    ols_product_category = pd.read_csv(f"{DATA_PATH}/product_category_name_translation.csv")

    # --------------------------------------------------------
    # 1. Geolocation
    # --------------------------------------------------------

    all_zip_codes = pd.concat([
        ols_geo["geolocation_zip_code_prefix"],
        ols_customers["customer_zip_code_prefix"],
        ols_sellers["seller_zip_code_prefix"]
    ]).dropna().unique()

    geolocation_df = (
        ols_geo[ols_geo["geolocation_zip_code_prefix"].isin(all_zip_codes)]
        .drop_duplicates(subset=["geolocation_zip_code_prefix"])
        .copy()
    )

    missing_geo_zips = pd.DataFrame(
        [z for z in all_zip_codes if z not in geolocation_df["geolocation_zip_code_prefix"].values],
        columns=["geolocation_zip_code_prefix"]
    )

    if not missing_geo_zips.empty:
        missing_geo_zips["geolocation_lat"] = 0.0
        missing_geo_zips["geolocation_lng"] = 0.0
        missing_geo_zips["geolocation_city"] = "unknown"
        missing_geo_zips["geolocation_state"] = "XX"
        geolocation_df = pd.concat([geolocation_df, missing_geo_zips], ignore_index=True)

    geolocation_df = geolocation_df[
        [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        ]
    ]

    insert_dataframe_to_sql(geolocation_df, "geolocation", engine)

    # --------------------------------------------------------
    # 2. Product Categories
    # --------------------------------------------------------

    product_categories_df = (
        ols_products[["product_category_name"]]
        .dropna()
        .drop_duplicates()
        .merge(
            ols_product_category,
            on="product_category_name",
            how="left"
        )
    )

    product_categories_df["product_category_name_english"] = product_categories_df[
        "product_category_name_english"
    ].fillna(product_categories_df["product_category_name"])

    insert_dataframe_to_sql(product_categories_df, "product_categories", engine)

    # --------------------------------------------------------
    # 3. Products
    # --------------------------------------------------------

    products_df = ols_products.copy()

    product_numeric_cols = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]

    for col in product_numeric_cols:
        products_df[col] = products_df[col].fillna(0).astype(int)

    products_df = products_df[
        products_df["product_category_name"].isin(product_categories_df["product_category_name"])
    ].copy()

    products_df = products_df.drop_duplicates(subset=["product_id"])

    products_df = products_df[
        [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        ]
    ]

    insert_dataframe_to_sql(products_df, "products", engine)

    # --------------------------------------------------------
    # 4. Sellers
    # --------------------------------------------------------

    sellers_df = ols_sellers[
        [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        ]
    ].copy()

    insert_dataframe_to_sql(sellers_df, "sellers", engine)

    # --------------------------------------------------------
    # 5. Customers
    # --------------------------------------------------------

    customers_df = ols_customers[
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        ]
    ].copy()

    customers_df = customers_df.drop_duplicates(subset=["customer_id"])

    insert_dataframe_to_sql(customers_df, "customers", engine)

    # --------------------------------------------------------
    # 6. Orders
    # --------------------------------------------------------

    order_items_agg = ols_order_items.groupby("order_id", as_index=False).agg(
        total_price=("price", "sum"),
        total_freight=("freight_value", "sum")
    )

    order_reviews_agg = ols_order_reviews.groupby("order_id", as_index=False).agg(
        has_review=("review_score", lambda x: x.notna().any())
    )

    orders_df = (
        ols_orders
        .merge(order_items_agg, on="order_id", how="left")
        .merge(order_reviews_agg, on="order_id", how="left")
    )

    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for col in date_cols:
        orders_df[col] = pd.to_datetime(orders_df[col], errors="coerce")

    orders_df["is_delivered"] = orders_df["order_status"].eq("delivered")
    orders_df["delivery_days"] = (
        orders_df["order_delivered_customer_date"] -
        orders_df["order_purchase_timestamp"]
    ).dt.days

    orders_df["delivery_delay"] = (
        orders_df["order_delivered_customer_date"] -
        orders_df["order_estimated_delivery_date"]
    ).dt.days

    orders_df["order_year"] = orders_df["order_purchase_timestamp"].dt.year
    orders_df["order_month"] = orders_df["order_purchase_timestamp"].dt.month_name()
    orders_df["total_order_value"] = (
        orders_df["total_price"].fillna(0) +
        orders_df["total_freight"].fillna(0)
    ).round(2)

    p99 = orders_df["total_order_value"].quantile(0.99)
    orders_df["is_high_value"] = orders_df["total_order_value"] > p99
    orders_df["is_valid_delivery"] = (
        orders_df["delivery_days"].isna() |
        ((orders_df["delivery_days"] >= 0) & (orders_df["delivery_days"] <= 180))
    )
    orders_df["delivery_data_complete"] = orders_df["order_delivered_customer_date"].notna()
    orders_df["has_review"] = orders_df["has_review"].fillna(False)

    with engine.connect() as conn:
        valid_customers = pd.read_sql("SELECT customer_id FROM customers;", conn)

    orders_df = orders_df[
        orders_df["customer_id"].isin(valid_customers["customer_id"])
    ].copy()

    orders_df = orders_df.drop_duplicates(subset=["order_id"])

    orders_df = orders_df[
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "is_delivered",
            "delivery_days",
            "delivery_delay",
            "order_year",
            "order_month",
            "total_order_value",
            "is_high_value",
            "is_valid_delivery",
            "delivery_data_complete",
            "has_review"
        ]
    ]

    insert_dataframe_to_sql(orders_df, "orders", engine)

    # --------------------------------------------------------
    # 7. Order Items
    # --------------------------------------------------------

    with engine.connect() as conn:
        valid_orders = pd.read_sql("SELECT order_id FROM orders;", conn)
        valid_products = pd.read_sql("SELECT product_id FROM products;", conn)
        valid_sellers = pd.read_sql("SELECT seller_id FROM sellers;", conn)

    order_items_df = ols_order_items.copy()

    order_items_df = order_items_df[
        order_items_df["order_id"].isin(valid_orders["order_id"]) &
        order_items_df["product_id"].isin(valid_products["product_id"]) &
        order_items_df["seller_id"].isin(valid_sellers["seller_id"])
    ].copy()

    order_items_df["shipping_limit_date"] = pd.to_datetime(
        order_items_df["shipping_limit_date"],
        errors="coerce"
    )

    order_items_df = order_items_df[
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value"
        ]
    ]

    insert_dataframe_to_sql(order_items_df, "order_items", engine)

    # --------------------------------------------------------
    # 8. Order Payments
    # --------------------------------------------------------

    order_payments_df = ols_order_payments[
        ols_order_payments["order_id"].isin(valid_orders["order_id"])
    ].copy()

    order_payments_df = order_payments_df[
        [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        ]
    ]

    insert_dataframe_to_sql(order_payments_df, "order_payments", engine)

    # --------------------------------------------------------
    # 9. Order Reviews
    # --------------------------------------------------------

    order_reviews_df = ols_order_reviews[
        ols_order_reviews["order_id"].isin(valid_orders["order_id"])
    ].copy()

    order_reviews_df["review_creation_date"] = pd.to_datetime(
        order_reviews_df["review_creation_date"],
        errors="coerce"
    )
    order_reviews_df["review_answer_timestamp"] = pd.to_datetime(
        order_reviews_df["review_answer_timestamp"],
        errors="coerce"
    )

    order_reviews_df = order_reviews_df[
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp"
        ]
    ]

    insert_dataframe_to_sql(order_reviews_df, "order_reviews", engine)

    # --------------------------------------------------------
    # 10. Countries GDP
    # --------------------------------------------------------

    # Expected file: countries_gdp.csv with columns:
    # year, gdp_per_capita_usd
    try:
        countries_gdp_df = pd.read_csv(f"{DATA_PATH}/countries_gdp.csv")
        countries_gdp_df["year"] = countries_gdp_df["year"].astype(int)
        countries_gdp_df = countries_gdp_df.drop_duplicates(subset=["year"])
        insert_dataframe_to_sql(countries_gdp_df, "countries_gdp", engine)
    except FileNotFoundError:
        print("countries_gdp.csv not found. Skipping countries_gdp insertion.")

    # --------------------------------------------------------
    # 11. Holidays
    # --------------------------------------------------------

    # Expected file: holidays.csv with columns:
    # country_or_region, holiday_name, holiday_date
    try:
        holidays_df = pd.read_csv(f"{DATA_PATH}/holidays.csv")
        holidays_df["holiday_date"] = pd.to_datetime(
            holidays_df["holiday_date"],
            errors="coerce"
        ).dt.date

        holidays_df = holidays_df[
            [
                "country_or_region",
                "holiday_name",
                "holiday_date"
            ]
        ]

        insert_dataframe_to_sql(holidays_df, "holidays", engine)
    except FileNotFoundError:
        print("holidays.csv not found. Skipping holidays insertion.")

    verify_all_tables(engine)
    print("Data insertion pipeline completed.")


if __name__ == "__main__":
    main()
