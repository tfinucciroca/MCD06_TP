import pandas as pd
from datetime import datetime, timedelta
import logging
import psycopg2
import os

def main():
    base_path = 'gs://adtech-tp-2025/raw/'

    # Leer archivos desde Cloud Storage
    advertiser_ids_df = pd.read_csv(base_path + 'advertiser_ids.csv', storage_options={"token": "cloud"})
    product_views_df = pd.read_csv(base_path + 'product_views.csv', storage_options={"token": "cloud"})
    ads_views_df = pd.read_csv(base_path + 'ads_views.csv', storage_options={"token": "cloud"})

    # Reco 1 - Top Product
    product_views_df['date'] = pd.to_datetime(product_views_df['date'])
    start_date = datetime(2025, 5, 12)
    end_date = datetime(2025, 5, 31)
    date_range = pd.date_range(start=start_date, end=end_date)

    reco1_list = []
    for advertiser_id in advertiser_ids_df['advertiser_id'].unique():
        for date in date_range:
            window_start = date - timedelta(days=7)
            window_end = date - timedelta(days=1)
            filtered_views = product_views_df[
                (product_views_df['advertiser_id'] == advertiser_id) &
                (product_views_df['date'] >= window_start) &
                (product_views_df['date'] <= window_end)
            ]
            if not filtered_views.empty:
                top_product = filtered_views['product_id'].value_counts().idxmax()
                reco1_list.append({
                    'advertiser_id': advertiser_id,
                    'date': date.strftime('%Y-%m-%d'),
                    'product_id': top_product,
                    'Modelo': 'top_product'
                })

    reco1_df = pd.DataFrame(reco1_list)

    # Reco 2 - Top CTR
    ads_views_df['date'] = pd.to_datetime(ads_views_df['date'])
    impressions_df = ads_views_df[ads_views_df['type'] == 'impression']
    clicks_df = ads_views_df[ads_views_df['type'] == 'click']

    reco2_list = []
    for advertiser_id in advertiser_ids_df['advertiser_id'].unique():
        for date in date_range:
            window_start = date - timedelta(days=7)
            window_end = date - timedelta(days=1)

            impressions_window = impressions_df[
                (impressions_df['advertiser_id'] == advertiser_id) &
                (impressions_df['date'] >= window_start) &
                (impressions_df['date'] <= window_end)
            ]
            clicks_window = clicks_df[
                (clicks_df['advertiser_id'] == advertiser_id) &
                (clicks_df['date'] >= window_start) &
                (clicks_df['date'] <= window_end)
            ]

            if not impressions_window.empty:
                impressions_count = impressions_window.groupby('product_id').size()
                clicks_count = clicks_window.groupby('product_id').size()
                ctr_df = pd.DataFrame({'impressions': impressions_count, 'clicks': clicks_count}).fillna(0)
                ctr_df['click_to_impression_ratio'] = ctr_df['clicks'] / ctr_df['impressions']
                top_product = ctr_df['click_to_impression_ratio'].idxmax()
                reco2_list.append({
                    'advertiser_id': advertiser_id,
                    'date': date.strftime('%Y-%m-%d'),
                    'product_id': top_product,
                    'Modelo': 'top_ctr'
                })

    reco2_df = pd.DataFrame(reco2_list)

    # Unificar y cargar en la base
    final_df = pd.concat([reco1_df, reco2_df], ignore_index=True)

    # Conexión a PostgreSQL
    conn = psycopg2.connect(
        host=os.getenv("34.9.49.121"),
        dbname=os.getenv("adtech-tp"),
        user=os.getenv("postgres"),
        password=os.getenv("Carozo"),
        port=5432
    )
    cursor = conn.cursor()

    for _, row in final_df.iterrows():
        cursor.execute("""
            INSERT INTO recommendations (advertiser_id, date, "Modelo", product_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (advertiser_id, date, "Modelo") DO UPDATE
            SET product_id = EXCLUDED.product_id
        """, (row['advertiser_id'], row['date'], row['Modelo'], row['product_id']))

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("✅ Recomendaciones generadas y guardadas en la base de datos.")

# Para correrlo manualmente
if __name__ == "__main__":
    main()
