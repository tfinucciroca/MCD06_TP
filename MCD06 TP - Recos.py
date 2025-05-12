import pandas as pd
from datetime import datetime, timedelta

base_path = 'gs://adtech-tp-2025/raw/'
# importamos los archivos generados
advertiser_ids_df = pd.read_csv(base_path + 'advertiser_ids.csv')
product_views_df = pd.read_csv(base_path + 'product_views.csv')
ads_views_df = pd.read_csv(base_path + 'ads_views.csv')

# Reco 1 - Top Product:
# Un primer modelo muy simple es recomendar los productos más visitados en la web del advertiser.

product_views_df['date'] = pd.to_datetime(product_views_df['date'])

start_date = datetime(2025, 5, 12)
end_date = datetime(2025, 5, 31)
date_range = pd.date_range(start=start_date, end=end_date)
#print(date_range)

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
                'product_reco': top_product
            })

reco1_df = pd.DataFrame(reco1_list)


# Reco 2 - Top CTR:
# Un segundo modelo consta de recomendar los productos que mejor performance tienen en los ads.
# Medimos como performance el Click-Through-Rate que indica cuántos clicks se hacen por cada impression para un producto determinado.

ads_views_df['date'] = pd.to_datetime(ads_views_df['date'])

start_date = datetime(2025, 5, 12)
end_date = datetime(2025, 5, 31)
date_range = pd.date_range(start=start_date, end=end_date)

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
                'product_reco': top_product
            })

reco2_df = pd.DataFrame(reco2_list)