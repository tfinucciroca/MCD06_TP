import pandas as pd
import random
import string

random.seed(4)
active_advertisers = [''.join(random.choices(string.ascii_uppercase + string.digits, k = 20)) for _ in range(20)]
inactive_advertisers = [''.join(random.choices(string.ascii_uppercase + string.digits, k = 20)) for _ in range(5)]
all_advertisers = active_advertisers+inactive_advertisers

pd.DataFrame(active_advertisers, columns=['advertiser_id']).to_csv('advertiser_ids', index=False)

## ACTIVE ADVERTISERS
pd.DataFrame(active_advertisers, columns=['advertiser_id']).to_csv('advertiser_ids', index=False)

advertisers_catalogs = {}
for advertiser in all_advertisers:
    advertisers_catalogs[advertiser] = [''.join(random.choices(string.ascii_lowercase + string.digits, k = 6)) for _ in range(100)]


possible_dates = [f'2024-04-{day:02d}' for day in range(28,31)] + [f'2024-05-{day:02d}' for day in range(1,30)]

## POSSIBLE DATES

product_views = [[advertiser := random.choice(all_advertisers), random.choice(advertisers_catalogs[advertiser]), random.choice(possible_dates)] for _ in range(100_000)]
df_product_views = pd.DataFrame(product_views, columns=['advertiser_id', 'product_id', 'date'])
df_product_views = df_product_views.sort_values('date').reset_index(drop=True)

## PRODUCT VIEWS
df_product_views.to_csv('product_views', index=False)


ads_views = [[advertiser := random.choice(all_advertisers), random.choice(advertisers_catalogs[advertiser]), random.choices(['impression', 'click'], weights=[99, 1])[0], random.choice(possible_dates)] for _ in range(100_000)]
df_ads_views = pd.DataFrame(ads_views, columns=['advertiser_id', 'product_id', 'type', 'date'])
df_ads_views = df_ads_views.sort_values('date').reset_index(drop=True)

## ADS VIEWS
df_ads_views.to_csv('ads_views', index=False)