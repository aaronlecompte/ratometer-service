import pandas as pd
import boto3
import os
import time
from datetime import datetime
from dotenv import load_dotenv
import simplejson


from pyathena import connect
from pyathena import cursor
from pyathena.cursor import DictCursor

load_dotenv()
S3_BUCKET=os.getenv('S3_BUCKET')
S3_BUCKET_PUBLIC=os.getenv('S3_BUCKET_PUBLIC')
S3_PREFIX=os.getenv('S3_PREFIX')
S3_STAGING_DIR=os.getenv('S3_STAGING_DIR')
REGION_NAME=os.getenv('REGION_NAME')

cursor_default = connect(s3_staging_dir=S3_STAGING_DIR,
                 region_name=REGION_NAME).cursor(DictCursor)


def perform_transforms():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET)

    print('Transitions transformation')
    print('Clearing data folder...')
    bucket.objects.filter(Prefix=f"{S3_PREFIX}/transitions/").delete()
    time.sleep(10)
    print('Executing transformation...')
    cursor_default.execute('DROP TABLE IF EXISTS ratometer.transitions')
    cursor_default.execute(f"""
        CREATE TABLE IF NOT EXISTS ratometer.transitions
        WITH (format='parquet', external_location='s3://{S3_BUCKET}/{S3_PREFIX}/transitions/') AS
        SELECT 
        address,
        date,
        status,
        LAG(status) OVER (PARTITION BY address ORDER BY date) as prev_status,
        LAG(date) OVER (PARTITION BY address ORDER BY date) as prev_date
        FROM ratometer.ratdata
    """)


    print('Location status transformation')
    print('Clearing data folder...')
    bucket.objects.filter(Prefix=f"{S3_PREFIX}/location-status/").delete()
    time.sleep(10)
    print('Executing transformation...')
    cursor_default.execute('DROP TABLE IF EXISTS ratometer.location_status')
    cursor_default.execute(f"""
        CREATE TABLE IF NOT EXISTS ratometer.location_status
        WITH (format='parquet', external_location='s3://{S3_BUCKET}/{S3_PREFIX}/location-status/') AS
        SELECT * FROM (
        WITH out_of_stock_dates AS (
        SELECT
        address,
        MAX(date) as latest_out_of_stock_date
        FROM ratometer.transitions
        WHERE status = 'NO_STOCK'
        and prev_status IN ('IN_STOCK', 'LOW_STOCK')
        AND from_iso8601_timestamp(date) >= CURRENT_TIMESTAMP - INTERVAL '72' hour
        AND from_iso8601_timestamp(date) < CURRENT_TIMESTAMP
        GROUP BY 1),

        restock_dates AS (
        SELECT
        address,
        MAX(date) as latest_restock_date
        FROM ratometer.transitions
        WHERE status IN ('IN_STOCK', 'LOW_STOCK')
        and prev_status = 'NO_STOCK'
        AND from_iso8601_timestamp(date) >= CURRENT_TIMESTAMP - INTERVAL '72' hour
        AND from_iso8601_timestamp(date) < CURRENT_TIMESTAMP
        GROUP BY 1),

        latest_status AS (SELECT * FROM (
        SELECT 
        address,
        lat,
        lng,
        status,
        ROW_NUMBER() OVER (PARTITION BY address ORDER BY date DESC) as rn
        FROM ratometer.ratdata
        WHERE from_iso8601_timestamp(date) >= CURRENT_TIMESTAMP - INTERVAL '72' hour
        AND from_iso8601_timestamp(date) < CURRENT_TIMESTAMP
        ) t WHERE t.rn = 1),

        location_data AS (
        SELECT
        A.address,
        A.lat,
        A.lng,
        A.status as current_status,
        CAST(REPLACE(REPLACE(B.latest_restock_date, 'T', ' '), 'Z', '') as timestamp) as latest_in_stock,
        CAST(REPLACE(REPLACE(C.latest_out_of_stock_date, 'T', ' '), 'Z', '') as timestamp) as latest_out_of_stock,
        CASE 
        WHEN ( 6371 * acos( cos( radians(-33.8688) ) * cos( radians( lat ) ) 
        * cos( radians( lng ) - radians(151.2093) ) + sin( radians(-33.8688) ) * sin(radians(lat)) ) ) < 50 THEN 'Sydney' 

        WHEN ( 6371 * acos( cos( radians(-37.8136) ) * cos( radians( lat ) ) 
        * cos( radians( lng ) - radians(144.9631) ) + sin( radians(-37.8136) ) * sin(radians(lat)) ) ) < 50 THEN 'Melbourne' 

        WHEN ( 6371 * acos( cos( radians(-27.4705) ) * cos( radians( lat ) ) 
        * cos( radians( lng ) - radians(153.0260) ) + sin( radians(-27.4705) ) * sin(radians(lat)) ) ) < 50 THEN 'Brisbane' 

        WHEN ( 6371 * acos( cos( radians(-34.9285) ) * cos( radians( lat ) ) 
        * cos( radians( lng ) - radians(138.6007) ) + sin( radians(-34.9285) ) * sin(radians(lat)) ) ) < 50 THEN 'Adelaide' 

        WHEN ( 6371 * acos( cos( radians(-31.9523) ) * cos( radians( lat ) ) 
        * cos( radians( lng ) - radians(115.8613) ) + sin( radians(-31.9523) ) * sin(radians(lat)) ) ) < 50 THEN 'Perth'

        WHEN ( 6371 * acos( cos( radians(-42.8826) ) * cos( radians( lat ) ) 
        * cos( radians( lng ) - radians(147.3257) ) + sin( radians(-42.8826) ) * sin(radians(lat)) ) ) < 50 THEN 'Hobart' 

        WHEN ( 6371 * acos( cos( radians(-35.2809) ) * cos( radians( lat ) ) 
        * cos( radians( lng ) - radians(149.1300) ) + sin( radians(-35.2809) ) * sin(radians(lat)) ) ) < 50 THEN 'Canberra' 

        ELSE 'Other'
        END as geo_city,
        CASE 
        WHEN A.address LIKE '% NSW %' THEN 'NSW'
        WHEN A.address LIKE '% VIC %' THEN 'VIC'
        WHEN A.address LIKE '% QLD %' THEN 'QLD'
        WHEN A.address LIKE '% WA %' THEN 'WA'
        WHEN A.address LIKE '% SA %' THEN 'SA'
        WHEN A.address LIKE '% TAS %' THEN 'TAS'
        WHEN A.address LIKE '% NT %' THEN 'NT'
        WHEN A.address LIKE '% ACT %' THEN 'ACT'
        ELSE 'Other' END as geo_state
        FROM latest_status A
        LEFT JOIN restock_dates B
        ON A.address = B.address
        LEFT JOIN out_of_stock_dates C
        ON A.address = C.address)
        
        SELECT
        *,
        CASE 
        WHEN current_status = 'NO_STOCK' AND latest_out_of_stock > latest_in_stock THEN 
        DATE_DIFF('minute', latest_in_stock, latest_out_of_stock) END 
        as out_of_stock_time_mins
        FROM location_data)
        """)

def get_summary_data():

    query_time = datetime.now().isoformat()

    print('Executing query')
    results = cursor_default.execute("""
        SELECT
        geo_state,
        geo_city,
        COUNT(*) as num_addresses,
        SUM(CASE WHEN current_status IN ('IN_STOCK', 'LOW_STOCK') THEN 1 ELSE 0 END) as num_in_stock,
        100.0*SUM(CASE WHEN current_status IN ('IN_STOCK', 'LOW_STOCK') THEN 1 ELSE 0 END) / COUNT(*) as percent_in_stock,
        100.0*SUM(CASE WHEN DATE_DIFF('hour', latest_in_stock, CURRENT_TIMESTAMP) < 24 OR current_status IN ('IN_STOCK', 'LOW_STOCK') THEN 1 ELSE 0 END) / COUNT(*) as percent_had_stock_last_24h,
        APPROX_PERCENTILE(out_of_stock_time_mins, 0.5) as out_of_stock_time_mins_median,
        APPROX_PERCENTILE(out_of_stock_time_mins, 0.25) as out_of_stock_time_mins_prc25,
        APPROX_PERCENTILE(out_of_stock_time_mins, 0.75) as out_of_stock_time_mins_prc75,
        COUNT(out_of_stock_time_mins) as num_out_of_stock_time_samples
        FROM ratometer.location_status
        WHERE geo_state <> 'Other'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)

    
    print('Formatting results')
    df = pd.DataFrame(results)
    df['query_time'] = query_time

    summary = df.to_dict(orient='records')
    
    results_hours = cursor_default.execute("""
        SELECT 
        HOUR(from_iso8601_timestamp(date) at time zone 'Australia/Melbourne') as date_hour,
        COUNT(*) as num_events
        FROM ratometer.transitions 
        WHERE from_iso8601_timestamp(date) >= CURRENT_TIMESTAMP - INTERVAL '72' hour
        AND from_iso8601_timestamp(date) < CURRENT_TIMESTAMP
        AND status = 'IN_STOCK'
        and prev_status = 'NO_STOCK'
        GROUP BY 1
        ORDER BY 1
    """)
    
    df_stock_hours = pd.DataFrame(results_hours)
    df_stock_hours['query_time'] = query_time

    stock_hours = df_stock_hours.to_dict(orient='records')

    print('Uploading to S3')
    filename_output = f"results_v2_{datetime.now().strftime('%Y%m%d%H00')}.json"
    with open(filename_output, 'w') as fp:
        simplejson.dump({
            'summary': summary,
            'stock_hours': stock_hours
        }, fp, ignore_nan=True)

    s3 = boto3.resource('s3')
    s3.Bucket(S3_BUCKET_PUBLIC).upload_file(filename_output, f'data/{filename_output}')
    s3.Bucket(S3_BUCKET_PUBLIC).upload_file(filename_output, f'data/latest_v2.json')

if __name__ == '__main__':
    perform_transforms()
    get_summary_data()