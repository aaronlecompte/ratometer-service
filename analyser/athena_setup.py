import pandas as pd
import os
from dotenv import load_dotenv

from pyathena import connect

load_dotenv()
S3_STAGING_DIR=os.getenv('S3_STAGING_DIR')
REGION_NAME=os.getenv('REGION_NAME')
S3_BUCKET=os.getenv('S3_BUCKET')

cursor = connect(s3_staging_dir=S3_STAGING_DIR,
                 region_name=REGION_NAME).cursor()

cursor.execute(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `ratometer`.`ratdata` (
  `id` string,
  `name` string,
  `address` string,
  `date` string,
  `status` string,
  `lat` float,
  `lng` float,
  `crawl_time` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://{S3_BUCKET}/ratdata/'
TBLPROPERTIES ('has_encrypted_data'='false');
""")