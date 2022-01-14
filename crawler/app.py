import requests
import pandas as pd
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
CRAWL_WEBSITE = os.getenv('CRAWL_WEBSITE')
S3_BUCKET = os.getenv('ratometer-internal')
S3_PREFIX = os.getenv('ratdata')

def main():
    print('Hello!')
    
    print('Reading website')
    resp = requests.get(CRAWL_WEBSITE)
    data = resp.json()
    df = pd.DataFrame(data)

    print('Formatting data')
    df['crawl_time'] = datetime.now().isoformat()
    filename = f"ratdata_{datetime.now().strftime('%Y%m%d%H%M')}.json.gz"
    df.to_json(filename, orient='records', lines=True)

    print('Uploading to S3')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET).upload_file(filename, f'{S3_PREFIX}/{filename}')


if __name__ == '__main__':
    main()