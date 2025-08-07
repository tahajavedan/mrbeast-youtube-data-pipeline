import boto3
import io
from io import StringIO
import pandas as pd

bucket = 'taha-aws'  # replace with your actual bucket name
prefix = 'Staging/'
key_tran = 'Transformed/'

# ---------- DATA TRANSFORMATION FUNCTION ----------
def data_trans():
    try:
        s3 = boto3.client('s3')
        data = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        for files in data['Contents']:
            key = files['Key']
            if key.endswith('.csv'):
                read_file = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
                df = pd.read_csv(io.BytesIO(read_file))
                
                if 'channels' in key:
                    df_channels = df
                elif 'video' in key:
                    df_videos = df
                else:
                    print(f"File not matched: {key}")
        
        final_result = df_channels.merge(df_videos, on='Channel_ID', how='inner')
        memory = StringIO()
        final_result.to_csv(memory, index=False)
        memory.seek(0)
        
        s3.put_object(Body=memory.getvalue(), Bucket=bucket, Key=f"{key_tran}MrBeast.csv")
        print(f"File {key_tran[12:]}MrBeast.csv uploaded to S3 at: {key_tran}")

    except Exception as e:
        print(f"Data Transformation Error: {e}")


# ---------- REDSHIFT COPY FUNCTION ----------
redshift_client = boto3.client('redshift-data', region_name='us-east-1')

WorkgroupName = 'my-redshift'
DATABASE = 'dev'
REGION = 'us-east-1'

def run_query(query):
    response = redshift_client.execute_statement(
        WorkgroupName=WorkgroupName,
        Database=DATABASE,
        Sql=query
    )
    print(f"Query submitted: ID = {response['Id']}")
    return response

# ---------- MAIN LAMBDA HANDLER ----------
def lambda_handler(event, context):
    try:
        data_trans()
        # Step 1: Truncate
        resp1 = run_query("TRUNCATE TABLE youtube.mrbeast;")

        # Step 2: Copy
        resp2 = run_query("""
            COPY youtube.mrbeast (
                 channel_id,
                 channel_name,
                 total_views,
                 total_subs,
                 total_videos,
                 country,
                 video_id,
                 title,
                 published_date,
                 video_views,
                 video_likes,
                 total_comments
                            )
                    FROM 's3://taha-aws/Transformed/MrBeast.csv'
                    IAM_ROLE 'arn:aws:iam::323517216656:role/lambda_s3_access_role'
                    FORMAT AS CSV
                    IGNOREHEADER 1; """)

        return {
            'statusCode': 200,
            'body': f" Data copied. Truncate ID: {resp1['Id']}, Copy ID: {resp2['Id']}"
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f" Error: {str(e)}"
        }

        
