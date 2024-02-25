import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from datetime import datetime, timedelta

s3 = boto3.client('s3')
client = boto3.client('logs')

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "amazonsales-capstone-sk"
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Check if the file is a CSV
    if object_key.endswith('.csv'):
        try:
            # Get the object from S3
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            csv_data = response['Body'].read().decode('utf-8')
            
            # Read CSV data into a DataFrame
            df = pd.read_csv(io.StringIO(csv_data))
            
            # Verify the presence of all required data columns
            required_columns = ['product_id', 'product_name', 'category', 'discounted_price','actual_price', 'discount_percentage', 'rating', 'rating_count','about_product', 'user_id', 'user_name', 'review_id', 'review_title','review_content', 'img_link', 'product_link']
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                # Move the file to the error folder if any required column is missing
                error_object_key = 'errorfiles/' + object_key
                s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                return
            
            # Clean the price columns and remove junk and special characters
            df['discounted_price'] = df['discounted_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            df['actual_price'] = df['actual_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            
            # Convert DataFrame to Parquet format
            table = pa.Table.from_pandas(df)
            parquet_file = io.BytesIO()
            pq.write_table(table, parquet_file)
            parquet_file.seek(0)
            
            # Upload the Parquet file to the cleaned files folder
            cleaned_object_key = 'cleanedfiles/' + object_key.split('/')[-1].replace('.csv', '.parquet')
            s3.put_object(Bucket=bucket_name, Key=cleaned_object_key, Body=parquet_file)
            
            # Trigger the Glue job
            trigger_glue_job(bucket_name, cleaned_object_key)
            
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            # Move the file to the error folder if an error occurs
            error_object_key = 'errorfiles/' + object_key
            s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
            s3.delete_object(Bucket=bucket_name, Key=object_key)
    else:
        # Move the file to the error folder if it's not a CSV
        error_object_key = 'errorfiles/' + object_key
        s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        
    # Export CloudWatch Logs to S3
    query = """fields @timestamp, @message | sort @timestamp desc | LIMIT 1"""
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
    s3_key = "cloudwatchlogs/{}.txt".format(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
    
    start_query_response = client.start_query(
        logGroupName=log_group,
        startTime=int((datetime.today() - timedelta(hours=24)).timestamp()),
        endTime=int(datetime.now().timestamp()),
        queryString=query,
    )

    query_id = start_query_response['queryId']

    response = None

    while response is None or response['status'] == 'Running':
        print('Waiting for query to complete ...')
        time.sleep(1)
        response = client.get_query_results(queryId=query_id)

    log_data = ""
    for result in response['results']:
        for field in result:
            log_data += f"{field['field']}={field['value']}\n"

    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=log_data)

    return {
        'statusCode': 200,
        'body': "Success"
    }

def trigger_glue_job(bucket_name, object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started:", response)
        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")
    else:
        print("File is not in the cleanedfiles folder or not a parquet")
