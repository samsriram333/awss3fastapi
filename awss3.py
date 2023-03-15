import boto3
import json
import os

s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAX3ZWXIIEUJXZMR42',
    aws_secret_access_key='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'
)

bucket_name = 'likedcategorybucket'            
response = s3.list_objects_v2(Bucket=bucket_name)

keys = []

if 'Contents' in response:
    for obj in response['Contents']:
        keys.append(obj["Key"])
else:
    print(f'Bucket {bucket_name} is empty.')

results = []

for key in keys:
    response = s3.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read().decode('utf-8')
    try:   
        parsed_data = json.loads(data)
    except json.decoder.JSONDecodeError as e:
    #   print(f"Error decoding JSON for file {key}: {e}")
      continue    
    file_name = os.path.splitext(os.path.basename(key))[0]
    try:
        user_id = int(file_name)
    except ValueError:
        # skip files that don't have a valid user ID in the file name
        continue
    result = {
        "user_id": user_id,
        "category": parsed_data.get('fav_recent10_cat', []) + parsed_data.get('favRecent30_cat', []) + parsed_data.get('All_fav_cat', [])
    }
    results.append(result)

# print(results)            
with open('results.json', 'w') as f:
    json.dump(results, f)            




            