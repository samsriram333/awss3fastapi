import boto3
import uvicorn
import json
import os
import logging
from fastapi import FastAPI

logger = logging.getLogger(__name__)

app = FastAPI()

s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAX3ZWXIIEUJXZMR42',
    aws_secret_access_key='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'
)

bucket_name = 'likedcategorybucket'     

logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)
handler = logging.FileHandler('app.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

@app.get("/")
def get_all_users():
    response = s3.list_objects_v2(Bucket=bucket_name)
    keys = []
    if 'Contents' in response:
        for obj in response['Contents']:
            keys.append(obj["Key"])
    else:
        logger.info(f'Bucket {bucket_name} is empty.')
        return []

    results = []

    for key in keys:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        data = response['Body'].read().decode('utf-8')
        try:
            parsed_data = json.loads(data)
        except json.decoder.JSONDecodeError as e:
            logger.warning(f"Error decoding JSON for file {key}: {e}")
            continue
        file_name = os.path.splitext(os.path.basename(key))[0]
        try:
            user_id = int(file_name)
        except ValueError:
            # skip files that don't have a valid user ID in the file name
            logger.warning(f"Invalid user ID in file name: {file_name}")
            continue
        result = {
            "user_id": user_id,
            "category": parsed_data.get('fav_recent10_cat', []) + parsed_data.get('favRecent30_cat', []) + parsed_data.get('All_fav_cat', [])
        }
        results.append(result)
        logger.info(f"Processed user ID {user_id}")

    # write results to file
    with open('results.json', 'w') as f:
        json.dump(results, f)

    return results



@app.get("/users/{user_id}")
async def get_userdetails(user_id:int):
# async def get_user_categories(request: Request):
    # user_id = await request.json()
    key = f'individual_user_data/{user_id}.json'
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        data = response['Body'].read().decode('utf-8')
        parsed_data = json.loads(data)
        file_name = os.path.splitext(os.path.basename(key))[0]
        result = {
            "user_id": int(file_name),
            "fav_category": parsed_data.get('fav_recent10_cat', []) + parsed_data.get('favRecent30_cat', []) + parsed_data.get('All_fav_cat', [])
        }
        logger.info(f"Successfully retrieved data for user {user_id}")
        return result
    except Exception as e:
        logger.error(f"Failed to retrieve data for user {user_id}: {e}")
        return {"message": str(e)}
        

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
