import boto3
import uvicorn
import json
import os
from fastapi import FastAPI, Request

app = FastAPI()

s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAX3ZWXIIEUJXZMR42',
    aws_secret_access_key='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'
)

bucket_name = 'likedcategorybucket'            
# response = s3.list_objects_v2(Bucket=bucket_name)

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
        return result
    except Exception as e:
        return {"message": str(e)}
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)    