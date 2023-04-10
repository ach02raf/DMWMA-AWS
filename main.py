#  Copyright (c) 2021.
#  __author__ = "Walid YAICH"
#  __copyright__ = "Copyright (c) 2021., AWS Module"
#  __license__ = "GPL"
#  __maintainer__ = "Yosra ABASSI, Chayma BEN HAHA"
#  __email__ = "abassi.yosra@tek-up.de, Chayma.BENHAHA@tek-up.de, walid.yaich@tek-up.de"
#  __status__ = "Education"
#
#
import logging
import os
from typing import Optional

import boto3
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from pydantic import BaseSettings

# logging configuration
logging.basicConfig(level=logging.INFO)

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World!"}


class File(BaseModel):
    path: str
    file_name: str
    content: Optional[str] = None


@app.post("/file/", status_code=status.HTTP_201_CREATED)
def write_text_file_to_disk(file: File) -> str:
    full_path = file.path + "/" + file.file_name
    try:
        file_handler = open(full_path, 'w')
        file_handler.write(file.content)
        logging.info("The file was stored successfully to : " + full_path)
        file_handler.close()
    except Exception:
        logging.error("Cannot write to disk !", exc_info=True)
        raise HTTPException(status_code=500, detail="Cannot write this file to disk : " + full_path)
    return "The file was stored successfully :" + full_path


@app.get("/file/")
def read_text_file_from_disk(full_path: str) -> str:
    if os.path.isfile(full_path):
        return open(full_path).read()
    else:
        raise HTTPException(status_code=404, detail="File does not exists : " + full_path)


class Settings(BaseSettings):
    """
    Settings are read from environment variable
    before running this program you must set all your environment variable like this :
    export AWS_TEK_UP_ACCESS_KEY_ID=....
    export AWS_TEK_UP_SECRET_ACCESS_KEY=...
    export AWS_TEK_UP_SESSION_TOKEN=...
    """
    aws_tek_up_access_key_id: str  # it will look for env variable named AWS_TEK_UP_ACCESS_KEY_ID
    aws_tek_up_secret_access_key: str
    aws_tek_up_session_token: Optional[str]  # you need this when you're using aws academy account


@app.get("/s3/file/")
def read_text_file_from_s3(role_is_attached: bool, bucket_name: str, file_name: str) -> str:
    try:
        if role_is_attached:
            # Do not need to provide credentials, this is the recommanded way yo access S3
            s3_resource = boto3.resource('s3')
        else:
            # Need to provide credentials, this is useful when developing using your local machine.
            settings = Settings()
            s3_resource = boto3.resource('s3',
                                         aws_access_key_id=settings.aws_tek_up_access_key_id,
                                         aws_secret_access_key=settings.aws_tek_up_secret_access_key,
                                         aws_session_token=settings.aws_tek_up_session_token
                                         )
        s3_object = s3_resource.Object(bucket_name, file_name)
        file_content = s3_object.get()['Body'].read().decode('utf-8')
        logging.info("File read successfully from S3")
        return file_content
    except Exception:
        logging.error("Cannot read from S3 the requested resource !", exc_info=True)
        raise HTTPException(status_code=500,
                            detail="Cannot read from S3 the requested resource !")


@app.post("/s3/file/", status_code=status.HTTP_201_CREATED)
def write_text_file_to_s3(role_is_attached: bool, bucket_name: str, file_name: str, content: str) -> str:
    try:
        if role_is_attached:
            # Do not need to provide credentials, this is the recommended way yo access S3
            s3_resource = boto3.resource('s3')
        else:
            # Need to provide credentials, this is useful when developing using your local machine.
            settings = Settings()
            s3_resource = boto3.resource('s3',
                                         aws_access_key_id=settings.aws_tek_up_access_key_id,
                                         aws_secret_access_key=settings.aws_tek_up_secret_access_key,
                                         aws_session_token=settings.aws_tek_up_session_token,
                                         )
        s3_resource.Object(bucket_name, file_name).put(Body=content)
        logging.info("File uploaded successfully to S3")
        return "File uploaded successfully to S3"
    except Exception:
        logging.error("Cannot upload file to S3 !", exc_info=True)
        raise HTTPException(status_code=500,
                            detail="Cannot upload file to S3 !")