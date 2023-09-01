import os
import json
from configparser import ConfigParser
import boto3
import pandas as pd
import io

# Automation call
# from . import  constant as CONST

# Single call
import constant as CONST


def initTranscriptEngine():
    config = ConfigParser(interpolation=None)
    config.read(os.getcwd()+'/config.ini')

    url = CONST.TRANSCRIBE_URL
    bucket_name = CONST.S3_BUCKET_IFNO['bucket_name']
    transcript_input_path = config['transcribe']['transcript_input_path']
    transcript_output_path = config['transcribe']['transcript_output_path']
    processed_dir = config['transcribe']['processed_dir']

    rtn = {"transcribe_url": url, "transcribe_input_path": transcript_input_path,
           "transcribe_output_path": transcript_output_path, "transcribe_processed": processed_dir, "bucket_name": bucket_name}
    return rtn


def initCombineTranscript():
    config = ConfigParser(interpolation=None)
    config.read(os.getcwd()+'/config.ini')
    bucket_name = CONST.S3_BUCKET_IFNO['bucket_name']
    input_path = config['combinetranscribe']['input_folder']
    output_path = config['combinetranscribe']['input_folder']
    ext = config['combinetranscribe']['file_ext']

    rtn = {"input_path": input_path, "output_path": output_path,
           "ext": ext, "bucket_name": bucket_name}
    return rtn


def initGetIdeas():
    config = ConfigParser(interpolation=None)
    config.read(os.getcwd()+'/config.ini')

    bucket_name = CONST.S3_BUCKET_IFNO['bucket_name']
    GET_IDEA_URL = CONST.GET_IDEA_URL
    get_idea_output_path = config['getideas']['get_idea_output_path']
    get_idea__input_path = config['getideas']['get_idea_input_path']

    rtn = {"get_idea_url": GET_IDEA_URL, "get_idea_input_path": get_idea__input_path,
           "get_idea_output_path": get_idea_output_path, "bucket_name": bucket_name}
    return rtn


def initGetAnswer():
    config = ConfigParser(interpolation=None)
    config.read(os.getcwd()+'/config.ini')
    bucket_name = CONST.S3_BUCKET_IFNO['bucket_name']
    question_db = config['question_db']['qdb']
    raw_url = CONST.GET_RAW_URL
    refined_url = CONST.GET_REFINED_URL
    input_path = config['raw_ans']['raw_ans_input_path']
    raw_ans_output_path = config['raw_ans']['raw_ans_output_path']
    output_path = config['refined_ans']['refined_output_path']
    rtn = {"raw_url": raw_url, "refined_url": refined_url, "input_path": input_path, "raw_ans_output_path": raw_ans_output_path,
           "output_path": output_path, "question_db": question_db, "bucket_name": bucket_name}
    return rtn


def initFlattening():
    config = ConfigParser(interpolation=None)
    config.read(os.getcwd()+'/config.ini')
    bucket_name = CONST.S3_BUCKET_IFNO['bucket_name']
    input_path = config['flattening']['input_folder_flattening']
    output_path = config['flattening']['output_folder_flattening']
    ext = config['flattening']['ext']
    rtn = {"bucket_name": bucket_name, "ext": ext,
           "input_path": input_path, "output_path": output_path}
    return rtn


def initMapping():
    config = ConfigParser(interpolation=None)
    config.read(os.getcwd()+'/config.ini')
    bucket_name = CONST.S3_BUCKET_IFNO['bucket_name']
    input_path = config['mapping']['input_folder_mapping']
    output_path = config['mapping']['output_folder_mapping']
    feature_list = config['mapping']['feature_list']
    ext = config['mapping']['ext']
    rtn = {"bucket_name": bucket_name, "ext": ext,
           "input_path": input_path, "output_path": output_path, "feature_list": feature_list}
    return rtn


def initUniqueFileCount():
    config = ConfigParser(interpolation=None)
    config.read(os.getcwd()+'/config.ini')
    bucket_name = config['s3details']['bucket_name']
    folder_lot = config['unique_file_counting']['folder_prefix_lot']
    return bucket_name, folder_lot


def s3_access():
    S3_BUCKET_IFNO = CONST.S3_BUCKET_IFNO
    return boto3.resource(
        service_name=S3_BUCKET_IFNO['service_name'],
        region_name=S3_BUCKET_IFNO['region_name'],
        aws_access_key_id=S3_BUCKET_IFNO['aws_access_key_id'],
        aws_secret_access_key=S3_BUCKET_IFNO['aws_secret_access_key']
    )


def s3_Client():
    S3_BUCKET_IFNO = CONST.S3_BUCKET_IFNO
    return boto3.client(
        service_name=S3_BUCKET_IFNO['service_name'],
        region_name=S3_BUCKET_IFNO['region_name'],
        aws_access_key_id=S3_BUCKET_IFNO['aws_access_key_id'],
        aws_secret_access_key=S3_BUCKET_IFNO['aws_secret_access_key']
    )


def s3FileListing(bucket_name, folder_prefix, ext):
    S3 = s3_access()
    s3_bucket = S3.Bucket(bucket_name)
    fileList = []
    ext = f".{ext}"
    for my_bucket_object in s3_bucket.objects.filter(Prefix=folder_prefix):
        fobj = my_bucket_object.key
        if fobj != folder_prefix:
            if fobj.endswith(ext):
                fileList.append(fobj)
    return fileList


def storeResponse(bucket_name, prefix, resp):
    S3 = s3_access()
    rtn = True
    try:
        object = S3.Object(bucket_name, prefix)
        object.put(Body=json.dumps(resp))
    except Exception as e:
        print(e)
        rtn = False
    finally:
        return rtn


def put_content_s3(bucket_name, file_key, file_content):
    s3_client = s3_Client()
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=file_content)


def move_s3_object(bucket_name, source_key, destination_key):
    s3 = s3_Client()
    s3.copy_object(Bucket=bucket_name,
                   CopySource=f'{bucket_name}/{source_key}', Key=destination_key)
    s3.delete_object(Bucket=bucket_name, Key=source_key)


def json_to_csv(json_data):
    csv_data = []
    df = pd.DataFrame(json_data)
    csv_data = df.to_csv(index=False)
    return csv_data


def read_csv_from_s3(bucket_name, csv_file_key):
    s3 = s3_Client()
    response = s3.get_object(Bucket=bucket_name, Key=csv_file_key)
    csv_data = response['Body'].read()
    df = pd.read_csv(io.BytesIO(csv_data))
    json_data = df.to_json(orient='records')

    print(df)
    print(json_data)


def read_json_from_s3(bucket_name, json_file_key):
    s3 = s3_Client()
    response = s3.get_object(Bucket=bucket_name, Key=json_file_key)
    file_content = response['Body'].read()
    return json.loads(file_content.decode('utf-8'))


def json2txt():
    allfilepath = s3FileListing(
        "ixolerator-cloud", "MeghnadAutomation/Supro_mini/Owner/Lot 1/Transcript/json", 'json')
    for path in allfilepath:
        ext = path.split(".")[-1].lower()
        if ext == "json":
            trn_json = read_json_from_s3("ixolerator-cloud", path)
            print(path)
            file_content = trn_json['transcribed_text']
            file_content = json.dumps(file_content)
            filename = path.split("/")[-1]
            filename = filename.split(".json")[0].strip()+".txt"
            file_key = f"MeghnadAutomation/Supro_mini/Owner/Lot 1/Transcript/txt/{filename}"
            put_content_s3("ixolerator-cloud", file_key, file_content)


def dwonload_s3_to_local():
    bucket_name = "ixolerator-cloud"
    source_path = "MeghnadAutomation/Supro_mini/Owner/Lot 1/Transcript/txt/"
    source_path = "BoleroFuelTankProject/sampleFiles/Transcript/"
    file_ext = "xlsx"
    local_directory = r'C:\Users\Manoranjan\Downloads\BolerFuelTank'

    s3 = s3_Client()
    allfilepath = s3FileListing(bucket_name, source_path, file_ext)
    for path in allfilepath:
        ext = (path.split("/")[-1]).split(".")[-1].lower()
        if ext == file_ext:
            filename = path.split("/")[-1]
            local_file_path = os.path.join(local_directory, filename)
            s3.download_file(bucket_name, path, local_file_path)


def df_to_excel_s3_upload(data, bucket_name, s3_key):
    s3 = s3_Client()
    df = pd.DataFrame(data)
    excel_buffer = io.BytesIO()
    sheet_name = 'Sheet1'
    df.to_excel(excel_buffer, index=False, sheet_name=sheet_name)
    excel_buffer.seek(0)
    s3.upload_fileobj(excel_buffer, bucket_name, s3_key)

def is_object_a_file(bucket_name, object_key, s3_client):
    response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    if 'ContentLength' in response:
        return True  # Object is a file
    else:
        return False  # Object is not a file (possibly a directory-like key)