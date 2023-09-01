import datetime
import time
import json
import requests
from requests.exceptions import ConnectTimeout
from requests.exceptions import ReadTimeout
import pandas as pd
import boto3
import io

S3_BUCKET_IFNO = {
    "service_name": 's3',
    "bucket_name": "ixolerator-cloud",
    "region_name": "ap-south-1",
    "aws_access_key_id": "AKIATWZQTUP5P4QMO43H",
    "aws_secret_access_key": "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"
}


# VTI_SERVER = "http://192.168.255.6:6009"
# VTI_SERVER = "http://13.234.134.87:5009"
VTI_SERVER = "http://demolive.meghnad.inxiteout.ai:5009"
GET_RAW_URL = VTI_SERVER+"/get_qa_raw_answers"
GET_REFINED_URL = VTI_SERVER + "/get_qa_refined_answers"

# S3PATH = "/home/deeplearningcv/s3_bucket/"
S3PATH = "/home/ubuntu/s3_bucket/"
# QDB="/home/deeplearningcv/s3_bucket/question_db/Bolero_Fuel_Tank/Owner_V1/Mahindra_Bolero_question_db_owner_v1.csv"
QDB="/home/ubuntu/s3_bucket/question_db/Bolero_Fuel_Tank/Owner_V1/Mahindra_Bolero_question_db_owner_v1.csv"

# if raw only
# raw_ans_input_path = "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/Transcript/Transcription_combined/"
raw_ans_output_path = "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/RawAns_v1/"

# if refine only
raw_ans_input_path = "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/RawAns_v1/json/"
refine_ans_output_path = "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/RefineAns_v1/"

RAW_PAYLOAD = {"data_parameters":{"data":"","data_type":"txt","mode":"conversation"},"pre_processing_parameters":{"do_punctuation_restoration":0,"non_en_lang":0},"company_brand_info_parameters":{"focal_company_brand_info":{"Mahindra":["Bolero"]},"aliases":{"Bolero":["bulero", "beloy", "bolero","bowlero", "bowler", "bollero"] }},
"question_db_parameters":{"question_db":QDB,"question_db_type":"csv","get_verbatim_answers":1,"high_complexity": 1}}

REFINED_PAYLOAD = {"company_brand_info_parameters":{"focal_company_brand_info":{"Mahindra":["Bolero"]}},"question_db_parameters":{"question_db":QDB,"question_db_type":"csv"},"raw_answer_parameters":{"raw_answer_db_type":"json","raw_answer_db":"","input_raw_answer_column_name":"Raw_Answers"}}

def initGetAnswer():
    bucket_name = S3_BUCKET_IFNO['bucket_name']
    raw_url = GET_RAW_URL
    refined_url = GET_REFINED_URL
    question_db = QDB
    input_path = raw_ans_input_path
    raw_output_path =raw_ans_output_path
    output_path = refine_ans_output_path
    rtn = {"raw_url": raw_url, "refined_url": refined_url, "input_path": input_path, "raw_ans_output_path": raw_output_path, "output_path": output_path, "question_db": question_db, "bucket_name": bucket_name,'raw_refine':'both'}
    return rtn

def s3_access():
    return boto3.resource(
        service_name=S3_BUCKET_IFNO['service_name'],
        region_name=S3_BUCKET_IFNO['region_name'],
        aws_access_key_id=S3_BUCKET_IFNO['aws_access_key_id'],
        aws_secret_access_key=S3_BUCKET_IFNO['aws_secret_access_key']
    )

def s3_Client():
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

def df_to_excel_s3_upload(data, bucket_name, s3_key):
    s3 = s3_Client()
    df = pd.DataFrame(data)
    excel_buffer = io.BytesIO()
    sheet_name = 'Sheet1'
    df.to_excel(excel_buffer, index=False, sheet_name=sheet_name)
    excel_buffer.seek(0)
    s3.upload_fileobj(excel_buffer, bucket_name, s3_key)       

def read_json_from_s3(bucket_name, json_file_key):
    s3 = s3_Client()
    response = s3.get_object(Bucket=bucket_name, Key=json_file_key)
    file_content = response['Body'].read()
    return json.loads(file_content.decode('utf-8'))

def getAnswer(**get_ans_config):
    bucket_name = get_ans_config['bucket_name']
    question_db = get_ans_config['question_db']
    raw_url = get_ans_config['raw_url']
    refined_url = get_ans_config['refined_url']
    inputfiles = get_ans_config['input_path']
    s3_folder_refined = get_ans_config['output_path']
    raw_refine=get_ans_config['raw_refine']
    
    #For raw file_ext = "txt" and refine  file_ext = "json"
    file_ext = "json"
    # allfilepath = s3FileListing(bucket_name, inputfiles, file_ext)
    allfilepath =[
        "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/RawAns_v1/json/Fuel Tank_BS6 19-20_Bolero_K5F46997.json",
        "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/RawAns_v1/json/Fuel Tank_BS6 19-20_Bolero_K5H51712.json",
        "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/RawAns_v1/json/Fuel Tank_BS6 19-20_Bolero_K5J56710.json",
        "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/RawAns_v1/json/Fuel Tank_BS6 19-20_Bolero_K5K59676.json",
        "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/RawAns_v1/json/Fuel Tank_BS6 19-20_Bolero_K5M14389.json"
    ]

    logs = []
    k = 0
    for path in allfilepath:
        k = k+1
        print(path)
        ext = (path.split("/")[-1]).split(".")[-1].lower()
        if ext == file_ext:
            data_path = f"{S3PATH}{path}"
            log = apiCall(question_db, data_path, raw_url,refined_url, s3_folder_refined,raw_refine)
            log['count'] = k
            filename = path.split("/")[-1]
            log["filepath"] = filename
            if log["status"] == "success" and raw_refine in ["refine","both"]:
                json_file = filename.rsplit('.', 1)[0] + '.json'
                prefix_json = f"{s3_folder_refined}json/{json_file}"
                storeResponse(bucket_name, prefix_json,log['response']['refined_answer_result'])
                xlsx_file = filename.rsplit('.', 1)[0] + '.xlsx'
                prefix_xlsx = f"{s3_folder_refined}excel/{xlsx_file}"
                df_to_excel_s3_upload(log['response']['refined_answer_result'], bucket_name, prefix_xlsx)
            logs.append(log)
        # if k == 12:break
    if len(allfilepath) == 0:
        logs.append({"status": '0', "count": "0", "status_code": "0","status_message": "No Files", "ext": "NILL", "filepath": "NILL"})

    print("\n***************GetAnswer Process Result**************")
    df = pd.DataFrame(logs)
    print(df)
    cdt = datetime.datetime.now()
    df_fn = raw_refine+"_log_"+cdt.strftime("%Y_%m_%d_%H%M%S")
    s3_key = f'{raw_ans_output_path}{df_fn}.xlsx'
    df_to_excel_s3_upload(df, bucket_name, s3_key)
    print("\n*************************************************")  

def getRawAnswers(question_db, data_path, raw_url):
    rtn = {"status": "", "status_code": "", "resp_code": "","status_message": "", "response": {}}
    try:
        compParam = RAW_PAYLOAD
        compParam["data_parameters"]["data"] = data_path
        compParam["question_db_parameters"]["question_db"] = question_db
        url = raw_url
        payload = json.dumps(compParam)
        headers = {'Content-Type': 'application/json'}
        response = requests.request("POST", url, headers=headers, data=payload, timeout=(10, 300))
        status = response.status_code
        if status == 200:
            respObj = response.json()
            if respObj["status_code"] == 0:
                filename = data_path.split("/")[-1]
                json_file = filename.rsplit('.', 1)[0] + '.json'
                prefix_json = f"{raw_ans_output_path}json/{json_file}"
                storeResponse("ixolerator-cloud", prefix_json, respObj['raw_answer_result'])
                xlsx_file = filename.rsplit('.', 1)[0] + '.xlsx'
                prefix_xlsx = f"{raw_ans_output_path}excel/{xlsx_file}"
                df_to_excel_s3_upload(respObj['raw_answer_result'], "ixolerator-cloud", prefix_xlsx)
                rtn = {"status": "success", "status_code": status,"resp_code": respObj["status_code"], "status_message": "", "response": respObj}
            else:
                rtn = {"status": "Error", "status_code": status, "resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
        else:
            rtn = {"status": "Error", "status_code": status, "resp_code": "","status_message": "get_qa_raw_answers api error", "response": {}}
    except ConnectTimeout as e:
        rtn = {"status": "Error", "status_code": 522, "resp_code": "","status_message": "ConnectTimeout error", "response": {}}
    except ReadTimeout as e:
        rtn = {"status": "Error", "status_code": 522, "resp_code": "","status_message": "ReadTimeout error", "response": {}}
    except Exception as e:
        print(e)
        rtn = {"status": "Error", "status_code": "Exception","resp_code": "", "status_message": "Error", "response": {}}
    finally:
        return rtn

def getRefinedAnswers(question_db, raw_answer_db, refined_url):
    try:
        compParam = REFINED_PAYLOAD
        compParam["question_db_parameters"]["question_db"] = question_db
        compParam["raw_answer_parameters"]["raw_answer_db"] = raw_answer_db
        payload = json.dumps(compParam)
        url = refined_url
        headers = {'Content-Type': 'application/json'}
        response = requests.request("POST", url, headers=headers, data=payload, timeout=(10, 60*3))
        status = response.status_code
        if status == 200:
            respObj = response.json()
            # print(respObj)
            if respObj["status_code"] == 0:
                rtn = {"status": "success", "status_code": status,"resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
            else:
                rtn = {"status": "Error", "status_code": status,"resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
        else:
            rtn = {"status": "Error", "status_code": 522, "resp_code": "","status_message": "get_qa_refined_answers api rrror", "response": {}}
    except ConnectTimeout as e:
        rtn = {"status": "Error", "status_code": 522, "resp_code": "","status_message": "ConnectTimeout", "response": {}}
    except ReadTimeout as e:
        rtn = {"status": "Error", "status_code": 522, "resp_code": "","status_message": "ReadTimeout", "response": {}}
    except Exception as e:
        print(e)
        rtn = {"status": "Error", "status_code": 522, "resp_code": "", "status_message": "Error", "response": {}}
    finally:
        return rtn

def apiCall(question_db, data_path, raw_url, refined_url, s3_folder_refined,raw_refine):
    time.sleep(30)
    opt=raw_refine.lower()
    if opt=='raw':
        rtn = getRawAnswers(question_db, data_path, raw_url)
        rtn['api'] = "Raw"
        return rtn
    elif opt=='refine':
        key=data_path.split(f"{S3PATH}")[-1]
        raw_answer_data = read_json_from_s3("ixolerator-cloud", key)
        rtn = getRefinedAnswers(question_db, raw_answer_data, refined_url)
        rtn['api'] = "Refined"
        return rtn
    else:
        rtn = getRawAnswers(question_db, data_path, raw_url)
        rtn['api'] = "Raw"
        if rtn['status'] == 'success':
            raw_answer_data = rtn['response']['raw_answer_result']
            rtn = getRefinedAnswers(question_db, raw_answer_data, refined_url)
            rtn['api'] = "Refined"
        return rtn

get_ans_config = initGetAnswer()
# get_ans_config['raw_refine']='raw'
get_ans_config['raw_refine']='refine'
#get_ans_config['raw_refine']='both'
getAnswer(**get_ans_config)