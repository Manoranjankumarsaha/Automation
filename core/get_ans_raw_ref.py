import datetime
import json
import time
import requests
from requests.exceptions import ConnectTimeout
from requests.exceptions import ReadTimeout
import pandas as pd

from common import initGetAnswer, s3FileListing, storeResponse, df_to_excel_s3_upload
from constant import S3PATH, RAW_PAYLOAD, REFINED_PAYLOAD


def getAnswer(**get_ans_config):
    bucket_name = get_ans_config['bucket_name']
    question_db = get_ans_config['question_db']
    # print(question_db)
    raw_url = get_ans_config['raw_url']
    # print(raw_url)
    refined_url = get_ans_config['refined_url']
    # print(refined_url)
    inputfiles = get_ans_config['input_path']

    raw_output = get_ans_config['raw_ans_output_path']
    # print(inputfiles)
    s3_folder_refined = get_ans_config['output_path']
    # print(s3_folder_refined)
    file_ext = "txt"
    allfilepath = s3FileListing(bucket_name, inputfiles, file_ext)
    logs = []
    k = 0
    for path in allfilepath:
        k = k+1
        print(path)
        ext = (path.split("/")[-1]).split(".")[-1].lower()
        if ext == file_ext:
            data_path = f"{S3PATH}{path}"
            raw_answer_data, log = apiCall(
                question_db, data_path, raw_url, refined_url, s3_folder_refined)
            log['count'] = k
            log["filepath"] = data_path
            if raw_answer_data is not None:
                filename = path.split("/")[-1]
                json_file = filename.rsplit('.', 1)[0] + '.json'
                prefix_json = f"{raw_output}json/{json_file}"
                storeResponse(bucket_name, prefix_json, raw_answer_data)
                xlsx_file = filename.rsplit('.', 1)[0] + '.xlsx'
                prefix_xlsx = f"{raw_output}excel/{xlsx_file}"
                df_to_excel_s3_upload(
                    raw_answer_data, bucket_name, prefix_xlsx)
            if log["status"] == "success":
                filename = path.split("/")[-1]
                json_file = filename.rsplit('.', 1)[0] + '.json'
                prefix_json = f"{s3_folder_refined}json/{json_file}"
                storeResponse(bucket_name, prefix_json,
                              log['response']['refined_answer_result'])
                xlsx_file = filename.rsplit('.', 1)[0] + '.xlsx'
                prefix_xlsx = f"{s3_folder_refined}excel/{xlsx_file}"
                df_to_excel_s3_upload(
                    log['response']['refined_answer_result'], bucket_name, prefix_xlsx)
            logs.append(log)
        # if k == 1:
        #     break
    if len(allfilepath) == 0:
        logs.append({"status": '0', "count": "0", "status_code": "0",
                    "status_message": "No Files", "ext": "NILL", "filepath": "NILL"})

    print("\n******************************************************************GetAnswer Process Result*********************************************************")
    df = pd.DataFrame(logs)
    print(df)
    cdt = datetime.datetime.now()
    df_fn = "GetAns_log"+cdt.strftime("%Y_%m_%d_%H%M%S")
    s3_key = f'{s3_folder_refined}{df_fn}.xlsx'
    df_to_excel_s3_upload(df, bucket_name, s3_key)
    print("\n*****************************************************************************************************************************************************")
    # df.to_excel(r'C:\Users\rokad\Downloads\getans_getans.xlsx',
    #             index=False, sheet_name="Sheet1")


def getRawAnswers(question_db, data_path, raw_url):
    rtn = {"status": "", "status_code": "", "resp_code": "",
           "status_message": "", "response": {}}
    try:
        compParam = RAW_PAYLOAD
        compParam["data_parameters"]["data"] = data_path
        compParam["question_db_parameters"]["question_db"] = question_db
        url = raw_url
        # print(url)
        payload = json.dumps(compParam)
        # print("Raw Payload")
        # print(payload)
        headers = {'Content-Type': 'application/json'}
        response = requests.request(
            "POST", url, headers=headers, data=payload, timeout=(10, 300))
        # print(response.json())
        status = response.status_code
        if status == 200:
            respObj = response.json()
            if respObj["status_code"] == 0:
                rtn = {"status": "success", "status_code": status,
                       "resp_code": respObj["status_code"], "status_message": "", "response": respObj}
            elif respObj["status_code"] == -6:
                response = requests.request(
                    "POST", url, headers=headers, data=payload, timeout=(10, 60*3))
                status = response.status_code
                if status == 200:
                    respObj = response.json()
                    if respObj["status_code"] == 0:
                        rtn = {"status": "success", "status_code": status,
                               "resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
                    else:
                        rtn = {"status": "Error", "status_code": status,
                               "resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
            else:
                rtn = {"status": "Error", "status_code": status,
                       "resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
        else:
            rtn = {"status": "Error", "status_code": status, "resp_code": "",
                   "status_message": "get_qa_raw_answers api error", "response": {}}
    except ConnectTimeout as e:
        rtn = {"status": "Error", "status_code": 522, "resp_code": "",
               "status_message": "ConnectTimeout error", "response": {}}
    except ReadTimeout as e:
        rtn = {"status": "Error", "status_code": 522, "resp_code": "",
               "status_message": "ReadTimeout error", "response": {}}
    except Exception as e:
        print(e)
        rtn = {"status": "Error", "status_code": "Exception",
               "resp_code": "", "status_message": "Error", "response": {}}
    finally:
        return rtn


def getRefinedAnswers(question_db, raw_answer_db, refined_url):
    try:
        compParam = REFINED_PAYLOAD
        compParam["question_db_parameters"]["question_db"] = question_db
        compParam["raw_answer_parameters"]["raw_answer_db"] = raw_answer_db
        payload = json.dumps(compParam)
        url = refined_url
        # print(url)
        # print("Refined Payload")
        # print(payload)
        headers = {'Content-Type': 'application/json'}
        response = requests.request(
            "POST", url, headers=headers, data=payload, timeout=(10, 60*3))
        status = response.status_code
        if status == 200:
            respObj = response.json()
            if respObj["status_code"] == 0:
                rtn = {"status": "success", "status_code": status,
                       "resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
            elif respObj["status_code"] == -6:
                time.sleep(90)
                response = requests.request(
                    "POST", url, headers=headers, data=payload, timeout=(10, 60*3))
                status = response.status_code
                if status == 200:
                    respObj = response.json()
                    if respObj["status_code"] == 0:
                        rtn = {"status": "success", "status_code": status,
                               "resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
                    elif respObj["status_code"] == -6:
                        time.sleep(90)
                        response = requests.request(
                            "POST", url, headers=headers, data=payload, timeout=(10, 60*3))
                        status = response.status_code
                        if status == 200:
                            respObj = response.json()
                            if respObj["status_code"] == 0:
                                rtn = {"status": "success", "status_code": status,
                                       "resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
                            else:
                                rtn = {"status": "Error", "status_code": status,
                                       "resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
            else:
                rtn = {"status": "Error", "status_code": status,
                       "resp_code": respObj["status_code"], "status_message": respObj["status_message"], "response": respObj}
        else:
            rtn = {"status": "Error", "status_code": 522, "resp_code": "",
                   "status_message": "get_qa_refined_answers api rrror", "response": {}}
    except ConnectTimeout as e:
        rtn = {"status": "Error", "status_code": 522, "resp_code": "",
               "status_message": "ConnectTimeout", "response": {}}
    except ReadTimeout as e:
        rtn = {"status": "Error", "status_code": 522, "resp_code": "",
               "status_message": "ReadTimeout", "response": {}}
    except Exception as e:
        rtn = {"status": "Error", "status_code": 522, "resp_code": "",
               "status_message": "Error", "response": {}}
    finally:
        return rtn


def apiCall(question_db, data_path, raw_url, refined_url, s3_folder_refined):
    rtn = getRawAnswers(question_db, data_path, raw_url)
    rtn['api'] = "Raw"
    raw_answer_data = None
    if rtn['status'] == 'success':
        raw_answer_data = rtn['response']['raw_answer_result']
        rtn = getRefinedAnswers(question_db, raw_answer_data, refined_url)
        rtn['api'] = "Refined"
    return raw_answer_data, rtn


get_ans_config = initGetAnswer()
getAnswer(**get_ans_config)
