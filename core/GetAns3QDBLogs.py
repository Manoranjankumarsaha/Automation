import datetime
import time
import pandas as pd
import requests
import json
from requests.exceptions import ConnectTimeout
from requests.exceptions import ReadTimeout
import requests
from common import s3FileListing, storeResponse, df_to_excel_s3_upload, move_s3_object


bucket_name = "ixolerator-cloud"
home_path = '/home/ubuntu/s3_bucket/'
VTI_SERVER = "http://demolive.meghnad.inxiteout.ai:5009"
GET_RAW_URL = VTI_SERVER+"/get_qa_raw_answers"
GET_REFINED_URL = VTI_SERVER + "/get_qa_refined_answers"
QDB1 = 'question_db/mahindra_supro/Version_5/Mahindra_Supro_Mini_V5_Part1.csv'
QDB2 = 'question_db/mahindra_supro/Version_5/Mahindra_Supro_Mini_V5_Part2.csv'
QDB3 = 'question_db/mahindra_supro/Version_5/Mahindra_Supro_Mini_V5_Part3.csv'
transcribe_folder = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Transcript/txt/'
raw_folder = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Raw3QDB_2.0.3/'
raw_folder_combined = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Raw3QDB_2.0.3_combined/'
refined_folder = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Refined3QDB_2.0.3/'
refined_folder_combined = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Refined3QDB_2.0.3_combined/'
processed_transcript_folder = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Transcript_Processed_2.0.3/'
ext = 'txt'
rawparams = {"data_parameters": {"data": "", "data_type": "txt", "mode": "conversation"}, "pre_processing_parameters": {"do_punctuation_restoration": 0, "non_en_lang": 0}, "company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["supro profit mini"]}, "competitor_company_brand_info": {"Mahindra": ["Jeeto", "Jeeto Plus", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo", "Alpha Auto"], "Tata": ["Ace Gold", "Intra", "Magic"], "Maruti Suzuki": ["Super Carry"], "Ashok Leyland": ["Dost"], "Scooters India Limited": ["Vikram"], "Auto": ["Auto", "Chakado"]}, "aliases": {"supro": ["supra", "supero", "supera", "supreme"], "supro profit mini": ["supro mini", "profit truck", "profit mini", "mini profit"], "Ace Gold": ["chhota hathi", "chota hathi", "chhota haathi", "chota haathi", "ace", "small elephant", "tata s"], "Profit": ["Prophet"], "Mahindra": ["Mhindra", "Mahendra", "Mehindra"], "Ashok Leyland": ["ashok Lehlam"], "Bolero Pikup": ["bulero", "beloy", "bolero", "bolero pickup"], "Bolero Maxx Pikup": ["bolero max", "max pickup", "bolero max pickup"], "jeeto": ["jeetu", "jito", "zeta"], "Supro Profit Maxi": ["supro maxi", "profit maxi", "maxi profit", "profit max", "supro max", "maxi truck"], "Supro profit CNG Duo": ["supro cng", "supro duo", "profit duo"], "Carry": ["kairi", "hikari"], "pikup": ["pickup", "pick up"]}},
             "question_db_parameters": {"question_db": "", "question_db_type": "csv", "get_verbatim_answers": 1, "high_complexity": 1}}

refinedparams = {"company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["supro profit mini"]}, "competitor_company_brand_info": {"Mahindra": ["Jeeto", "Jeeto Plus", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo", "Alpha Auto"], "Tata": ["Ace Gold", "Intra", "Magic"], "Maruti Suzuki": [
    "Super Carry"], "Ashok Leyland": ["Dost"], "Scooters India Limited": ["Vikram"], "Auto": ["Auto", "Chakado"]}}, "question_db_parameters": {"question_db": "", "question_db_type": "csv"}, "raw_answer_parameters": {"raw_answer_db_type": "json", "raw_answer_db": [], "input_raw_answer_column_name": "Raw_Answers"}}

headers = {'Content-Type': 'application/json'}


def send_request(url, payload, max_retries=3, sleep_time=90):
    for _ in range(max_retries):
        response = requests.post(url, headers=headers,
                                 data=payload, timeout=(15, 300))
        if response.status_code == 200:
            return response.json()
        time.sleep(sleep_time)
    return None


def combine_outputs_raw(raw_ans_1, raw_ans_2, raw_ans_3):
    response_all = []
    for dict in raw_ans_1:
        response_all.append(dict)
    for dict in raw_ans_2:
        response_all.append(dict)
    for dict in raw_ans_3:
        response_all.append(dict)
    return (response_all)


def combine_outputs_refined(refined_ans_1, refined_ans_2, refined_ans_3):
    response_all = []
    for dict in refined_ans_1:
        response_all.append(dict)
    for dict in refined_ans_2:
        response_all.append(dict)
    for dict in refined_ans_3:
        response_all.append(dict)
    return (response_all)


def process_raw_ans(qdb_params, url, folder_prefix, transcribe_path, QDBstr):
    rtn = {"status": "", "status_code": "200", "resp_code": "",
           "status_message": "", "response": {}}
    raw_answer_result = None
    try:
        filename = transcribe_path.split("/")[-1]
        rawparams['data_parameters']['data'] = home_path+transcribe_path
        rawparams['question_db_parameters']['question_db'] = home_path + qdb_params
        payload = json.dumps(rawparams)
        # print(url)
        # print(payload)
        response_qdb = send_request(url, payload)
        # print(response_qdb)
        if response_qdb and response_qdb['status_code'] == 0:
            rtn = {"status": "success",
                   "resp_code": response_qdb["status_code"], "status_message": "", "response": response_qdb}
            raw_answer_result = response_qdb['raw_answer_result']
            json_file = filename.rsplit('.', 1)[0] + f'_{QDBstr}.json'
            prefix_json = f"{folder_prefix}json/{json_file}"
            storeResponse(bucket_name, prefix_json, raw_answer_result)
            xlsx_file = filename.rsplit('.', 1)[0] + f'_{QDBstr}.xlsx'
            prefix_xlsx = f"{folder_prefix}excel/{xlsx_file}"
            df_to_excel_s3_upload(raw_answer_result, bucket_name, prefix_xlsx)
            return raw_answer_result, rtn
        elif response_qdb and response_qdb['status_code'] != 0:
            rtn = {"status": "success",
                   "resp_code": response_qdb["status_code"], "status_message": "", "response": response_qdb}
            return None, rtn
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
        return raw_answer_result, rtn


def process_refined_response(url, answer_result, folder_prefix, filename, qdb_params, QDBstr):
    rtn = {"status": "", "status_code": "200", "resp_code": "",
           "status_message": "", "response": {}}
    refined_answer_result = None
    try:
        refinedparams['question_db_parameters']['question_db'] = home_path + qdb_params
        refinedparams['raw_answer_parameters']['raw_answer_db'] = answer_result
        payload = json.dumps(refinedparams)
        # print(url)
        # print(payload)
        response_refined = send_request(url, payload)
        # print(response_refined)
        if response_refined and response_refined['status_code'] == 0:
            rtn = {"status": "success",
                   "resp_code": response_refined["status_code"], "status_message": "", "response": response_refined}
            refined_answer_result = response_refined['refined_answer_result']
            json_file = filename.rsplit('.', 1)[0] + f'_{QDBstr}.json'
            prefix_json = f"{folder_prefix}json/{json_file}"
            xlsx_file = filename.rsplit('.', 1)[0] + f'_{QDBstr}.xlsx'
            prefix_xlsx = f"{folder_prefix}excel/{xlsx_file}"
            storeResponse(bucket_name, prefix_json, refined_answer_result)
            df_to_excel_s3_upload(refined_answer_result,
                                  bucket_name, prefix_xlsx)
            return refined_answer_result, rtn
        elif response_refined and response_refined['status_code'] != 0:
            rtn = {"status": "success",
                   "resp_code": response_refined["status_code"], "status_message": "", "response": response_refined}
        return None, rtn
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
        return refined_answer_result, rtn


def get_ans_raw_refined():
    all_tanscript_paths = s3FileListing(bucket_name, transcribe_folder, ext)
    k = 0
    logs = []
    raw_answer_result_1 = None
    raw_answer_result_2 = None
    raw_answer_result_3 = None
    refined_answer_result_1 = None
    refined_answer_result_2 = None
    refined_answer_result_3 = None
    for transcribe_path in all_tanscript_paths:
        print(transcribe_path)
        k = k+1
        filename = transcribe_path.split("/")[-1]
        # Process QDB1
        qdb_params = QDB1
        QDBstr = 'QDB1'
        raw_answer_result_1, log = process_raw_ans(
            qdb_params, GET_RAW_URL, raw_folder, transcribe_path, QDBstr)
        log['count'] = k
        log["filepath"] = transcribe_path
        log['api'] = 'RAW1'
        # print(f"Raw log 1 : {log}")
        logs.append(log)
        if raw_answer_result_1 is not None:
            refined_answer_result_1, log = process_refined_response(
                GET_REFINED_URL, raw_answer_result_1, refined_folder, filename, qdb_params, QDBstr)
            log['count'] = k
            log["filepath"] = transcribe_path
            log['api'] = 'REFINED1'
            # print(f"Ref log 1: {log}")
            logs.append(log)
        else:
            log = {"status": "Raw1:None",
                   "resp_code": 'None', "status_message": 'None', "response": 'None'}
            log['count'] = k
            log["filepath"] = transcribe_path
            log['api'] = 'REFINED1'
            logs.append(log)
        # Process QDB2
        qdb_params = QDB2
        QDBstr = 'QDB2'
        raw_answer_result_2, log = process_raw_ans(
            qdb_params, GET_RAW_URL, raw_folder, transcribe_path, QDBstr)
        log['count'] = k
        log["filepath"] = transcribe_path
        log['api'] = 'RAW2'
        # print(f"Raw log 2: {log}")
        logs.append(log)
        if raw_answer_result_2 is not None:
            refined_answer_result_2, log = process_refined_response(
                GET_REFINED_URL, raw_answer_result_2, refined_folder, filename, qdb_params, QDBstr)
            log['count'] = k
            log["filepath"] = transcribe_path
            log['api'] = 'REFINED2'
            # print(f"Ref log 2: {log}")
            logs.append(log)
        else:
            log = {"status": "Raw2:None",
                   "resp_code": 'None', "status_message": 'None', "response": 'None'}
            log['count'] = k
            log["filepath"] = transcribe_path
            log['api'] = 'REFINED2'
            logs.append(log)
        # Process QDB3
        qdb_params = QDB3
        QDBstr = 'QDB3'
        raw_answer_result_3, log = process_raw_ans(
            qdb_params, GET_RAW_URL, raw_folder, transcribe_path, QDBstr)
        log['count'] = k
        log["filepath"] = transcribe_path
        log['api'] = 'RAW3'
        # print(f"Raw log 3: {log}")
        logs.append(log)
        if raw_answer_result_3 is not None:
            refined_answer_result_3, log = process_refined_response(
                GET_REFINED_URL, raw_answer_result_3, refined_folder, filename, qdb_params, QDBstr)
            log['count'] = k
            log["filepath"] = transcribe_path
            log['api'] = 'REFINED3'
            # print(f"Ref log 3: {log}")
            logs.append(log)
        else:
            log = {"status": "Raw3:None",
                   "resp_code": 'None', "status_message": 'None', "response": 'None'}
            log['count'] = k
            log["filepath"] = transcribe_path
            log['api'] = 'REFINED3'
            logs.append(log)
        if raw_answer_result_1 is not None and raw_answer_result_2 is not None and raw_answer_result_3 is not None:
            combined_raw_answer_result = combine_outputs_raw(
                raw_answer_result_1, raw_answer_result_2, raw_answer_result_3)
            filename = transcribe_path.split("/")[-1]
            json_file = filename.rsplit(
                '.', 1)[0] + '.json'
            prefix_json = f"{raw_folder_combined}json/{json_file}"
            storeResponse(
                bucket_name, prefix_json, combined_raw_answer_result)
            xlsx_file = filename.rsplit(
                '.', 1)[0] + '.xlsx'
            prefix_xlsx = f"{raw_folder_combined}excel/{xlsx_file}"
            df_to_excel_s3_upload(
                combined_raw_answer_result, bucket_name, prefix_xlsx)
        else:
            pass
        if refined_answer_result_1 is not None and refined_answer_result_2 is not None and refined_answer_result_3 is not None:
            combined_refined_answer_result = combine_outputs_refined(
                refined_answer_result_1, refined_answer_result_2, refined_answer_result_3)
            filename = transcribe_path.split("/")[-1]
            json_file = filename.rsplit(
                '.', 1)[0] + '.json'
            prefix_json = f"{refined_folder_combined}json/{json_file}"
            storeResponse(
                bucket_name, prefix_json, combined_refined_answer_result)
            xlsx_file = filename.rsplit(
                '.', 1)[0] + '.xlsx'
            prefix_xlsx = f"{refined_folder_combined}excel/{xlsx_file}"
            df_to_excel_s3_upload(
                combined_refined_answer_result, bucket_name, prefix_xlsx)
            move_s3_object(bucket_name, transcribe_path,
                           f"{processed_transcript_folder}{filename}")
        else:
            pass

        # if k == 2:
        #     break
    if len(all_tanscript_paths) == 0:
        logs.append({"status": '0', "count": "0", "status_code": "0",
                    "status_message": "No Files", "ext": "NILL", "filepath": "NILL"})

    print("\n******************************************************************GetAnswer Process Result*********************************************************")
    df = pd.DataFrame(logs)
    print(df)
    cdt = datetime.datetime.now()
    df_fn = "GetAns_log"+cdt.strftime("%Y_%m_%d_%H%M%S")
    s3_key = f'{refined_folder}{df_fn}.xlsx'
    df_to_excel_s3_upload(df, bucket_name, s3_key)
    print("\n*****************************************************************************************************************************************************")


get_ans_raw_refined()
