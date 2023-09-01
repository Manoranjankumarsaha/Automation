import json
import time

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
transcribe_folder = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/TranscriptForGetAns/txt/'
raw_folder = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Raw3QDB/'
raw_folder_combined = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Raw3QDB/Combined'
refined_folder = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Refined3QDB/'
refined_folder_combined = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Refined3QDB/Combined'
processed_transcript_folder = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/TranscriptsProcessed/'
ext = 'txt'
rawparams = {"data_parameters": {"data": "", "data_type": "txt", "mode": "conversation"}, "pre_processing_parameters": {"do_punctuation_restoration": 0, "non_en_lang": 0}, "company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["supro profit mini"]}, "competitor_company_brand_info": {"Mahindra": ["Jeeto", "Jeeto Plus", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo", "Alpha Auto"], "Tata": ["Ace Gold", "Intra", "Magic"], "Maruti Suzuki": ["Super Carry"], "Ashok Leyland": ["Dost"], "Scooters India Limited": ["Vikram"], "Auto": ["Auto", "Chakado"]}, "aliases": {"supro": ["supra", "supero", "supera", "supreme"], "supro profit mini": ["supro mini", "profit truck", "profit mini", "mini profit"], "Ace Gold": ["chhota hathi", "chota hathi", "chhota haathi", "chota haathi", "ace", "small elephant", "tata s"], "Profit": ["Prophet"], "Mahindra": ["Mhindra", "Mahendra", "Mehindra"], "Ashok Leyland": ["ashok Lehlam"], "Bolero Pikup": ["bulero", "beloy", "bolero", "bolero pickup"], "Bolero Maxx Pikup": ["bolero max", "max pickup", "bolero max pickup"], "jeeto": ["jeetu", "jito", "zeta"], "Supro Profit Maxi": ["supro maxi", "profit maxi", "maxi profit", "profit max", "supro max", "maxi truck"], "Supro profit CNG Duo": ["supro cng", "supro duo", "profit duo"], "Carry": ["kairi", "hikari"], "pikup": ["pickup", "pick up"]}},
             "question_db_parameters": {"question_db": "", "question_db_type": "csv", "get_verbatim_answers": 1}}

refinedparams = {"company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["supro profit mini"]}, "competitor_company_brand_info": {"Mahindra": ["Jeeto", "Jeeto Plus", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo", "Alpha Auto"], "Tata": ["Ace Gold", "Intra", "Magic"], "Maruti Suzuki": [
    "Super Carry"], "Ashok Leyland": ["Dost"], "Scooters India Limited": ["Vikram"], "Auto": ["Auto", "Chakado"]}}, "question_db_parameters": {"question_db": "", "question_db_type": "csv"}, "raw_answer_parameters": {"raw_answer_db_type": "json", "raw_answer_db": [], "input_raw_answer_column_name": "Raw_Answers"}}


def get_raw_ans(qdb1, qdb2, qdb3, rawparams, refinedparams):
    all_tanscript_paths = s3FileListing(bucket_name, transcribe_folder, ext)
    k = 0
    for transcribe_path in all_tanscript_paths:
        k = k+1
        rawparams['data_parameters']['data'] = home_path+transcribe_path
#  ******************************************QDB1************************************************************************
        rawparams['question_db_parameters']['question_db'] = home_path+qdb1
        payload = json.dumps(rawparams)
        headers = {'Content-Type': 'application/json'}
        # ------------------------------------------------1ST TRY---------------------------------------------------------
        response_qdb1 = requests.request(
            "POST", GET_RAW_URL, headers=headers, data=payload, timeout=(10, 300))
        status = response_qdb1.status_code
        if status == 200:
            response_qdb1 = response_qdb1.json()
            # print(response_qdb1)
            if response_qdb1['status_code'] == 0:
                raw_answer_result = response_qdb1['raw_answer_result']
                filename = transcribe_path.split("/")[-1]
                json_file = filename.rsplit('.', 1)[0] + '_QDB1.json'
                prefix_json = f"{raw_folder}json/{json_file}"
                storeResponse(bucket_name, prefix_json, raw_answer_result)
                xlsx_file = filename.rsplit('.', 1)[0] + '_QDB1.xlsx'
                prefix_xlsx = f"{raw_folder}excel/{xlsx_file}"
                df_to_excel_s3_upload(
                    raw_answer_result, bucket_name, prefix_xlsx)
                # Refined ans
                refinedparams['question_db_parameters']['question_db'] = home_path+qdb1
                refinedparams['raw_answer_parameters']['raw_answer_db'] = response_qdb1['raw_answer_result']
                payload = json.dumps(refinedparams)
                headers = {'Content-Type': 'application/json'}
                # ------------------------------------------------1ST TRY---------------------------------------------------------
                response_refined_qdb1 = requests.request(
                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                status = response_refined_qdb1.status_code
                if status == 200:
                    response_refined_qdb1 = response_refined_qdb1.json()
                    # print(response_refined_qdb1)
                    if response_refined_qdb1['status_code'] == 0:
                        refined_answer_result = response_refined_qdb1['refined_answer_result']
                        json_file = filename.rsplit(
                            '.', 1)[0] + '_QDB1.json'
                        prefix_json = f"{refined_folder}json/{json_file}"
                        xlsx_file = filename.rsplit(
                            '.', 1)[0] + '_QDB1.xlsx'
                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                        storeResponse(
                            bucket_name, prefix_json, refined_answer_result)
                        df_to_excel_s3_upload(
                            refined_answer_result, bucket_name, prefix_xlsx)
                        # Refined ans
                # ------------------------------------------------2ND TRY---------------------------------------------------------
                    elif response_refined_qdb1['status_code'] == -6:
                        time.sleep(90)
                        response_refined_qdb1 = requests.request(
                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                        status = response_refined_qdb1.status_code
                        if status == 200:
                            response_refined_qdb1 = response_refined_qdb1.json()
                            if response_refined_qdb1['status_code'] == 0:
                                refined_answer_result = response_refined_qdb1['refined_answer_result']
                                json_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB1.json'
                                prefix_json = f"{refined_folder}json/{json_file}"
                                xlsx_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB1.xlsx'
                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                storeResponse(
                                    bucket_name, prefix_json, refined_answer_result)
                                df_to_excel_s3_upload(
                                    refined_answer_result, bucket_name, prefix_xlsx)
                                # Refined ans
                # ------------------------------------------------3RD TRY---------------------------------------------------------
                            elif response_refined_qdb1['status_code'] == -6:
                                time.sleep(90)
                                response_refined_qdb1 = requests.request(
                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                status = response_refined_qdb1.status_code
                                if status == 200:
                                    response_refined_qdb1 = response_refined_qdb1.json()
                                    if response_refined_qdb1['status_code'] == 0:
                                        refined_answer_result = response_refined_qdb1['refined_answer_result']
                                        json_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB1.json'
                                        prefix_json = f"{refined_folder}json/{json_file}"
                                        xlsx_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB1.xlsx'
                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                        storeResponse(
                                            bucket_name, prefix_json, refined_answer_result)
                                        df_to_excel_s3_upload(
                                            refined_answer_result, bucket_name, prefix_xlsx)
        # ------------------------------------------------2ND TRY---------------------------------------------------------
            elif response_qdb1['status_code'] == -6:
                time.sleep(90)
                response_qdb1 = requests.request(
                    "POST", GET_RAW_URL, headers=headers, data=payload, timeout=(10, 300))
                status = response_qdb1.status_code
                if status == 200:
                    response_qdb1 = response_qdb1.json()
                    if response_qdb1['status_code'] == 0:
                        raw_answer_result = response_qdb1['raw_answer_result']
                        filename = transcribe_path.split("/")[-1]
                        json_file = filename.rsplit('.', 1)[0] + '_QDB1.json'
                        prefix_json = f"{raw_folder}json/{json_file}"
                        storeResponse(bucket_name, prefix_json,
                                      raw_answer_result)
                        xlsx_file = filename.rsplit('.', 1)[0] + '_QDB1.xlsx'
                        prefix_xlsx = f"{raw_folder}excel/{xlsx_file}"
                        df_to_excel_s3_upload(
                            raw_answer_result, bucket_name, prefix_xlsx)
                        # Refined ans
                        refinedparams['question_db_parameters']['question_db'] = home_path+qdb1
                        refinedparams['raw_answer_parameters']['raw_answer_db'] = response_qdb1['raw_answer_result']
                        payload = json.dumps(refinedparams)
                        headers = {'Content-Type': 'application/json'}
                        # ------------------------------------------------1ST TRY---------------------------------------------------------
                        response_refined_qdb1 = requests.request(
                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                        status = response_refined_qdb1.status_code
                        if status == 200:
                            response_refined_qdb1 = response_refined_qdb1.json()
                            # print(response_refined_qdb1)
                            if response_refined_qdb1['status_code'] == 0:
                                refined_answer_result = response_refined_qdb1['refined_answer_result']
                                json_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB1.json'
                                prefix_json = f"{refined_folder}json/{json_file}"
                                xlsx_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB1.xlsx'
                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                storeResponse(
                                    bucket_name, prefix_json, refined_answer_result)
                                df_to_excel_s3_upload(
                                    refined_answer_result, bucket_name, prefix_xlsx)
                                # Refined ans

                        # ------------------------------------------------2ND TRY---------------------------------------------------------
                            elif response_refined_qdb1['status_code'] == -6:
                                time.sleep(90)
                                response_refined_qdb1 = requests.request(
                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                status = response_refined_qdb1.status_code
                                if status == 200:
                                    response_refined_qdb1 = response_refined_qdb1.json()
                                    if response_refined_qdb1['status_code'] == 0:
                                        refined_answer_result = response_refined_qdb1['refined_answer_result']
                                        json_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB1.json'
                                        prefix_json = f"{refined_folder}json/{json_file}"
                                        xlsx_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB1.xlsx'
                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                        storeResponse(
                                            bucket_name, prefix_json, refined_answer_result)
                                        df_to_excel_s3_upload(
                                            refined_answer_result, bucket_name, prefix_xlsx)
                                        # Refined ans
                        # ------------------------------------------------3RD TRY---------------------------------------------------------
                                    elif response_refined_qdb1['status_code'] == -6:
                                        time.sleep(90)
                                        response_refined_qdb1 = requests.request(
                                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                        status = response_refined_qdb1.status_code
                                        if status == 200:
                                            response_refined_qdb1 = response_refined_qdb1.json()
                                            if response_refined_qdb1['status_code'] == 0:
                                                refined_answer_result = response_refined_qdb1[
                                                    'refined_answer_result']
                                                json_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB1.json'
                                                prefix_json = f"{refined_folder}json/{json_file}"
                                                xlsx_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB1.xlsx'
                                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                                storeResponse(
                                                    bucket_name, prefix_json, refined_answer_result)
                                                df_to_excel_s3_upload(
                                                    refined_answer_result, bucket_name, prefix_xlsx)
        # ------------------------------------------------3RD TRY---------------------------------------------------------
                    elif response_qdb1['status_code'] == -6:
                        time.sleep(90)
                        response_qdb1 = requests.request(
                            "POST", GET_RAW_URL, headers=headers, data=payload, timeout=(10, 300))
                        status = response_qdb1.status_code
                        if status == 200:
                            response_qdb1 = response_qdb1.json()
                            if response_qdb1['status_code'] == 0:
                                raw_answer_result = response_qdb1['raw_answer_result']
                                filename = transcribe_path.split("/")[-1]
                                json_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB1.json'
                                prefix_json = f"{raw_folder}json/{json_file}"
                                storeResponse(
                                    bucket_name, prefix_json, raw_answer_result)
                                xlsx_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB1.xlsx'
                                prefix_xlsx = f"{raw_folder}excel/{xlsx_file}"
                                df_to_excel_s3_upload(
                                    raw_answer_result, bucket_name, prefix_xlsx)
                                # Refined ans
                                refinedparams['question_db_parameters']['question_db'] = home_path+qdb1
                                refinedparams['raw_answer_parameters']['raw_answer_db'] = response_qdb1['raw_answer_result']
                                payload = json.dumps(refinedparams)
                                headers = {'Content-Type': 'application/json'}
                                # ------------------------------------------------1ST TRY---------------------------------------------------------
                                response_refined_qdb1 = requests.request(
                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                status = response_refined_qdb1.status_code
                                if status == 200:
                                    response_refined_qdb1 = response_refined_qdb1.json()
                                    # print(response_refined_qdb1)
                                    if response_refined_qdb1['status_code'] == 0:
                                        refined_answer_result = response_refined_qdb1['refined_answer_result']
                                        json_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB1.json'
                                        prefix_json = f"{refined_folder}json/{json_file}"
                                        xlsx_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB1.xlsx'
                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                        storeResponse(
                                            bucket_name, prefix_json, refined_answer_result)
                                        df_to_excel_s3_upload(
                                            refined_answer_result, bucket_name, prefix_xlsx)
                                        # Refined ans
                                # ------------------------------------------------2ND TRY---------------------------------------------------------
                                    elif response_refined_qdb1['status_code'] == -6:
                                        time.sleep(90)
                                        response_refined_qdb1 = requests.request(
                                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                        status = response_refined_qdb1.status_code
                                        if status == 200:
                                            response_refined_qdb1 = response_refined_qdb1.json()
                                            if response_refined_qdb1['status_code'] == 0:
                                                refined_answer_result = response_refined_qdb1[
                                                    'refined_answer_result']
                                                json_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB1.json'
                                                prefix_json = f"{refined_folder}json/{json_file}"
                                                xlsx_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB1.xlsx'
                                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                                storeResponse(
                                                    bucket_name, prefix_json, refined_answer_result)
                                                df_to_excel_s3_upload(
                                                    refined_answer_result, bucket_name, prefix_xlsx)
                                                # Refined ans
                                # ------------------------------------------------3RD TRY---------------------------------------------------------
                                            elif response_refined_qdb1['status_code'] == -6:
                                                time.sleep(90)
                                                response_refined_qdb1 = requests.request(
                                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                                status = response_refined_qdb1.status_code
                                                if status == 200:
                                                    response_refined_qdb1 = response_refined_qdb1.json()
                                                    if response_refined_qdb1['status_code'] == 0:
                                                        refined_answer_result = response_refined_qdb1[
                                                            'refined_answer_result']
                                                        json_file = filename.rsplit(
                                                            '.', 1)[0] + '_QDB1.json'
                                                        prefix_json = f"{refined_folder}json/{json_file}"
                                                        xlsx_file = filename.rsplit(
                                                            '.', 1)[0] + '_QDB1.xlsx'
                                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                                        storeResponse(
                                                            bucket_name, prefix_json, refined_answer_result)
                                                        df_to_excel_s3_upload(
                                                            refined_answer_result, bucket_name, prefix_xlsx)
# ********************************************************************************QDB2*****************************************************
        rawparams['question_db_parameters']['question_db'] = home_path+qdb2
        payload = json.dumps(rawparams)
        headers = {'Content-Type': 'application/json'}
        # ------------------------------------------------1ST TRY---------------------------------------------------------
        response_qdb2 = requests.request(
            "POST", GET_RAW_URL, headers=headers, data=payload, timeout=(10, 300))
        status = response_qdb2.status_code
        if status == 200:
            response_qdb2 = response_qdb2.json()
            # print(response_qdb2)
            if response_qdb2['status_code'] == 0:
                raw_answer_result = response_qdb2['raw_answer_result']
                filename = transcribe_path.split("/")[-1]
                json_file = filename.rsplit('.', 1)[0] + '_QDB2.json'
                prefix_json = f"{raw_folder}json/{json_file}"
                storeResponse(bucket_name, prefix_json, raw_answer_result)
                xlsx_file = filename.rsplit('.', 1)[0] + '_QDB2.xlsx'
                prefix_xlsx = f"{raw_folder}excel/{xlsx_file}"
                df_to_excel_s3_upload(
                    raw_answer_result, bucket_name, prefix_xlsx)
                # Refined ans
                refinedparams['question_db_parameters']['question_db'] = home_path+qdb2
                refinedparams['raw_answer_parameters']['raw_answer_db'] = response_qdb2['raw_answer_result']
                payload = json.dumps(refinedparams)
                headers = {'Content-Type': 'application/json'}
                # ------------------------------------------------1ST TRY---------------------------------------------------------
                response_refined_qdb2 = requests.request(
                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                status = response_refined_qdb2.status_code
                if status == 200:
                    response_refined_qdb2 = response_refined_qdb2.json()
                    # print(response_refined_qdb2)
                    if response_refined_qdb2['status_code'] == 0:
                        refined_answer_result = response_refined_qdb2['refined_answer_result']
                        json_file = filename.rsplit('.', 1)[0] + '_QDB2.json'
                        prefix_json = f"{refined_folder}json/{json_file}"
                        storeResponse(bucket_name, prefix_json,
                                      refined_answer_result)
                        xlsx_file = filename.rsplit('.', 1)[0] + '_QDB2.xlsx'
                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                        df_to_excel_s3_upload(
                            refined_answer_result, bucket_name, prefix_xlsx)

                # ------------------------------------------------2ND TRY---------------------------------------------------------
                    elif response_refined_qdb2['status_code'] == -6:
                        time.sleep(90)
                        response_refined_qdb2 = requests.request(
                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                        status = response_refined_qdb2.status_code
                        if status == 200:
                            response_refined_qdb2 = response_refined_qdb2.json()
                            if response_refined_qdb2['status_code'] == 0:
                                refined_answer_result = response_refined_qdb2['refined_answer_result']
                                json_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB2.json'
                                prefix_json = f"{refined_folder}json/{json_file}"
                                storeResponse(
                                    bucket_name, prefix_json, refined_answer_result)
                                xlsx_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB2.xlsx'
                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                df_to_excel_s3_upload(
                                    refined_answer_result, bucket_name, prefix_xlsx)

                # ------------------------------------------------3RD TRY---------------------------------------------------------
                            elif response_refined_qdb2['status_code'] == -6:
                                time.sleep(90)
                                response_refined_qdb2 = requests.request(
                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                status = response_refined_qdb2.status_code
                                if status == 200:
                                    response_refined_qdb2 = response_refined_qdb2.json()
                                    if response_refined_qdb2['status_code'] == 0:
                                        refined_answer_result = response_refined_qdb2['refined_answer_result']
                                        json_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB2.json'
                                        prefix_json = f"{refined_folder}json/{json_file}"
                                        storeResponse(
                                            bucket_name, prefix_json, refined_answer_result)
                                        xlsx_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB2.xlsx'
                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                        df_to_excel_s3_upload(
                                            refined_answer_result, bucket_name, prefix_xlsx)
        # ------------------------------------------------2ND TRY---------------------------------------------------------
            elif response_qdb2['status_code'] == -6:
                time.sleep(90)
                response_qdb2 = requests.request(
                    "POST", GET_RAW_URL, headers=headers, data=payload, timeout=(10, 300))
                status = response_qdb2.status_code
                if status == 200:
                    response_qdb2 = response_qdb2.json()
                    if response_qdb2['status_code'] == 0:
                        raw_answer_result = response_qdb2['raw_answer_result']
                        filename = transcribe_path.split("/")[-1]
                        json_file = filename.rsplit('.', 1)[0] + '_QDB2.json'
                        prefix_json = f"{raw_folder}json/{json_file}"
                        storeResponse(bucket_name, prefix_json,
                                      raw_answer_result)
                        xlsx_file = filename.rsplit('.', 1)[0] + '_QDB2.xlsx'
                        prefix_xlsx = f"{raw_folder}excel/{xlsx_file}"
                        df_to_excel_s3_upload(
                            raw_answer_result, bucket_name, prefix_xlsx)
                        # Refined ans
                        refinedparams['question_db_parameters']['question_db'] = home_path+qdb2
                        refinedparams['raw_answer_parameters']['raw_answer_db'] = response_qdb2['raw_answer_result']
                        payload = json.dumps(refinedparams)
                        headers = {'Content-Type': 'application/json'}
                        # ------------------------------------------------1ST TRY---------------------------------------------------------
                        response_refined_qdb2 = requests.request(
                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                        status = response_refined_qdb2.status_code
                        if status == 200:
                            response_refined_qdb2 = response_refined_qdb2.json()
                            # print(response_refined_qdb2)
                            if response_refined_qdb2['status_code'] == 0:
                                refined_answer_result = response_refined_qdb2['refined_answer_result']
                                json_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB2.json'
                                prefix_json = f"{refined_folder}json/{json_file}"
                                storeResponse(
                                    bucket_name, prefix_json, refined_answer_result)
                                xlsx_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB2.xlsx'
                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                df_to_excel_s3_upload(
                                    refined_answer_result, bucket_name, prefix_xlsx)

                        # ------------------------------------------------2ND TRY---------------------------------------------------------
                            elif response_refined_qdb2['status_code'] == -6:
                                time.sleep(90)
                                response_refined_qdb2 = requests.request(
                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                status = response_refined_qdb2.status_code
                                if status == 200:
                                    response_refined_qdb2 = response_refined_qdb2.json()
                                    if response_refined_qdb2['status_code'] == 0:
                                        refined_answer_result = response_refined_qdb2['refined_answer_result']
                                        json_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB2.json'
                                        prefix_json = f"{refined_folder}json/{json_file}"
                                        storeResponse(
                                            bucket_name, prefix_json, refined_answer_result)
                                        xlsx_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB2.xlsx'
                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                        df_to_excel_s3_upload(
                                            refined_answer_result, bucket_name, prefix_xlsx)
                        # ------------------------------------------------3RD TRY---------------------------------------------------------
                                    elif response_refined_qdb2['status_code'] == -6:
                                        time.sleep(90)
                                        response_refined_qdb2 = requests.request(
                                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                        status = response_refined_qdb2.status_code
                                        if status == 200:
                                            response_refined_qdb2 = response_refined_qdb2.json()
                                            if response_refined_qdb2['status_code'] == 0:
                                                refined_answer_result = response_refined_qdb2[
                                                    'refined_answer_result']
                                                json_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB2.json'
                                                prefix_json = f"{refined_folder}json/{json_file}"
                                                storeResponse(
                                                    bucket_name, prefix_json, refined_answer_result)
                                                xlsx_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB2.xlsx'
                                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                                df_to_excel_s3_upload(
                                                    refined_answer_result, bucket_name, prefix_xlsx)
        # ------------------------------------------------3RD TRY---------------------------------------------------------
                    elif response_qdb2['status_code'] == -6:
                        time.sleep(90)
                        response_qdb2 = requests.request(
                            "POST", GET_RAW_URL, headers=headers, data=payload, timeout=(10, 300))
                        status = response_qdb2.status_code
                        if status == 200:
                            response_qdb2 = response_qdb2.json()
                            if response_qdb2['status_code'] == 0:
                                raw_answer_result = response_qdb2['raw_answer_result']
                                filename = transcribe_path.split("/")[-1]
                                json_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB2.json'
                                prefix_json = f"{raw_folder}json/{json_file}"
                                storeResponse(
                                    bucket_name, prefix_json, raw_answer_result)
                                xlsx_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB2.xlsx'
                                prefix_xlsx = f"{raw_folder}excel/{xlsx_file}"
                                df_to_excel_s3_upload(
                                    raw_answer_result, bucket_name, prefix_xlsx)
                                # Refined ans
                                refinedparams['question_db_parameters']['question_db'] = home_path+qdb2
                                refinedparams['raw_answer_parameters']['raw_answer_db'] = response_qdb2['raw_answer_result']
                                payload = json.dumps(refinedparams)
                                headers = {'Content-Type': 'application/json'}
                                # ------------------------------------------------1ST TRY---------------------------------------------------------
                                response_refined_qdb2 = requests.request(
                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                status = response_refined_qdb2.status_code
                                if status == 200:
                                    response_refined_qdb2 = response_refined_qdb2.json()
                                    # print(response_refined_qdb2)
                                    if response_refined_qdb2['status_code'] == 0:
                                        refined_answer_result = response_refined_qdb2['refined_answer_result']
                                        json_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB2.json'
                                        prefix_json = f"{refined_folder}json/{json_file}"
                                        storeResponse(
                                            bucket_name, prefix_json, refined_answer_result)
                                        xlsx_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB2.xlsx'
                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                        df_to_excel_s3_upload(
                                            refined_answer_result, bucket_name, prefix_xlsx)

                                # ------------------------------------------------2ND TRY---------------------------------------------------------
                                    elif response_refined_qdb2['status_code'] == -6:
                                        time.sleep(90)
                                        response_refined_qdb2 = requests.request(
                                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                        status = response_refined_qdb2.status_code
                                        if status == 200:
                                            response_refined_qdb2 = response_refined_qdb2.json()
                                            if response_refined_qdb2['status_code'] == 0:
                                                refined_answer_result = response_refined_qdb2[
                                                    'refined_answer_result']
                                                json_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB2.json'
                                                prefix_json = f"{refined_folder}json/{json_file}"
                                                storeResponse(
                                                    bucket_name, prefix_json, refined_answer_result)
                                                xlsx_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB2.xlsx'
                                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                                df_to_excel_s3_upload(
                                                    refined_answer_result, bucket_name, prefix_xlsx)

                                # ------------------------------------------------3RD TRY---------------------------------------------------------
                                            elif response_refined_qdb2['status_code'] == -6:
                                                time.sleep(90)
                                                response_refined_qdb2 = requests.request(
                                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                                status = response_refined_qdb2.status_code
                                                if status == 200:
                                                    response_refined_qdb2 = response_refined_qdb2.json()
                                                    if response_refined_qdb2['status_code'] == 0:
                                                        refined_answer_result = response_refined_qdb2[
                                                            'refined_answer_result']
                                                        json_file = filename.rsplit(
                                                            '.', 1)[0] + '_QDB2.json'
                                                        prefix_json = f"{refined_folder}json/{json_file}"
                                                        storeResponse(
                                                            bucket_name, prefix_json, refined_answer_result)
                                                        xlsx_file = filename.rsplit(
                                                            '.', 1)[0] + '_QDB2.xlsx'
                                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                                        df_to_excel_s3_upload(
                                                            refined_answer_result, bucket_name, prefix_xlsx)
# *******************************************************************QDB3*****************************************************************
        rawparams['question_db_parameters']['question_db'] = home_path+qdb3
        payload = json.dumps(rawparams)
        headers = {'Content-Type': 'application/json'}
        # ------------------------------------------------1ST TRY---------------------------------------------------------
        response_qdb3 = requests.request(
            "POST", GET_RAW_URL, headers=headers, data=payload, timeout=(10, 300))
        status = response_qdb3.status_code
        if status == 200:
            response_qdb3 = response_qdb3.json()
            # print(response_qdb3)
            if response_qdb3['status_code'] == 0:
                raw_answer_result = response_qdb3['raw_answer_result']
                filename = transcribe_path.split("/")[-1]
                json_file = filename.rsplit('.', 1)[0] + '_QDB3.json'
                prefix_json = f"{raw_folder}json/{json_file}"
                storeResponse(bucket_name, prefix_json, raw_answer_result)
                xlsx_file = filename.rsplit('.', 1)[0] + '_QDB3.xlsx'
                prefix_xlsx = f"{raw_folder}excel/{xlsx_file}"
                df_to_excel_s3_upload(
                    raw_answer_result, bucket_name, prefix_xlsx)
                # Refined ans
                refinedparams['question_db_parameters']['question_db'] = home_path+qdb3
                refinedparams['raw_answer_parameters']['raw_answer_db'] = response_qdb3['raw_answer_result']
                payload = json.dumps(refinedparams)
                headers = {'Content-Type': 'application/json'}
                # ------------------------------------------------1ST TRY---------------------------------------------------------
                response_refined_qdb3 = requests.request(
                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                status = response_refined_qdb3.status_code
                if status == 200:
                    response_refined_qdb3 = response_refined_qdb3.json()
                    # print(response_refined_qdb3)
                    if response_refined_qdb3['status_code'] == 0:
                        refined_answer_result = response_refined_qdb3['refined_answer_result']
                        json_file = filename.rsplit('.', 1)[0] + '_QDB3.json'
                        prefix_json = f"{refined_folder}json/{json_file}"
                        storeResponse(bucket_name, prefix_json,
                                      refined_answer_result)
                        xlsx_file = filename.rsplit('.', 1)[0] + '_QDB3.xlsx'
                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                        df_to_excel_s3_upload(
                            refined_answer_result, bucket_name, prefix_xlsx)

                # ------------------------------------------------2ND TRY---------------------------------------------------------
                    elif response_refined_qdb3['status_code'] == -6:
                        time.sleep(90)
                        response_refined_qdb3 = requests.request(
                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                        status = response_refined_qdb3.status_code
                        if status == 200:
                            response_refined_qdb3 = response_refined_qdb3.json()
                            if response_refined_qdb3['status_code'] == 0:
                                refined_answer_result = response_refined_qdb3['refined_answer_result']
                                json_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB3.json'
                                prefix_json = f"{refined_folder}json/{json_file}"
                                storeResponse(
                                    bucket_name, prefix_json, refined_answer_result)
                                xlsx_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB3.xlsx'
                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                df_to_excel_s3_upload(
                                    refined_answer_result, bucket_name, prefix_xlsx)
                # ------------------------------------------------3RD TRY---------------------------------------------------------
                            elif response_refined_qdb3['status_code'] == -6:
                                time.sleep(90)
                                response_refined_qdb3 = requests.request(
                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                status = response_refined_qdb3.status_code
                                if status == 200:
                                    response_refined_qdb3 = response_refined_qdb3.json()
                                    if response_refined_qdb3['status_code'] == 0:
                                        refined_answer_result = response_refined_qdb3['refined_answer_result']
                                        json_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB3.json'
                                        prefix_json = f"{refined_folder}json/{json_file}"
                                        storeResponse(
                                            bucket_name, prefix_json, refined_answer_result)
                                        xlsx_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB3.xlsx'
                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                        df_to_excel_s3_upload(
                                            refined_answer_result, bucket_name, prefix_xlsx)
        # ------------------------------------------------2ND TRY---------------------------------------------------------
            elif response_qdb3['status_code'] == -6:
                time.sleep(90)
                response_qdb3 = requests.request(
                    "POST", GET_RAW_URL, headers=headers, data=payload, timeout=(10, 300))
                status = response_qdb3.status_code
                if status == 200:
                    response_qdb3 = response_qdb3.json()
                    if response_qdb3['status_code'] == 0:
                        raw_answer_result = response_qdb3['raw_answer_result']
                        filename = transcribe_path.split("/")[-1]
                        json_file = filename.rsplit('.', 1)[0] + '_QDB3.json'
                        prefix_json = f"{raw_folder}json/{json_file}"
                        storeResponse(bucket_name, prefix_json,
                                      raw_answer_result)
                        xlsx_file = filename.rsplit('.', 1)[0] + '_QDB3.xlsx'
                        prefix_xlsx = f"{raw_folder}excel/{xlsx_file}"
                        df_to_excel_s3_upload(
                            raw_answer_result, bucket_name, prefix_xlsx)
                        # Refined ans
                        refinedparams['question_db_parameters']['question_db'] = home_path+qdb3
                        refinedparams['raw_answer_parameters']['raw_answer_db'] = response_qdb3['raw_answer_result']
                        payload = json.dumps(refinedparams)
                        headers = {'Content-Type': 'application/json'}
                        # ------------------------------------------------1ST TRY---------------------------------------------------------
                        response_refined_qdb3 = requests.request(
                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                        status = response_refined_qdb3.status_code
                        if status == 200:
                            response_refined_qdb3 = response_refined_qdb3.json()
                            # print(response_refined_qdb3)
                            if response_refined_qdb3['status_code'] == 0:
                                refined_answer_result = response_refined_qdb3['refined_answer_result']
                                json_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB3.json'
                                prefix_json = f"{refined_folder}json/{json_file}"
                                storeResponse(
                                    bucket_name, prefix_json, refined_answer_result)
                                xlsx_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB3.xlsx'
                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                df_to_excel_s3_upload(
                                    refined_answer_result, bucket_name, prefix_xlsx)

                        # ------------------------------------------------2ND TRY---------------------------------------------------------
                            elif response_refined_qdb3['status_code'] == -6:
                                time.sleep(90)
                                response_refined_qdb3 = requests.request(
                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                status = response_refined_qdb3.status_code
                                if status == 200:
                                    response_refined_qdb3 = response_refined_qdb3.json()
                                    if response_refined_qdb3['status_code'] == 0:
                                        refined_answer_result = response_refined_qdb3['refined_answer_result']
                                        json_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB3.json'
                                        prefix_json = f"{refined_folder}json/{json_file}"
                                        storeResponse(
                                            bucket_name, prefix_json, refined_answer_result)
                                        xlsx_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB3.xlsx'
                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                        df_to_excel_s3_upload(
                                            refined_answer_result, bucket_name, prefix_xlsx)

                        # ------------------------------------------------3RD TRY---------------------------------------------------------
                                    elif response_refined_qdb3['status_code'] == -6:
                                        time.sleep(90)
                                        response_refined_qdb3 = requests.request(
                                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                        status = response_refined_qdb3.status_code
                                        if status == 200:
                                            response_refined_qdb3 = response_refined_qdb3.json()
                                            if response_refined_qdb3['status_code'] == 0:
                                                refined_answer_result = response_refined_qdb3[
                                                    'refined_answer_result']
                                                json_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB3.json'
                                                prefix_json = f"{refined_folder}json/{json_file}"
                                                storeResponse(
                                                    bucket_name, prefix_json, refined_answer_result)
                                                xlsx_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB3.xlsx'
                                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                                df_to_excel_s3_upload(
                                                    refined_answer_result, bucket_name, prefix_xlsx)
        # ------------------------------------------------3RD TRY---------------------------------------------------------
                    elif response_qdb3['status_code'] == -6:
                        time.sleep(90)
                        response_qdb3 = requests.request(
                            "POST", GET_RAW_URL, headers=headers, data=payload, timeout=(10, 300))
                        status = response_qdb3.status_code
                        if status == 200:
                            response_qdb3 = response_qdb3.json()
                            if response_qdb3['status_code'] == 0:
                                raw_answer_result = response_qdb3['raw_answer_result']
                                filename = transcribe_path.split("/")[-1]
                                json_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB3.json'
                                prefix_json = f"{raw_folder}json/{json_file}"
                                storeResponse(
                                    bucket_name, prefix_json, raw_answer_result)
                                xlsx_file = filename.rsplit(
                                    '.', 1)[0] + '_QDB3.xlsx'
                                prefix_xlsx = f"{raw_folder}excel/{xlsx_file}"
                                df_to_excel_s3_upload(
                                    raw_answer_result, bucket_name, prefix_xlsx)
                                # Refined ans
                                refinedparams['question_db_parameters']['question_db'] = home_path+qdb3
                                refinedparams['raw_answer_parameters']['raw_answer_db'] = response_qdb3['raw_answer_result']
                                payload = json.dumps(refinedparams)
                                headers = {'Content-Type': 'application/json'}
                                # ------------------------------------------------1ST TRY---------------------------------------------------------
                                response_refined_qdb3 = requests.request(
                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                status = response_refined_qdb3.status_code
                                if status == 200:
                                    response_refined_qdb3 = response_refined_qdb3.json()
                                    # print(response_refined_qdb3)
                                    if response_refined_qdb3['status_code'] == 0:
                                        refined_answer_result = response_refined_qdb3['refined_answer_result']
                                        json_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB3.json'
                                        prefix_json = f"{refined_folder}json/{json_file}"
                                        storeResponse(
                                            bucket_name, prefix_json, refined_answer_result)
                                        xlsx_file = filename.rsplit(
                                            '.', 1)[0] + '_QDB3.xlsx'
                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                        df_to_excel_s3_upload(
                                            refined_answer_result, bucket_name, prefix_xlsx)
                                # ------------------------------------------------2ND TRY---------------------------------------------------------
                                    elif response_refined_qdb3['status_code'] == -6:
                                        time.sleep(90)
                                        response_refined_qdb3 = requests.request(
                                            "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                        status = response_refined_qdb3.status_code
                                        if status == 200:
                                            response_refined_qdb3 = response_refined_qdb3.json()
                                            if response_refined_qdb3['status_code'] == 0:
                                                refined_answer_result = response_refined_qdb3[
                                                    'refined_answer_result']
                                                json_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB3.json'
                                                prefix_json = f"{refined_folder}json/{json_file}"
                                                storeResponse(
                                                    bucket_name, prefix_json, refined_answer_result)
                                                xlsx_file = filename.rsplit(
                                                    '.', 1)[0] + '_QDB3.xlsx'
                                                prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                                df_to_excel_s3_upload(
                                                    refined_answer_result, bucket_name, prefix_xlsx)

                                # ------------------------------------------------3RD TRY---------------------------------------------------------
                                            elif response_refined_qdb3['status_code'] == -6:
                                                time.sleep(90)
                                                response_refined_qdb3 = requests.request(
                                                    "POST", GET_REFINED_URL, headers=headers, data=payload, timeout=(10, 300))
                                                status = response_refined_qdb3.status_code
                                                if status == 200:
                                                    response_refined_qdb3 = response_refined_qdb3.json()
                                                    if response_refined_qdb3['status_code'] == 0:
                                                        refined_answer_result = response_refined_qdb3[
                                                            'refined_answer_result']
                                                        json_file = filename.rsplit(
                                                            '.', 1)[0] + '_QDB3.json'
                                                        prefix_json = f"{refined_folder}json/{json_file}"
                                                        storeResponse(
                                                            bucket_name, prefix_json, refined_answer_result)
                                                        xlsx_file = filename.rsplit(
                                                            '.', 1)[0] + '_QDB3.xlsx'
                                                        prefix_xlsx = f"{refined_folder}excel/{xlsx_file}"
                                                        df_to_excel_s3_upload(
                                                            refined_answer_result, bucket_name, prefix_xlsx)
        if response_qdb1 is not None and response_qdb2 is not None and response_qdb3 is not None:
            combined_raw_answer_result = combine_outputs_raw(
                response_qdb1, response_qdb2, response_qdb3)
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
        if response_refined_qdb1 is not None and response_refined_qdb2 is not None and response_refined_qdb3 is not None:
            combined_refined_answer_result = combine_outputs_refined(
                response_refined_qdb1, response_refined_qdb2, response_refined_qdb3)
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

        if k == 5:
            break


def combine_outputs_raw(response_qdb1, response_qdb2, response_qdb3):
    raw_ans_1 = response_qdb1['raw_answer_result']
    raw_ans_2 = response_qdb2['raw_answer_result']
    raw_ans_3 = response_qdb3['raw_answer_result']
    response_all = []
    for dict in raw_ans_1:
        response_all.append(dict)
    for dict in raw_ans_2:
        response_all.append(dict)
    for dict in raw_ans_3:
        response_all.append(dict)
    return (response_all)


def combine_outputs_refined(response_qdb1, response_qdb2, response_qdb3):
    raw_ans_1 = response_qdb1['refined_answer_result']
    raw_ans_2 = response_qdb2['refined_answer_result']
    raw_ans_3 = response_qdb3['refined_answer_result']
    response_all = []
    for dict in raw_ans_1:
        response_all.append(dict)
    for dict in raw_ans_2:
        response_all.append(dict)
    for dict in raw_ans_3:
        response_all.append(dict)
    return (response_all)


get_raw_ans(QDB1, QDB2, QDB3, rawparams, refinedparams)
