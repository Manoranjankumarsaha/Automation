import json
import time
import traceback
import datetime
import requests
from requests.exceptions import ConnectTimeout
from requests.exceptions import ReadTimeout
import pandas as pd

# self call
from common import initTranscriptEngine, s3FileListing, storeResponse, move_s3_object, df_to_excel_s3_upload
from constant import AUDIO_EXT

S3PATH = "/home/deeplearningcv/s3_bucket/"


def try_failure(resp, url, payload):
    payload = json.loads(payload)
    retry_final = False
    if resp["status_code"] == -6:
        for i in range(1, 4):
            if (i < 3):
                time.sleep(60)
            response = transcriptRequest(url, json.dumps(payload))
            if response.status_code == 200:
                rtn = response.json()
                if rtn["status_code"] == 0:
                    rtn["status"] = f'success try cont-{i}'
                    retry_final = False
                    return True, rtn
                else:
                    retry_final = True
            else:
                retry_final = True
    if resp["status_code"] == -1 or (resp["status_code"] == 0 and resp["confidence_score"] < 0.2) or retry_final == True:
        if resp["transcribed_detected_lang"] is not None:
            payload['lang'] = resp["transcribed_detected_lang"]
            response = transcriptRequest(url, json.dumps(payload))
            if response.status_code == 200:
                rtn = response.json()
                rtn["lang"] = payload['lang']
                if rtn["status_code"] == 0:
                    rtn["status"] = 'success'
                    return True, rtn
                else:
                    rtn["status"] = 'error try lang-det'
                    return True, rtn
            else:
                return False, {}
        else:
            payload['lang'] = "none"
            response = transcriptRequest(url, json.dumps(payload))
            if response.status_code == 200:
                rtn = response.json()
                rtn["lang"] = payload['lang']
                if rtn["status_code"] == 0:
                    rtn["status"] = 'success'
                    return True, rtn
                else:
                    rtn["status"] = 'error try lang-none'
                    return True, rtn
            else:
                return False, {}
    else:
        return False, {}


def transcriptRequest(url, payload):
    response = requests.post(url=url, data=payload, headers={'Content-Type': 'application/json'}, timeout=(10, 600))
    return response


def transcribe(**config_prm):
    url = "http://192.168.255.6:6011/transcribe"
    s3_uncompressed = config_prm["transcribe_input_path"]
    s3_transcript = config_prm["transcribe_output_path"]
    s3_processed = config_prm["transcribe_processed"]
    bucket_name = config_prm["bucket_name"]
    param = {"data": "", "number_of_speakers": 2, "lang": "en","enable_post_process_segmentation": 1, "root_s3": "/home/deeplearningcv/s3_bucket/"}
    allfilepath = s3FileListing(bucket_name, s3_uncompressed, AUDIO_EXT)
    logs = []
    k = 0
    log = {"status": "", "count": 0, "status_code": "", "status_message": "", 'lang': "en","transcribed_detected_lang": "", "ext": "", "api_version": "", "confidence_score": "", "filepath": "", "json": ""}
    for path in allfilepath:
        print(path)
        k = k+1
        ext = (path.split("/")[-1]).split(".")[-1].lower()
        if ext == AUDIO_EXT:
            param["data"] = f"{S3PATH}{path}"
            json_param = json.dumps(param)
            try:
                response = transcriptRequest(url, json_param)
                if response.status_code == 200:
                    respObj = response.json()
                    # print(respObj)
                    if respObj["status_code"] == 0 and respObj["confidence_score"] >= 0.2:
                        log = {"status": 'success', "count": k, "status_code": respObj["status_code"], "status_message": respObj["status_message"], 'lang': param["lang"], "transcribed_detected_lang": respObj[
                            "transcribed_detected_lang"], "ext": ext, "api_version": respObj["api_version"], "confidence_score": respObj["confidence_score"], "filepath": param["data"]}
                        audio_filename = path.split("/")[-1]
                        filename = audio_filename.split(".wav")[0].strip()+".json"
                        prefix = f"{s3_transcript}json/{filename}"
                        log['json'] = storeResponse(bucket_name, prefix, respObj["transcribed_text"])  # json store
                        filename_txt = audio_filename.split(".wav")[0].strip()+".txt"
                        prefix_txt = f"{s3_transcript}txt/{filename_txt}"
                        storeResponse(bucket_name, prefix_txt,respObj["transcribed_text"])  # txt store
                        move_s3_object(bucket_name, path,f"{s3_processed}{audio_filename}") # move processed file
                        time.sleep(60)
                    else:
                        status, respObj = try_failure(response.json(), url, json_param)
                        print(respObj)
                        if status != False:
                            log = {"status": respObj["status"], "count": k, "status_code": respObj["status_code"], "status_message": respObj["status_message"], 'lang': respObj["lang"],
                                   "transcribed_detected_lang": respObj["transcribed_detected_lang"], "ext": ext, "api_version": respObj["api_version"], "confidence_score": respObj["confidence_score"], "filepath": param["data"]}
                            audio_filename = path.split("/")[-1]
                            filename = audio_filename.split(".wav")[0].strip()+".json"
                            prefix = f"{s3_transcript}json/{filename}"
                            log['json'] = storeResponse(bucket_name, prefix, respObj["transcribed_text"])  # json store
                            filename_txt = audio_filename.split(".wav")[0].strip()+".txt"
                            prefix_txt = f"{s3_transcript}txt/{filename_txt}"
                            storeResponse(bucket_name, prefix_txt,respObj["transcribed_text"])  # txt store
                            move_s3_object(bucket_name, path,f"{s3_processed}{audio_filename}") # move processed file
                            time.sleep(60)
                        else:
                            log = {"status": 'error', "count": k, "status_code": "try error", "status_message": "error", 'lang': "en",
                                   "transcribed_detected_lang": "", "ext": ext, "api_version": "", "confidence_score": "", "filepath": param["data"], "json": False}
                else:
                    log = {"status": 'error', "count": k, "status_code": response.status_code, "status_message": "", 'lang': "en",
                           "transcribed_detected_lang": "", "ext": ext, "api_version": "", "confidence_score": "", "filepath": param["data"], "json": False}

                time.sleep(60)
            except ConnectTimeout as e:
                log = {"status": 'error', "count": k, "status_code": 502, "status_message": "ConnectTimeout", 'lang': "en",
                       "transcribed_detected_lang": "", "ext": ext, "api_version": "", "confidence_score": "", "filepath": param["data"], "json": False}
            except ReadTimeout as e:
                log = {"status": 'error', "count": k, "status_code": 502, "status_message": "ReadTimeout", 'lang': "en",
                       "transcribed_detected_lang": "", "ext": ext, "api_version": "", "confidence_score": "", "filepath": param["data"], "json": False}
            except Exception as e:
                print(e)
                traceback_str = traceback.format_exc()
                print(traceback_str)
            finally:
                logs.append(log)
        else:
            log = {"status": "error", "count": k, "status_code": "", "status_message": "", 'lang': "en",
                   "transcribed_detected_lang": "", "ext": ext, "api_version": "", "confidence_score": "", "filepath": "", "json": False}
            logs.append(log)

        # if k==1:break

    if len(allfilepath) == 0:
        logs.append({"status": '0', "count": "0", "status_code": "0", "status_message": "No files", 'lang': "en",
                    "transcribed_detected_lang": "", "ext": "NILL", "api_version": "", "confidence_score": "", "filepath": "NILL"})

    print("\n******************************************************************Transcription Process Result*********************************************************")
    df = pd.DataFrame(logs)
    print(df)
    cdt = datetime.datetime.now()
    df_fn = "Maxwell_Transcription_"+cdt.strftime("%Y_%m_%d_%H%M%S")
    s3_key = f'{s3_transcript}{df_fn}.xlsx'
    df_to_excel_s3_upload(df, bucket_name, s3_key)
    print("\n*****************************************************************************************************************************************************")


# self call
trn_config = initTranscriptEngine()
transcribe(**trn_config)
