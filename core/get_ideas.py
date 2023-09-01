import json
import requests
import datetime
from requests.exceptions import ConnectTimeout
from requests.exceptions import ReadTimeout
import pandas as pd


from common import initGetIdeas,s3FileListing ,read_json_from_s3,storeResponse,df_to_excel_s3_upload
from constant import GET_IDEA_PAYLOAD

def get_ideas_api(**get_idea_config):  
        url=get_idea_config["get_idea_url"]
        bucket_name=get_idea_config["bucket_name"]
        # s3_transcript="MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Transcript/json/" 
        s3_transcript=get_idea_config["get_idea_input_path"]
        # s3_getideas="MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch1/Transcript/get_idea/"
        s3_getideas=get_idea_config["get_idea_output_path"]
        payload = GET_IDEA_PAYLOAD
        allfilepath = s3FileListing(bucket_name, s3_transcript,'json')        
        logs=[]
        k = 0
        log={"file name":"","status":"error","count":k,"status_code":200,"status_message": True,"ext":"json","api_version": "2.0.02","filepath":"",
         "company_brand_info_aliases_result":"","company_brand_info_company_brand_result":"","product_features_attributes":"","product_features_topics":""}
        for path in allfilepath:
            print(path)
            payload["data_parameters"]["data"]=read_json_from_s3(bucket_name,path)
            k= k+1
            ext=(path.split("/")[-1]).split(".")[-1].lower()
            if ext=="json":
                data_payload = json.dumps(payload)
                audio_filename = path.split("/")[-1]
                try:
                    response = requests.post(url=url, data=data_payload, headers={'Content-Type': 'application/json'},timeout=(10, 240))
                    if response.status_code == 200:
                        respObj = response.json()
                        if respObj["status_code"] == 0:
                            log={"file name":audio_filename,"status":"success","count":k,"status_code":respObj["status_code"],"status_message": respObj["status_message"],"ext":ext,"api_version": respObj["api_version"],"filepath":path,"company_brand_info_aliases_result":"","company_brand_info_company_brand_result":"","product_features_attributes":"","product_features_topics":""}
                            if "company_brand_info" in respObj:
                                if "aliases_result" in respObj['company_brand_info']:
                                    log['company_brand_info_aliases_result']=json.dumps(respObj['company_brand_info']['aliases_result'])
                                if "company_brand_result" in respObj['company_brand_info']:    
                                    log['company_brand_info_company_brand_result']=json.dumps(respObj['company_brand_info']['company_brand_result'])

                            if "product_features" in respObj:
                                if "attributes" in respObj['product_features']:
                                    log['product_features_attributes']=json.dumps(respObj['product_features']['attributes'])
                                if "topics" in respObj['product_features']:
                                    log['product_features_topics']=json.dumps(respObj['product_features']['topics'])

                            filename=audio_filename.split(".json")[0].strip()+".json"
                            prefix = f"{s3_getideas}{filename}"
                            storeResponse(bucket_name,prefix,respObj)
                        else:
                            log={"file name":audio_filename,"status":"error","count":k,"status_code":respObj["status_code"],"status_message": respObj["status_message"],"ext":ext,"api_version": respObj["api_version"],"filepath":path,"company_brand_info_aliases_result":"","company_brand_info_company_brand_result":"","product_features_attributes":"","product_features_topics":""}
                    else:
                        log={"file name":audio_filename,"status":"error","count":k,"status_code":response.status_code,"status_message": "","ext":ext,"api_version": "","filepath":"","company_brand_info_aliases_result":"","company_brand_info_company_brand_result":"","product_features_attributes":"","product_features_topics":""}
                except ConnectTimeout as e:
                    log={"file name":audio_filename,"status":"error","count":k,"status_code":502,"status_message": "ConnectTimeout","ext":ext,"api_version": "","filepath":"","company_brand_info_aliases_result":"","company_brand_info_company_brand_result":"","product_features_attributes":"","product_features_topics":""}
                except ReadTimeout as e:
                    log={"file name":audio_filename,"status":"error","count":k,"status_code":502,"status_message": "ReadTimeout","ext":ext,"api_version": "","filepath":"","company_brand_info_aliases_result":"","company_brand_info_company_brand_result":"","product_features_attributes":"","product_features_topics":""}
                except Exception as e:
                    print(e)   
                finally: 
                    logs.append(log)
            else:
                log={"file name":audio_filename,"status":"error","count":k,"status_code":"","status_message": "","ext":"","api_version": "","filepath":"","company_brand_info_aliases_result":"","company_brand_info_company_brand_result":"","product_features_attributes":"","product_features_topics":""}
                logs.append(log)     
  
            if k==1:break 

        if len(allfilepath)==0:
            logs.append(log)


    
        print("\n******************************************************************GetIdea Process Result*********************************************************") 
        
        df = pd.DataFrame(logs)
        print(df)
        cdt = datetime.datetime.now()
        df_fn = "getIdeas_summary_"+cdt.strftime("%Y_%m_%d_%H%M%S")
        s3_key = f'{s3_getideas=}{df_fn}.xlsx'
        df_to_excel_s3_upload(df,bucket_name,s3_key)

        print("\n*****************************************************************************************************************************************************")


get_idea_config=initGetIdeas()
get_ideas_api(**get_idea_config)