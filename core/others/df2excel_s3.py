import pandas as pd
import json

rsp={
    "api_release_date": "2023.07.14",
    "api_version": "2.0.0",
    "company_brand_info": {
        "aliases_result": {
            "Ashok Leyland": [
                "Ashok",
                "Ashok's"
            ],
            "Mahindra": [
                "Mahendra"
            ],
            "Supro": [
                "Supra"
            ]
        },
        "company_brand_result": {
            "ashok leyland": [
                "truck"
            ],
            "mahindra": [
                "supro",
                "profit truck"
            ]
        }
    },
    "product_features": {
        "attributes": [
            "diesel car",
            "Ashok Leyland",
            "recommendation",
            "improvement",
            "TM",
            "kilometers",
            "VX variant",
            "dumper",
            "business",
            "kilograms",
            "Uday program",
            "top model",
            "commercial car",
            "Supra Mini Profit Truck",
            "loading capacity",
            "mileage average",
            "Supro Mini Profit Truck",
            "Mahindra",
            "driver",
            "profit truck",
            "load"
        ],
        "topics": [
            "Requirements for buying a new car",
            "Kilometers driven per day and location of usage",
            "Decision making process for buying the car",
            "Recommendation of the car to others",
            "Usage and load capacity of the Supro Mini Profit Truck",
            "Comparison with other cars in the market",
            "Experience and satisfaction with the car",
            "Choice of variant and fuel type",
            "Opinion about Mahindra as a commercial vehicle manufacturer",
            "Other commercial vehicles owned by the customer",
            "Reasons for considering Ashok Leyland's car",
            "Reasons for buying the Supro Mini Profit Truck",
            "Awareness of Mahindra's Uday program"
        ]
    },
    "status_code": 0,
    "status_message": "Success."
}

logs=[]
for k in range(1,11):
    log={"file name":"","status":"success","count":k,"status_code":200,"status_message": True,"ext":"json","api_version": "2.0.02","filepath":"",
         "company_brand_info_aliases_result":rsp,"company_brand_info_company_brand_result":"","product_features_attributes":"","product_features_topics":""}
    if "company_brand_info" in rsp:
        if "aliases_result" in rsp['company_brand_info']:
            log['company_brand_info_aliases_result']=json.dumps(rsp['company_brand_info']['aliases_result'])
        if "company_brand_result" in rsp['company_brand_info']:    
            log['company_brand_info_company_brand_result']=json.dumps(rsp['company_brand_info']['company_brand_result'])

    if "product_features" in rsp:
        if "attributes" in rsp['product_features']:
            log['product_features_attributes']=json.dumps(rsp['product_features']['attributes'])
        if "topics" in rsp['product_features']:
            log['product_features_topics']=json.dumps(rsp['product_features']['topics'])
    
    logs.append(log)


df = pd.DataFrame(logs)
print(df)

#local save

# excel_file_path = r'D:\s3\data.xlsx'
# sheet_name = 'Sheet1'
# df.to_excel(excel_file_path, index=False, sheet_name=sheet_name)

#s3 upload
from common import df_to_excel_s3_upload
s3_key = 'MeghnadAutomation/Supro_mini/Owner/Lot_2/getIdeas/data3.xlsx'
df_to_excel_s3_upload(df,"ixolerator-cloud",s3_key)





