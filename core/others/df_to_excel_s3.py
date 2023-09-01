import boto3
import pandas as pd
from io import BytesIO

bucket_name="ixolerator-cloud"
s3 =  boto3.client('s3', aws_access_key_id="AKIATWZQTUP5P4QMO43H", aws_secret_access_key="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")
data = {
        "Name": ["John", "Alice", "Bob"],
        "Age": [30, 25, 28],
        "City": ["New York", "London", "Los Angeles"]
    }
s3_key = 'MeghnadAutomation/Supro_mini/Owner/Lot_2/getIdeas/data2.xlsx'
    
def df_to_excel_s3_upload(data,bucket_name,s3_key):
    df = pd.DataFrame(data)
    excel_buffer = BytesIO()
    sheet_name = 'Sheet1'
    df.to_excel(excel_buffer, index=False,sheet_name=sheet_name)
    excel_buffer.seek(0)
    s3.upload_fileobj(excel_buffer, bucket_name, s3_key)


df_to_excel_s3_upload(data,bucket_name,s3_key)