import json
import pandas as pd

with open(r'D:\s3\Supro_Owner_N6L56922.json', 'r') as json_file:
    data = json.load(json_file)
df = pd.DataFrame(data)
# print(df.head(3))

column_to_transpose = 'Questionnaire'
df.set_index(column_to_transpose, inplace=True)
transposed_df = df.transpose()
print(transposed_df)

excel_file_path = r'D:\s3\test.xlsx'
sheet_name = 'Sheet1'
transposed_df.to_excel(excel_file_path, index=False, sheet_name=sheet_name)


