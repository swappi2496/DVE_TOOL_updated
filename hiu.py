
from io import StringIO
from azure.storage.blob import BlobServiceClient
import pandas as pd
#import snowflake.connector as sc
import datacompy
conn_str = "DefaultEndpointsProtocol=https;AccountName=datavalidationblob;AccountKey=2J8+dzIqeZBCCqInVCDzoNs6FsSRLZfqlz+xFpmufuDcBHF7nqsY9MK8nrlusFumP0VnpRmwIzx56BFki4EKgw==;EndpointSuffix=core.windows.ne"
container = 'testcontainer'
blob_name = "1sheet/1sheet.csv"

container_client = BlobServiceClient(account_url="https://datavalidationblob.blob.core.windows.net", credential="?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2022-04-30T15:52:24Z&st=2022-02-10T07:52:24Z&spr=https&sig=veUIwKunWRC5yVL%2F9jp3dnfXdpC9ODOkUh0vXUrp9R0%3D")
blob_client = container_client.get_blob_client(container=container, blob=blob_name)
stream = blob_client.download_blob()
df_src = pd.read_csv(StringIO(stream.content_as_text()))
print(df_src.head())
# db2 = sc.connect(account='je93546.switzerland-north.azure', user='devsingh01', password='Qwerty123')
# df1 = pd.read_sql_query("select * from test.public.na_store_data",db2)
# compare = datacompy.Compare(df_src, df1, join_columns="ID",)
# if not compare.matches(ignore_extra_columns=False):
#     print("yes")
# else:
#     print('done')



