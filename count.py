import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
from io import StringIO
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions
import xlwings as xw
from datetime import datetime
now = datetime.now()

dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

def count_file(db2, fileDir, cloud,count_fil):

    smain = count_fil
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    df_count = pd.DataFrame(
        columns=['TestCase_ID', 'DataMart_Name', 'Source_Table', 'Target_Table',
       'Source_Primary_Key', 'Target_Primary_Key', 'Number_of_columns',
       'Source_Record_count', 'Target_Record_count',
       'Testcase_Execution_timestamp', 'Data_Comparison_Result'])
    df_count_c = pd.DataFrame(
        columns=['TestCase_Id', 'Test_Type', 'source_tablename', 'Source_count',
                 'target_tablename', 'Target_count', 'Status'])
    for i in range(0, NR):
        y = i + 2
        start1_time = time.time()
        tid = int(smain.iloc[i]["Test Case ID"])
        datamart = smain.iloc[i]["Datamart"]
        print("Executing TestCaseID - " + tid)
        source_bucket= str(smain.iloc[i]["Source Bucket/Container Name"])
        source_file = str(smain.iloc[i]["Source File Name"])
        target_databasename = str(smain.iloc[i]["Target Database"])
        target_tablename = str(smain.iloc[i]["Target Table Name"])
        source_primary_key = str(smain.iloc[i]["Primary Source Column"])
        target_primary_key = str(smain.iloc[i]["Primary Target Column"])
        priority_column = smain.iloc[i]["Priority Column(Y/N)"]
        if priority_column == "Y":
            if(cloud=='s3'):
                print("no")
                obj = cloud.Bucket(source_bucket).Object(source_file).get()
                df_src = pd.read_csv(obj['Body'], index_col=0)
            else:
                print(cloud)
                print("hi")
                blob_client = cloud.get_blob_client(container=source_bucket, blob=source_file)
                stream = blob_client.download_blob()
                df_src = pd.read_csv(StringIO(stream.content_as_text()),header=None)
                df_src= df_src.astype('object')
                print(df_src.head())

            start = time.time()
            if source_bucket == "None":
                try:
                    Query2 = "select '" + target_tablename + "' as table_name , count(*) as cnt from " + target_tablename
                    df_tgt = pd.read_sql_query(Query2, db2)
                    df_tgt= df_tgt.astype('object')
                    add_row = [tid, 'Count_Check', "None", "None", target_tablename, df_tgt.iloc[0][1],
                               "None"]
                    df_count.loc[i] = add_row
                    df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)
                except Exception as e:
                    add_row = [tid, 'Count_Check', "None", "None",
                               target_tablename, np.nan, e]
                    df_count.loc[i] = add_row
                    df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)
            elif target_databasename == "None" or target_tablename == "None":
                print('2')
                try:
                    add_row = [tid, 'Count_Check', source_file, df_src.shape[0], "None", "None",
                               "None"]
                    df_count.loc[i] = add_row
                    df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)
                except Exception as e:
                    add_row = [tid, 'Count_Check', source_file, np.nan,
                               "None", "None", e]
                    df_count.loc[i] = add_row
                    df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)
            elif source_bucket != "None" and source_file != "None" and target_databasename != "None" and target_tablename != "None":
                print('3')
                try:
                    df_tgt = pd.read_sql_query(Query2, db2)
                    df_tgt= df_tgt.astype('object')
                    print(df_src)

                    if df_src.iloc[0][0] == df_tgt.iloc[0][0]:
                        add_row = [tid, datamart, source_file ,target_tablename, source_primary_key,
                                   target_primary_key, df_src.shape[1], df_src.shape[0],
                                   df_tgt.shape[0], dt_string, "match"]
                        df_count.loc[i] = add_row
                        df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx',
                                          index=False)
                        add_row_c = [tid, 'Count_Check', source_file, df_src.shape[0],
                                target_tablename, df_tgt.iloc[0][1], 'success']
                        df_count_c.loc[i] = add_row_c

                    else:
                        add_row = [tid, datamart, source_file ,target_tablename, source_primary_key,
                                   target_primary_key, df_src.shape[1], df_src.shape[0],
                                   df_tgt.shape[0], dt_string, "mismatch"]
                        df_count.loc[i] = add_row
                        df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx',
                                          index=False)
                        add_row_c = [tid, 'Count_Check', source_file, df_src.shape[0],
                                target_tablename, df_tgt.iloc[0][1], 'fail']
                        df_count_c.loc[i] = add_row_c

                        df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx',
                                          index=False)

                except Exception as e:
                    add_row = [tid, datamart, source_file ,target_tablename, source_primary_key,
                                   target_primary_key, df_src.shape[1], df_src.shape[0],
                                   df_tgt.shape[0], dt_string, e]
                    df_count.loc[i] = add_row
                    df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)


            end = time.time()
    print("Time: ", end - start)
    #return df_count
    return df_count_c

def count(db1, db2, fileDir,countt):
    
    smain = countt
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    headers = ['Test Case Id', 'Test Type', 'Status', 'Source Count', 'Target Count']
    df_count = pd.DataFrame(
        columns=['TestCase_ID', 'DataMart_Name', 'Source_Table', 'Target_Table',
       'Source_Primary_Key', 'Target_Primary_Key', 'Number_of_columns',
       'Source_Record_count', 'Target_Record_count',
       'Testcase_Execution_timestamp', 'Data_Comparison_Result'])
    df_count_c = pd.DataFrame(
        columns=['TestCase_Id', 'Test_Type', 'source_tablename', 'Source_count',
                 'target_tablename', 'Target_count', 'Status'])


    for i in range(0, NR):
        y = i + 2
        start1_time = time.time()
        tid = smain.iloc[i]["Test Case ID"]
        print("Executing TestCaseID - " + tid)
        datamart = str(smain.iloc[i]["Datamart"])
        source_databasename = str(smain.iloc[i]["Source DataBase"])
        source_tablename = str(smain.iloc[i]["Source Table Name"])
        target_databasename = str(smain.iloc[i]["Target Database"])
        target_tablename = str(smain.iloc[i]["Target Table Name"])
        source_primary_key = str(smain.iloc[i]["Primary Source Column"])
        target_primary_key = str(smain.iloc[i]["Primary Target Column"])
        priority_column = smain.iloc[i]["Priority Column(Y/N)"]
        if priority_column == "Y":
            start = time.time()
            if source_databasename == "None" or source_tablename == "None":
                try:
                    Query2 = "select count(*) as cnt from " + target_tablename
                    df_tgt = pd.read_sql_query(Query2, db2)
                    add_row = [tid, 'Count_Check', "None", "None", target_tablename, df_tgt.iloc[0][1],
                               "None"]
                    
                    df_count.loc[i] = add_row
                    df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)
                except Exception as e:
                    add_row = [tid, 'Count_Check', source_tablename, np.nan,
                                   target_tablename, np.nan, e]
                    df_count.loc[i] = add_row
                    df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)

                    #flash('Unable to find the table in Database. Please! Give Correct Target Details')
                    return render_template("home.html")
            elif target_databasename == "None" or target_tablename == "None":
                print('2')
                try:
                    Query1 = "select count(*) as cnt from " + source_tablename
                    df_src = pd.read_sql_query(Query1, db1)
                    df_src= df_src.astype('object')
                    add_row = [tid, 'Count_Check', source_tablename, df_src.iloc[0][1], "None", "None",
                               "None"]
                    df_count.loc[i] = add_row
                    df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)
                except Exception as e:
                        add_row = [tid, 'Count_Check', source_tablename, np.nan,
                                   target_tablename, np.nan, e]
                        df_count.loc[i] = add_row
                        df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)
            elif source_databasename != "None" and source_tablename != "None" and target_databasename != "None" and target_tablename != "None":
                print('3')
                try:
                    Query1 = "SELECT * FROM " + source_tablename
                    Query2 = "SELECT * FROM " + target_tablename
                    df_tgt = pd.read_sql_query(Query2, db2)
                    print(df_tgt)
                    df_tgt=df_tgt.astype('object')
                    df_src = pd.read_sql_query(Query1, db1)
                    
                    if df_src.shape[0] == df_tgt.shape[0]:
                        add_row = [tid, datamart, source_tablename ,target_tablename, source_primary_key,
                                   target_primary_key, df_src.shape[1], df_src.shape[0],
                                   df_tgt.shape[0], dt_string, "match"]
                        
                        df_count.loc[i] = add_row
                        add_row_c = [tid, 'Count_Check', source_tablename, df_src.shape[0],
                                target_tablename, df_tgt.iloc[0][1], 'success']
                        df_count_c.loc[i] = add_row_c

                        df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx',
                                          index=False)
                    else:
                        add_row = [tid, datamart, source_tablename ,target_tablename, source_primary_key,
                                   target_primary_key, df_src.shape[1], df_src.shape[0],
                                   df_tgt.shape[0], dt_string, "mismatch"]
                        df_count.loc[i] = add_row
                        add_row_c = [tid, 'Count_Check', source_tablename, df_src.shape[0],
                                target_tablename, df_tgt.iloc[0][1], 'fail']
                        df_count_c.loc[i] = add_row_c

                        df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx',
                                          index=False)
                except Exception as e:
                    add_row = [tid, datamart, source_tablename ,target_tablename, source_primary_key,
                                   target_primary_key, df_src.shape[1], df_src.shape[0],
                                   df_tgt.shape[0], dt_string, e]
                    df_count.loc[i] = add_row
                    df_count.to_excel(fileDir + '\\' + 'Report_for_Count_Check' + '.xlsx', index=False)

            end = time.time()
    print("Time: ", end - start)
    print(len(df_count))
   # return df_count
    return df_count_c