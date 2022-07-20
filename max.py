import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import datacompy
import numpy as np
from io import StringIO
from zipfile import ZipFile
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions
import xlwings as xw
def max(db1, db2, fileDir, maxx,file_name) :
    zip_list = []
    smain = maxx
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    headers = ['Test Case Id', 'Test Type', 'Status', 'Source Count', 'Target Count']
    df_report = pd.DataFrame(columns=headers)
    df_report1 = pd.DataFrame(
        columns=['TestId', 'Check Type', 'Source Tablename', 'Status', 'Target Tablename', 'Status'])
    df_max = pd.DataFrame(columns=['Test_CaseId', 'Test_Type', 'Source_TableName', 'Target_TableName', 'Status'])
    try:
        for i in range(0, NR):
            y = i + 2
            start1_time = time.time()
            tid = smain.iloc[i]["Test Case ID"]
            print("Executing TestCaseID - " + tid)
            # ttype = smain.iloc[i]["Test CaseType"]
            source_databasename = str(smain.iloc[i]["Source DataBase"])
            source_tablename = str(smain.iloc[i]["Source Table Name"])
            target_databasename = str(smain.iloc[i]["Target Database"])
            target_tablename = str(smain.iloc[i]["Target Table Name"])
            priority_column = smain.iloc[i]["Priority Column(Y/N)"]
            if priority_column == "Y":
                start = time.time()
                if source_databasename == "None" or source_tablename == "None":
                    try:
                        Query2 = "select * from " + target_tablename + ";"
                        df_tgt = pd.read_sql_query(Query2, db2)
                        tmaxm = df_tgt.min(numeric_only=True)
                        add_row = [tid, "Max_Check", "None", target_tablename, "None"]
                        df_max.loc[i] = add_row
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_max' + '.txt', "w")
                        print('Target', file=f)
                        print(tmaxm.round(2), file=f)
                        f.close()
                        # df_tgt.to_excel(fileDir +'\\'+'Report for Max. Value'+'.xlsx')
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                elif target_databasename == "None" or target_tablename == "None":
                    try:
                        Query1 = "select * from " + source_tablename + ";"
                        df_src = pd.read_sql_query(Query1, db1)
                        smaxm = df_src.max(numeric_only=True)
                        add_row = [tid, "Max_Check", source_tablename, "None", "None"]
                        df_max.loc[i] = add_row
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_max' + '.txt', "w")
                        print('Source', file=f)
                        print(smaxm.round(2), file=f)
                        f.close()
                        # df_src.to_excel(fileDir +'\\'+'Report for Max. Value'+'.xlsx')
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                else:
                    try:
                        Query1 = "select * from " + source_tablename + ";"
                        Query2 = "select * from " + target_tablename + ";"
                        df_src = pd.read_sql_query(Query1, db1)
                        df_tgt = pd.read_sql_query(Query2, db2)
                        smaxm = pd.DataFrame(df_src.max(numeric_only=True))
                        tmaxm = pd.DataFrame(df_tgt.max(numeric_only=True))
                        df_maxx = pd.concat([smaxm, tmaxm], axis=1)
                        df_maxx.columns = ['Source', 'Target']
                        df_maxx['Status'] = np.where(df_maxx['Source'] == df_maxx['Target'], 'Success', 'Fail')
                        if df_maxx['Status'][df_maxx['Status']=='Fail'].count()>0:
                            add_row = [tid, 'Stats_Max Check', source_tablename, target_tablename, 'Fail']
                            df_max.loc[i] = add_row
                            df_max.to_excel(fileDir + '\\' + 'Report for Max. Value' + '.xlsx', index=False)

                        else:
                            add_row = [tid, 'Stats_Max Check', source_tablename, target_tablename, 'Success']
                            df_max.loc[i] = add_row
                            df_max.to_excel(fileDir + '\\' + 'Report for Max. Value' + '.xlsx', index=False)
                        zip_list.append(tid)
                        df_maxx.round(2).to_excel(fileDir + '\\' + str(tid) + 'Report for Max. Value' + '.xlsx',
                                                  index=True)
                        file_name.append("Report for Max. Value.xlsx")
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    with ZipFile('Max_Check_Report.zip', 'w') as zipObj3:
        for z,l in zip(range(0,NR),zip_list):
            priority_column = smain.iloc[z]["Priority Column(Y/N)"]
            if priority_column == "Y":
                filename_max = str(l) + "Report for Max. Value.xlsx"
                zipObj3.write(filename_max)
    return df_max

def max_file(db2, fileDir, cloud,max_fil,file_name):
    zip_list = []
    smain = max_fil
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    headers = ['Test Case Id', 'Test Type', 'Status', 'Source Count', 'Target Count']
    df_report = pd.DataFrame(columns=headers)
    df_report1 = pd.DataFrame(
        columns=['TestId', 'Check Type', 'Source Tablename', 'Status', 'Target Tablename', 'Status'])
    df_max = pd.DataFrame(columns=['Test_CaseId', 'Test_Type', 'Source_TableName', 'Target_TableName', 'Status'])
    try:
        for i in range(0, NR):
            y = i + 2
            start1_time = time.time()
            tid = smain.iloc[i]["Test Case ID"]
            print("Executing TestCaseID - " + tid)
            # ttype = smain.iloc[i]["Test CaseType"]
            source_bucket = str(smain.iloc[i]["Source Bucket/Container Name"])
            source_file = str(smain.iloc[i]["Source File Name"])
            target_databasename = str(smain.iloc[i]["Target Database"])
            target_tablename = str(smain.iloc[i]["Target Table Name"])
            Join_columns = str(smain.iloc[i]['Primary Column'])
            priority_column = smain.iloc[i]["Priority Column(Y/N)"]
            if priority_column == "Y":
                if (cloud == 's3'):
                    print("no")
                    obj = cloud.Bucket(source_bucket).Object(source_file).get()
                    df_src = pd.read_csv(obj['Body'], index_col=0)
                else:
                    print(cloud)
                    blob_client = cloud.get_blob_client(container=source_bucket, blob=source_file)
                    stream = blob_client.download_blob()
                    df_src = pd.read_csv(StringIO(stream.content_as_text()),header=None)
                    print(df_src.head())
                start = time.time()
                if source_bucket == "None" or source_file == "None":
                    try:
                        Query2 = "select * from " + target_tablename + ";"
                        df_tgt = pd.read_sql_query(Query2, db2)
                        tmaxm = df_tgt.min(numeric_only=True)
                        add_row = [tid, "Max_Check", "None", target_tablename, "None"]
                        df_max.loc[i] = add_row
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_max' + '.txt', "w")
                        print('Target', file=f)
                        print(tmaxm.round(2), file=f)
                        f.close()
                        # df_tgt.to_excel(fileDir +'\\'+'Report for Max. Value'+'.xlsx')
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                elif target_databasename == "None" or target_tablename == "None":
                    try:
                        Query1 = "select * from " + source_file + ";"
                        #df_src = pd.read_sql_query(Query1, db1)
                        smaxm = df_src.max(numeric_only=True)
                        add_row = [tid, "Max_Check", source_file, "None", "None"]
                        df_max.loc[i] = add_row
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_max' + '.txt', "w")
                        print('Source', file=f)
                        print(smaxm.round(2), file=f)
                        f.close()
                        # df_src.to_excel(fileDir +'\\'+'Report for Max. Value'+'.xlsx')
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                else:
                    try:
                        Query1 = "select * from " + source_file + ";"
                        Query2 = "select * from " + target_tablename + ";"
                        #df_src = pd.read_sql_query(Query1, db1)
                        df_tgt = pd.read_sql_query(Query2, db2)
                        df_src.columns= df_tgt.columns
                        smaxm = df_src.max(numeric_only=True)
                        tmaxm = df_tgt.max(numeric_only=True)
                        df_maxx = pd.concat([smaxm, tmaxm], axis=1)
                        df_maxx.columns = ['Source', 'Target']
                        df_maxx['Status'] = np.where(df_maxx['Source'] == df_maxx['Target'], 'Success', 'Fail')
                        if df_maxx['Status'][df_maxx['Status'] == 'Fail'].count() > 0:
                            add_row = [tid, 'Stats_Max Check', source_file, target_tablename, 'Fail']
                            df_max.loc[i] = add_row
                            df_max.to_excel(fileDir + '\\' + 'Report for Max. Value' + '.xlsx', index=False)
                        else:
                            add_row = [tid, 'Stats_Max Check', source_file, target_tablename, 'Success']
                            df_max.loc[i] = add_row
                            df_max.to_excel(fileDir + '\\' + 'Report for Max. Value' + '.xlsx', index=False)
                        zip_list.append(tid)
                        df_maxx.round(2).to_excel(fileDir + '\\' + str(tid) + 'Report for Max. Value' + '.xlsx',
                                                  index=True)
                        file_name.append("Report for Max. Value.xlsx")
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    with ZipFile('Max_Check_Report.zip', 'w') as zipObj3:
        for z, l in zip(range(0, NR), zip_list):
            priority_column = smain.iloc[z]["Priority Column(Y/N)"]
            if priority_column == "Y":
                filename_max = str(l) + "Report for Max. Value.xlsx"
                zipObj3.write(filename_max)
    return df_max

