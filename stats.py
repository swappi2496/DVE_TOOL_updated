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

def stat_file(db2, fileDir, cloud,stat_fil,file_name):
    zip_list = []
    smain = stat_fil
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    headers = ['Test Case Id', 'Test Type', 'Status', 'Source Count', 'Target Count']
    df_report = pd.DataFrame(columns=headers)
    df_report1 = pd.DataFrame(
        columns=['TestId', 'Check Type', 'Source Tablename', 'Status', 'Target Tablename', 'Status'])
    df_stats = pd.DataFrame(columns=['Test CaseId', 'Test_Type', 'Source TableName', 'Target TableName', 'Status'])
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
                    df_src = pd.read_csv(StringIO(stream.content_as_text()))
                    print(df_src.head())
                start = time.time()
                if source_bucket == "None" or source_file == "None":
                    try:
                        Query2 = "select * from " + target_tablename + ";"
                        df_tgt = pd.read_sql_query(Query2, db2)
                        # df_tgt['index'] = df[target_tablename]
                        df_tgt = df_tgt.describe()
                        df_tgt = df_tgt.drop(['25%'], axis=0)
                        df_tgt = df_tgt.drop(['50%'], axis=0)
                        df_tgt = df_tgt.drop(['75%'], axis=0)
                        df_tgt.index.name = target_tablename
                        add_row = [tid, "Stats_Check", "None", target_tablename, "None"]
                        df_stats.loc[i] = add_row
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_stats' + '.txt', "w")
                        print('Target', file=f)
                        print(df_tgt.round(2), file=f)
                        f.close()
                        # df_tgt.to_excel(fileDir +'\\'+'Report for Statistics'+'.xlsx')
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                elif target_databasename == "None" or target_tablename == "None":
                    try:
                        # Query1 = "select * from " + source_databasename + "." + source_tablename + ";"
                        # df_src = pd.read_sql_query(Query1, db1)
                        df_src = df_src.describe()
                        df_src = df_src.drop(['25%'], axis=0)
                        df_src = df_src.drop(['50%'], axis=0)
                        df_src = df_src.drop(['75%'], axis=0)
                        df_src.index.name = source_file
                        add_row = [tid, "Stats_Check", source_file, "None", "None"]
                        df_stats.loc[i] = add_row
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_stats' + '.txt', "w")
                        print('Source', file=f)
                        print(df_src.round(2), file=f)
                        f.close()
                        # df_src.to_excel(fileDir +'\\'+'Report for Statistics'+'.xlsx')
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                else:
                    try:
                        print('3')
                        #Query1 = "select * from " + source_databasename + "." + source_tablename + ";"
                        Query2 = "select * from " + target_tablename + ";"
                        #df_src = pd.read_sql_query(Query1, db1)
                        df_tgt = pd.read_sql_query(Query2, db2)
                        df_src = df_src.describe()
                        df_src = df_src.drop(['25%'], axis=0)
                        df_src = df_src.drop(['50%'], axis=0)
                        df_src = df_src.drop(['75%'], axis=0)
                        df_tgt = df_tgt.describe()
                        df_tgt = df_tgt.drop(['25%'], axis=0)
                        df_tgt = df_tgt.drop(['50%'], axis=0)
                        df_tgt = df_tgt.drop(['75%'], axis=0)
                        print("df_src= ",df_src,"df_tgt=", df_tgt)
                        df = df_src == df_tgt
                        print(df)
                        if df[(df.values.ravel() == False).reshape(df.shape).any(1)].shape[0] > 0:
                            print('Yes')
                            add_row = [tid, 'Stats Check', source_file, target_tablename, 'Fail']
                            df_stats.loc[i] = add_row
                            df_stats.to_excel(fileDir + '\\' + 'Report for Statistics' + '.xlsx', index=False)
                        else:
                            print('No')
                            add_row = [tid, 'Stats Check', source_file, target_tablename, 'Success']
                            df_stats.loc[i] = add_row
                            print(df_stats)
                            df_stats.to_excel(fileDir + '\\' + 'Report for Statistics' + '.xlsx', index=False)
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_stats' + '.txt', "w")
                        print('Source', file=f)
                        print(df_src.round(2), file=f)
                        print('\n', file=f)
                        print('Target', file=f)
                        print(df_tgt.round(2), file=f)
                        f.close()
                        file_name.append("Report for Statistics.xlsx")
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    with ZipFile('Statistic_Check_Textfiles.zip', 'w') as zipObj3:
        for z,l in zip(range(0,NR),zip_list):
            priority_column = smain.iloc[z]["Priority Column(Y/N)"]
            if priority_column == "Y":
                filename_stats = str(l) + "_stats.txt"
                zipObj3.write(filename_stats)
    return df_stats

def stat(db1, db2, fileDir, statt,file_name):
    smain = statt
    zip_list = []
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    headers = ['Test Case Id', 'Test Type', 'Status', 'Source Count', 'Target Count']
    df_report = pd.DataFrame(columns=headers)
    df_report1 = pd.DataFrame(
        columns=['TestId', 'Check Type', 'Source Tablename', 'Status', 'Target Tablename', 'Status'])
    df_stats = pd.DataFrame(columns=['Test CaseId', 'Test_Type', 'Source TableName', 'Target TableName', 'Status'])
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
                        # df_tgt['index'] = df[target_tablename]
                        df_tgt = df_tgt.describe()
                        df_tgt = df_tgt.drop(['25%'], axis=0)
                        df_tgt = df_tgt.drop(['50%'], axis=0)
                        df_tgt = df_tgt.drop(['75%'], axis=0)
                        df_tgt.index.name = target_tablename
                        add_row = [tid, "Stats_Check", "None", target_tablename, "None"]
                        df_stats.loc[i] = add_row
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_stats' + '.txt', "w")
                        print('Target', file=f)
                        print(df_tgt.round(2), file=f)
                        f.close()
                        # df_tgt.to_excel(fileDir +'\\'+'Report for Statistics'+'.xlsx')
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                elif target_databasename == "None" or target_tablename == "None":
                    try:
                        Query1 = "select * from "  + source_tablename + ";"
                        df_src = pd.read_sql_query(Query1, db1)
                        df_src = df_src.describe()
                        df_src = df_src.drop(['25%'], axis=0)
                        df_src = df_src.drop(['50%'], axis=0)
                        df_src = df_src.drop(['75%'], axis=0)
                        df_src.index.name = source_tablename
                        add_row = [tid, "Stats_Check", source_tablename, "None", "None"]
                        df_stats.loc[i] = add_row
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_stats' + '.txt', "w")
                        print('Source', file=f)
                        print(df_src.round(2), file=f)
                        f.close()
                        # df_src.to_excel(fileDir +'\\'+'Report for Statistics'+'.xlsx')
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                else:
                    try:
                        print('3')
                        Query1 = "select * from " + source_tablename + ";"
                        Query2 = "select * from " + target_tablename + ";"
                        df_src = pd.read_sql_query(Query1, db1)
                        df_tgt = pd.read_sql_query(Query2, db2)
                        df_src = df_src.describe()
                        df_src = df_src.drop(['25%'], axis=0)
                        df_src = df_src.drop(['50%'], axis=0)
                        df_src = df_src.drop(['75%'], axis=0)
                        df_tgt = df_tgt.describe()
                        df_tgt = df_tgt.drop(['25%'], axis=0)
                        df_tgt = df_tgt.drop(['50%'], axis=0)
                        df_tgt = df_tgt.drop(['75%'], axis=0)
                        print(df_src, df_tgt)
                        df = df_src == df_tgt
                        print(df)
                        if df[(df.values.ravel() == False).reshape(df.shape).any(1)].shape[0] > 0:
                            print('Yes')
                            add_row = [tid, 'Stats Check', source_tablename, target_tablename, 'Fail']
                            df_stats.loc[i] = add_row
                            df_stats.to_excel(fileDir + '\\' + 'Report for Statistics' + '.xlsx', index=False)
                        else:
                            print('No')
                            add_row = [tid, 'Stats Check', source_tablename, target_tablename, 'Success']
                            df_stats.loc[i] = add_row
                            print(df_stats)
                            df_stats.to_excel(fileDir + '\\' + 'Report for Statistics' + '.xlsx', index=False)
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        zip_list.append(tid)
                        f = open(fileDir + '\\' + str(tid) + '_stats' + '.txt', "w")
                        print('Source', file=f)
                        print(df_src.round(2), file=f)
                        print('\n', file=f)
                        print('Target', file=f)
                        print(df_tgt.round(2), file=f)
                        f.close()
                        file_name.append("Report for Statistics.xlsx")
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    with ZipFile('Statistic_Check_Textfiles.zip', 'w') as zipObj3:
        for z,l in zip(range(0,NR),zip_list):
            priority_column = smain.iloc[z]["Priority Column(Y/N)"]
            if priority_column == "Y":
                filename_stats = str(l) + "_stats.txt"
                zipObj3.write(filename_stats)
    return df_stats