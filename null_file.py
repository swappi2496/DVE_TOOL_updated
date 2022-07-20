import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
import xlwings as xw
from io import StringIO
def null_file(db2, fileDir,cloud, null_fil):
    smain = null_fil
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    df_null = pd.DataFrame(
        columns=['TestCase_Id', 'Test_Type', 'Source_File_Name', 'Source_Null_Count', 'Source_Validation',
                 'Target_Table_Name', 'Target_Null_Count', 'Target_validation', 'Status'])
    try:
        # db1=teradatasql.connect(host=hostname, user=userconnect, password=password)
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
            primary = str(smain.iloc[i]["Primary Column"])
            priority_column = smain.iloc[i]["Priority Column(Y/N)"]
            if priority_column == "Y":
                if(cloud=='s3'):
                    obj = cloud.Bucket(source_bucket).Object(source_file).get()
                    df_src = pd.read_csv(obj['Body'], index_col=0)
                else:
                    blob_client = cloud.get_blob_client(container=source_bucket, blob=source_file)
                    stream = blob_client.download_blob()
                    df_src = pd.read_csv(StringIO(stream.content_as_text()),header=None)
                start = time.time()
                if source_bucket == "None":
                    try:
                        Query2 = "select * from " + target_tablename + ";"
                        df_tgt = pd.read_sql_query(Query2, db2)
                        if df_tgt.isnull().all(axis=1).sum() == 0:
                            add_row = [tid, 'Null_Check', "None", "None", "None", target_tablename,
                                       df_tgt.isnull().all(axis=1).sum(), 'Success', 'Success']
                        else:
                            add_row = [tid, 'Null_Check', "None", "None", "None", target_tablename,
                                       df_tgt.isnull().all(axis=1).sum(), 'Fail', 'Fail']
                        df_null.loc[i] = add_row
                        df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                elif target_databasename == "None" or target_tablename == "None":
                    try:
                        if df_src.isnull().all(axis=1).sum() == 0:
                            add_row = [tid, 'Null_Check', source_file, df_src.isnull().all(axis=1).sum(),
                                       'Success', "None", "None", "None", 'Success']
                        else:
                            add_row = [tid, 'Null_Check', source_file, df_src.isnull().all(axis=1).sum(), 'Fail',
                                       "None", "None", "None", 'Fail']
                        df_null.loc[i] = add_row
                        df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                else:
                    try:
                        Query2 = "select * from " + target_tablename + ";"
                        try:
                            df_tgt = pd.read_sql_query(Query2, db2)
                            print(df_tgt)
                            df_src.columns = df_tgt.columns
                            if df_src.isnull().all(axis=1).sum() == df_tgt.isnull().all(axis=1).sum() == 0:
                                add_row = [tid, 'Null_Check', source_file, df_src.isnull().all(axis=1).sum(),
                                           'Success', target_tablename, df_tgt.isnull().all(axis=1).sum(), 'Success',
                                           'Success']
                                df_null.loc[i] = add_row
                                df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                            elif df_src.isnull().all(axis=1).sum() == 0 and df_tgt.isnull().all(axis=1).sum() != 0:
                                add_row = [tid, 'Null_Check', source_file, df_src.isnull().all(axis=1).sum(),
                                           'Success', target_tablename, df_tgt.isnull().all(axis=1).sum(), 'Fail',
                                           'Fail']
                                df_null.loc[i] = add_row
                                df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                            elif df_src.isnull().all(axis=1).sum() != 0 and df_tgt.isnull().all(axis=1).sum() == 0:
                                add_row = [tid, 'Null_Check', source_file, df_src.isnull().all(axis=1).sum(),
                                           'Fail', target_tablename, df_tgt.isnull().all(axis=1).sum(), 'Success',
                                           'Fail']
                                df_null.loc[i] = add_row
                                df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                            else:
                                add_row = [tid, 'Null_Check', source_file, df_src.isnull().all(axis=1).sum(),
                                           'Fail', target_tablename, df_tgt.isnull().all(axis=1).sum(), 'Fail', 'Fail']
                                df_null.loc[i] = add_row
                                df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                        except Exception as e:
                            flash(e)
                            return render_template("home.html")
                    except:
                        flash('Unable to find the table in Database. Please! Give the correct Details')
                        return render_template("home.html")
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    return df_null

def null(db1, db2, fileDir,nulll):
    smain = nulll
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    headers = ['Test Case Id', 'Test Type', 'Status', 'Source Count', 'Target Count']
    df_report = pd.DataFrame(columns=headers)
    df_report1 = pd.DataFrame(
        columns=['TestId', 'Check Type', 'Source Tablename', 'Status', 'Target Tablename', 'Status'])
    a, b = False, False
    df_null = pd.DataFrame(
        columns=['TestCase_Id', 'Test_Type', 'Souce_Table_Name', 'Source_Null_Count', 'Source_Validation',
                 'Target_Table_Name', 'Target_Null_Count', 'Target_validation', 'Status'])
    try:
        # db1=teradatasql.connect(host=hostname, user=userconnect, password=password)
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
                        Query2 = "select * from "+ target_tablename + ";"
                        df_tgt = pd.read_sql_query(Query2, db2)
                        if df_tgt.isnull().all(axis=1).sum() == 0:
                            add_row = [tid, 'Null_Check', "None", "None", "None", target_tablename,
                                       df_tgt.isnull().all(axis=1).sum(), 'Success', 'Success']
                        else:
                            add_row = [tid, 'Null_Check', "None", "None", "None", target_tablename,
                                       df_tgt.isnull().all(axis=1).sum(), 'Fail', 'Fail']

                        df_null.loc[i] = add_row
                        df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                elif target_databasename == "None" or target_tablename == "None":
                    try:
                        Query1 = "select * from " + source_tablename + ";"
                        df_src = pd.read_sql_query(Query1, db1)
                        if df_src.isnull().all(axis=1).sum() == 0:
                            add_row = [tid, 'Null_Check', source_tablename, df_src.isnull().all(axis=1).sum(),
                                       'Success', "None", "None", "None", 'Success']
                        else:
                            add_row = [tid, 'Null_Check', source_tablename, df_src.isnull().all(axis=1).sum(), 'Fail',
                                       "None", "None", "None", 'Fail']
                        df_null.loc[i] = add_row
                        df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                else:
                    try:
                        Query1 = "select * from "  + source_tablename + ";"
                        Query2 = "select * from " + target_tablename + ";"
                        try:
                            df_tgt = pd.read_sql_query(Query2, db2)
                            df_src = pd.read_sql_query(Query1, db1)
                            print(df_tgt)
                            if df_src.isnull().all(axis=1).sum() == df_tgt.isnull().all(axis=1).sum() == 0:
                                add_row = [tid, 'Null_Check', source_tablename, df_src.isnull().all(axis=1).sum(),
                                           'Success', target_tablename, df_tgt.isnull().all(axis=1).sum(), 'Success',
                                           'Success']
                                df_null.loc[i] = add_row
                                df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                            elif df_src.isnull().all(axis=1).sum() == 0 and df_tgt.isnull().all(axis=1).sum() != 0:
                                add_row = [tid, 'Null_Check', source_tablename, df_src.isnull().all(axis=1).sum(),
                                           'Success', target_tablename, df_tgt.isnull().all(axis=1).sum(), 'Fail',
                                           'Fail']
                                df_null.loc[i] = add_row
                                df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                            elif df_src.isnull().all(axis=1).sum() != 0 and df_tgt.isnull().all(axis=1).sum() == 0:
                                add_row = [tid, 'Null_Check', source_tablename, df_src.isnull().all(axis=1).sum(),
                                           'Fail', target_tablename, df_tgt.isnull().all(axis=1).sum(), 'Success',
                                           'Fail']
                                df_null.loc[i] = add_row
                                df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                            else:
                                add_row = [tid, 'Null_Check', source_tablename, df_src.isnull().all(axis=1).sum(),
                                           'Fail', target_tablename, df_tgt.isnull().all(axis=1).sum(), 'Fail', 'Fail']
                                df_null.loc[i] = add_row
                                df_null.to_excel(fileDir + '\\' + 'Report_for_Null_Check' + '.xlsx', index=False)
                        except Exception as e:
                            flash(e)
                            return render_template("home.html")
                    except:
                        flash('Unable to find the table in Database. Please! Give the correct Details')
                        return render_template("home.html")
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    return df_null