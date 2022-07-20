import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
import xlwings as xw
from io import StringIO
def duplicate_file(db2, fileDir, cloud,dupp_file):
    smain = dupp_file
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    df_duplicate = pd.DataFrame(
        columns=['TestCase_Id', 'Test_Type', 'Source_File_Name', 'Source_Duplicates', 'Source_Validation',
                 'Target_Table_Name', 'Target_Duplicates', 'Target_Validation', 'Status'])
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
            if (cloud == 's3'):
                obj = cloud.Bucket(source_bucket).Object(source_file).get()
                df_src = pd.read_csv(obj['Body'], index_col=0)
            else:
                blob_client = cloud.get_blob_client(container=source_bucket, blob=source_file)
                stream = blob_client.download_blob()
                df_src = pd.read_csv(StringIO(stream.content_as_text()),header=None)
            start = time.time()
            if source_bucket== "None":
                try:
                    Query2 = "select * from " + target_tablename + ";"
                    df_tgt = pd.read_sql_query(Query2, db2)
                    duplicate_tgt = df_tgt[df_tgt.duplicated()]
                    if len(duplicate_tgt) == 0:
                        add_row = [tid, 'Duplicate_Check', "None", "None", "None", target_tablename, len(duplicate_tgt),
                                   'Success', "Success"]
                    else:
                        add_row = [tid, 'Duplicate_Check', "None", "None", "None", target_tablename, len(duplicate_tgt),
                                   'Fail', 'Fail']
                    df_duplicate.loc[i] = add_row
                    df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                except Exception as e:
                    flash(e)
                    return render_template("home.html")
            elif target_databasename == "None" or target_tablename == "None":
                try:
                    duplicate_src = df_src[df_src.duplicated()]
                    if len(duplicate_src) == 0:
                        add_row = [tid, 'Duplicate_Check', source_file, len(duplicate_src), 'Success', "None",
                                   "None", "None", 'Success']
                    else:
                        add_row = [tid, 'Duplicate_Check', source_file, len(duplicate_src), 'Fail', "None", "None",
                                   "None", 'Fail']
                    df_duplicate.loc[i] = add_row
                    df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                except Exception as e:
                    flash(e)
                    return render_template("home.html")
            else:
                try:
                    Query2 = "select * from " +  target_tablename + ";"
                    try:
                        df_tgt = pd.read_sql_query(Query2, db2)
                        duplicate_tgt = df_tgt[df_tgt.duplicated()]
                        duplicate_src = df_src[df_src.duplicated()]
                        if len(duplicate_tgt) == len(duplicate_src) == 0:
                            add_row = [tid, 'Duplicate_Check', source_file, len(duplicate_src), 'Success',
                                       target_tablename, len(duplicate_tgt), 'Success', 'Success']
                            df_duplicate.loc[i] = add_row
                            df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                        elif len(duplicate_tgt) == 0 and len(duplicate_src) != 0:
                            add_row = [tid, 'Duplicate_Check', source_file, len(duplicate_src), 'Success',
                                       target_tablename, len(duplicate_tgt), 'Fail', 'Fail']
                            df_duplicate.loc[i] = add_row
                            df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                        elif len(duplicate_tgt) != 0 and len(duplicate_src) == 0:
                            add_row = [tid, 'Duplicate_Check', source_file, len(duplicate_src), 'Fail',
                                       target_tablename, len(duplicate_tgt), 'Success', 'Fail']
                            df_duplicate.loc[i] = add_row
                            df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                        else:
                            add_row = [tid, 'Duplicate_Check', source_file, len(duplicate_src), 'Fail',
                                       target_tablename, len(duplicate_tgt), 'Fail', 'Fail']
                            df_duplicate.loc[i] = add_row
                            df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                except:
                    flash('Unable to find the table in Database. Please! Give the correct Details')
                    return render_template("home.html")
            end = time.time()
    print("Time: ", end - start)
    return df_duplicate

def duplicate(db1, db2, fileDir,dupp):
    smain = dupp
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
    df_duplicate = pd.DataFrame(
        columns=['TestCase_Id', 'Test_Type', 'Souce_Table_Name', 'Source_Duplicates', 'Source_Validation',
                 'Target_Table_Name', 'Target_Duplicates', 'Target_Validation', 'Status'])
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
                    duplicate_tgt = df_tgt[df_tgt.duplicated()]
                    if len(duplicate_tgt) == 0:
                        add_row = [tid, 'Duplicate_Check', "None", "None", "None", target_tablename, len(duplicate_tgt),
                                   'Success', "Success"]
                    else:
                        add_row = [tid, 'Duplicate_Check', "None", "None", "None", target_tablename, len(duplicate_tgt),
                                   'Fail', 'Fail']
                    df_duplicate.loc[i] = add_row
                    df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                except Exception as e:
                    flash(e)
                    return render_template("home.html")
            elif target_databasename == "None" or target_tablename == "None":
                try:
                    Query1 = "select * from "  + source_tablename + ";"
                    df_src = pd.read_sql_query(Query1, db1)
                    duplicate_src = df_src[df_src.duplicated()]
                    if len(duplicate_src) == 0:
                        add_row = [tid, 'Duplicate_Check', source_tablename, len(duplicate_src), 'Success', "None",
                                   "None", "None", 'Success']
                    else:
                        add_row = [tid, 'Duplicate_Check', source_tablename, len(duplicate_src), 'Fail', "None", "None",
                                   "None", 'Fail']
                    df_duplicate.loc[i] = add_row
                    df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                except Exception as e:
                    flash(e)
                    return render_template("home.html")
            else:
                try:
                    Query1 = "select * from " + source_tablename + ";"
                    Query2 = "select * from "  + target_tablename + ";"
                    try:
                        df_tgt = pd.read_sql_query(Query2, db2)
                        duplicate_tgt = df_tgt[df_tgt.duplicated()]
                        df_src = pd.read_sql_query(Query1, db1)
                        duplicate_src = df_src[df_src.duplicated()]
                        if len(duplicate_tgt) == len(duplicate_src) == 0:
                            add_row = [tid, 'Duplicate_Check', source_tablename, len(duplicate_src), 'Success',
                                       target_tablename, len(duplicate_tgt), 'Success', 'Success']
                            df_duplicate.loc[i] = add_row
                            df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                        elif len(duplicate_tgt) == 0 and len(duplicate_src) != 0:
                            add_row = [tid, 'Duplicate_Check', source_tablename, len(duplicate_src), 'Success',
                                       target_tablename, len(duplicate_tgt), 'Fail', 'Fail']
                            df_duplicate.loc[i] = add_row
                            df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                        elif len(duplicate_tgt) != 0 and len(duplicate_src) == 0:
                            add_row = [tid, 'Duplicate_Check', source_tablename, len(duplicate_src), 'Fail',
                                       target_tablename, len(duplicate_tgt), 'Success', 'Fail']
                            df_duplicate.loc[i] = add_row
                            df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                        else:
                            add_row = [tid, 'Duplicate_Check', source_tablename, len(duplicate_src), 'Fail',
                                       target_tablename, len(duplicate_tgt), 'Fail', 'Fail']
                            df_duplicate.loc[i] = add_row
                            df_duplicate.to_excel(fileDir + '\\' + 'Report_for_Duplicate_Check' + '.xlsx', index=False)
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                except:
                    flash('Unable to find the table in Database. Please! Give the correct Details')
                    return render_template("home.html")
            end = time.time()
    print("Time: ", end - start)
    return df_duplicate