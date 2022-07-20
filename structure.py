import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
from io import StringIO
import xlwings as xw
def structure_file(db2, fileDir, cloud, struct_file):
    smain = struct_file
    NR = smain.shape[0]
    df_sv = pd.DataFrame(
    columns=['TestCase_Id', 'Test_Type', 'Souce_File_Name','Target_Table_Name', 'Status'])
    for i in range(0, NR):
        start1_time = time.time()
        tid = smain.iloc[i]["Test Case ID"]
        source_bucket = str(smain.iloc[i]["Source Bucket/Container Name"])
        source_file = str(smain.iloc[i]["Source File Name"])
        target_databasename = str(smain.iloc[i]["Target Database"])
        target_tablename = str(smain.iloc[i]["Target Table Name"])
        primary = str(smain.iloc[i]["Primary Column"])
        priority_column = smain.iloc[i]["Priority Column(Y/N)"]
        if priority_column == "Y":
            if cloud=='s3':
                obj = cloud.Bucket(source_bucket).Object(source_file).get()
                df_src = pd.read_csv(obj['Body'], index_col=0)
            else:
                blob_client = cloud.get_blob_client(container=source_bucket, blob=source_file)
                stream = blob_client.download_blob()
                df_src = pd.read_csv(StringIO(stream.content_as_text()),header=None)
            start = time.time()
            try:
                Query2 = "select * from " + target_tablename + ";"
                try:
                    df_tgt = pd.read_sql_query(Query2, db2)
                    df_src.columns = df_tgt.columns

                    print(df_src.shape, df_tgt.shape,df_src.columns,df_tgt.columns)
                    if len(df_tgt.columns) == len(df_src.columns):
                        a = df_tgt.columns == df_src.columns
                        if a.all() == True:
                            add_row = [tid, 'Structure_Check', source_file, target_tablename, 'Success']
                        else:
                            add_row = [tid, 'Structure_Check', source_file, target_tablename, 'Fail']
                        df_sv.loc[i] = add_row
                        df_sv.to_excel(fileDir + '\\' + 'Report_for_Structure_Check' + '.xlsx', index=False)
                    else:
                        add_row = [tid, 'Structure_Check', source_file, target_tablename, 'Fail']
                        df_sv.loc[i] = add_row
                        df_sv.to_excel(fileDir + '\\' + 'Report_for_Structure_Check' + '.xlsx', index=False)
                except Exception as e:
                    flash(e)
                    return render_template("home.html")
            except:
                flash('Unable to find the table in Database. Please! Give the correct Details')
                return render_template("home.html")
        end = time.time()

    print("Time: ", end - start)
    return df_sv

def structure(db1, db2, fileDir,structt):
    smain = structt
    NR = smain.shape[0]
    df_sv = pd.DataFrame(
        columns=['TestCase_Id', 'Test_Type', 'Souce_Table_Name','Target_Table_Name', 'Status'])
    for i in range(0, NR):
        start1_time = time.time()
        tid = smain.iloc[i]["Test Case ID"]
        source_databasename = str(smain.iloc[i]["Source DataBase"])
        source_tablename = str(smain.iloc[i]["Source Table Name"])
        target_databasename = str(smain.iloc[i]["Target Database"])
        target_tablename = str(smain.iloc[i]["Target Table Name"])
        priority_column = smain.iloc[i]["Priority Column(Y/N)"]
        if priority_column == "Y":
            start = time.time()
            try:
                Query1 = "select * from " + source_tablename + ";"
                Query2 = "select * from " + target_tablename + ";"
                try:
                    df_tgt = pd.read_sql_query(Query2, db2)
                    df_src = pd.read_sql_query(Query1, db1)
                    try:
                        if len(df_tgt.columns) == len(df_src.columns):
                            a = df_tgt.columns == df_src.columns
                            if a.all() == True:
                                add_row = [tid, 'Structure_Check', source_tablename, target_tablename, 'Success']
                            else:
                                add_row = [tid, 'Structure_Check', source_tablename, target_tablename, 'Fail']
                            df_sv.loc[i] = add_row
                            df_sv.to_excel(fileDir + '\\' + 'Report_for_Structure_Check' + '.xlsx', index=False)
                    except:
                        add_row = [tid, 'Structure_Check', source_tablename, target_tablename, 'Fail']
                        df_sv.loc[i] = add_row
                        df_sv.to_excel(fileDir + '\\' + 'Report_for_Structure_Check' + '.xlsx', index=False)
                except Exception as e:
                    flash(e)
                    return render_template("home.html")
            except:
                flash('Unable to find the table in Database. Please! Give the correct Details')
                return render_template("home.html")
            end = time.time()

    print("Time: ", end - start)
    return df_sv