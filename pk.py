import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
from io import StringIO
import xlwings as xw
def pk_file(db2, fileDir, cloud,pk):

    ddmain = pk
    NR = ddmain.shape[0]
    NC = ddmain.shape[1]
    df_pk = pd.DataFrame(
        columns=['TestCase_ID', 'Test_Type', 'Database_Name/Storage_Name', 'Table_Name/Blob_Name', 'Primary_Key_Column_Name'])
    try:
        for x in range(0, NR):
            t = x + 2
            tid = ddmain.iloc[x]["TestCase Id"]
            name = ddmain.iloc[x]["DB/Storage"]
            databasename = ddmain.iloc[x]["Database/Bucket"]
            tablename = ddmain.iloc[x]["TableName/FileName"]
            priority_column = ddmain.iloc[x]["Priority Column(Y/N)"]
            if priority_column == "Y":
                start = time.time()
                if name == 'Teradata' or name == 'PostgreSql' or name == 'Snowflake' or name=='sql':
                    try:
                        print('1')
                        query = "select * from " + tablename
                        print(query)
                        df = pd.read_sql_query(query, db2)
                        pk = []
                        for i in df.columns:
                            df[i] = df[i].drop_duplicates()
                            if df[i].isnull().sum() == 0:
                                pk.append(i)
                        add_rows = [tid, "Primary_Key_Check", databasename, tablename,pk]
                        df_pk.loc[x] = add_rows
                        df_pk.to_excel(fileDir + '\\' + 'Report for Primary_Key_Check' + '.xlsx',
                                            index=False)
                    except:
                        flash("Unable to find the table in Database. Please! Give Correct Target Details")
                        return render_template("home.html")
                else:
                    if cloud=='s3':
                        obj = cloud.Bucket(databasename).Object(tablename).get()
                        df = pd.read_csv(obj['Body'], index_col=0)
                    else:
                        blob_client = cloud.get_blob_client(container=databasename, blob=tablename)
                        stream = blob_client.download_blob()
                        df_src = pd.read_csv(StringIO(stream.content_as_text()))

                    pk = []
                    for i in df.columns:
                        df[i] = df[i].drop_duplicates()
                        if df[i].isnull().sum() == 0:
                            pk.append(i)
                    add_rows = [tid, "Primary_Key_Check", databasename, tablename, pk]
                    df_pk.loc[x] = add_rows
                    df_pk.to_excel(fileDir + '\\' + 'Report for Primary_Key_Check' + '.xlsx',
                                   index=False)
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    return df_pk

def pkey(db2, fileDir, pk):
    ddmain = pk
    NR = ddmain.shape[0]
    NC = ddmain.shape[1]
    df_pk = pd.DataFrame(
        columns=['TestCase_ID', 'Test_Type', 'Database_Name/Storage_Name', 'Table_Name/Blob_Name', 'Primary_Key_Column_Name'])
    try:
        for x in range(0, NR):
            t = x + 2
            tid = ddmain.iloc[x]["TestCase Id"]
            name = ddmain.iloc[x]["DB/Storage"]
            databasename = ddmain.iloc[x]["Database/Bucket"]
            tablename = ddmain.iloc[x]["TableName/FileName"]
            priority_column = ddmain.iloc[x]["Priority Column(Y/N)"]
            if priority_column == "Y":
                start = time.time()
                if name == 'Teradata' or name == 'PostgreSql' or name == 'Snowflake':
                    try:
                        print('1')
                        query = "select * from " + tablename
                        print(query)
                        df = pd.read_sql_query(query, db2)
                        pk = []
                        for i in df.columns:
                            df[i] = df[i].drop_duplicates()
                            if df[i].isnull().sum() == 0:
                                pk.append(i)
                        add_rows = [tid, "Primary_Key_Check", databasename, tablename,pk]
                        df_pk.loc[x] = add_rows
                        df_pk.to_excel(fileDir + '\\' + 'Report for Primary_Key_Check' + '.xlsx',
                                            index=False)
                    except:
                        flash("Unable to find the table in Database. Please! Give Correct Target Details")
                        return render_template("home.html")

                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    return df_pk


