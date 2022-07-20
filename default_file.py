import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
from io import StringIO
import xlwings as xw
def default(db1,db2, fileDir,defaultt ):
    ddmain = defaultt
    NR = ddmain.shape[0]
    NC = ddmain.shape[1]
    df_default = pd.DataFrame(
        columns=['TestCase_ID', 'Test_Type', 'Database_Name', 'Table_Name', 'Column_Name', 'Expected_Value', 'Actual_Value',
                 'Status'])
    try:
        for x in range(0, NR):
            t = x + 2
            tid = ddmain.iloc[x]["TestCase Id"]
            name = ddmain.iloc[x]["DB"]
            username = ddmain.iloc[x]["User"]
            databasename = ddmain.iloc[x]["Database/Container"]
            tablename = ddmain.iloc[x]["TableName/BlobName"]
            columnname = ddmain.iloc[x]["Column"]
            print(columnname)
            default = ddmain.iloc[x]["Default Value"]
            print('default', default)
            priority_column = ddmain.iloc[x]["Priority Column(Y/N)"]
            if priority_column == "Y":
                start = time.time()
                if name == 'Teradata' or name == 'PostgreSql' or name == 'Snowflake' or name == 'sql':
                    try:
                        try:
                            print('1')
                            query = "select * from "+ tablename
                            print(query)
                            df = pd.read_sql_query(query, db1)
                            x1 = df.groupby(columnname)
                            y = x1.groups
                            default = default.split(',')
                            cnt = 0
                            for i in default:
                                if i in list(y):
                                    cnt += 1
                                else:
                                    pass
                            if cnt == len(df.groupby(columnname).size()):
                                print('Success')
                                add_rows = [tid, "Default_Check", databasename, tablename, columnname, default, list(y),
                                            'Success']
                                df_default.loc[x] = add_rows
                                df_default.to_excel(fileDir + '\\' + 'Report for Default_Check' + '.xlsx', index=False)
                            else:
                                add_rows = [tid, "Default_Check", databasename, tablename, columnname, default, list(y),
                                            'Fail']
                                df_default.loc[x] = add_rows
                                df_default.to_excel(fileDir + '\\' + 'Report for Default_Check' + '.xlsx', index=False)
                        except:
                            print('1')
                            query = "select * from " + tablename
                            print(query)
                            df = pd.read_sql_query(query, db2)
                            x1 = df.groupby(columnname)
                            y = x1.groups
                            default = default.split(',')
                            cnt = 0
                            for i in default:
                                if i in list(y):
                                    cnt += 1
                                else:
                                    pass
                            if cnt == len(df.groupby(columnname).size()):
                                print('Success')
                                add_rows = [tid, "Default_Check", databasename, tablename, columnname, default,
                                            list(y), 'Success']
                                df_default.loc[x] = add_rows
                                df_default.to_excel(fileDir + '\\' + 'Report for Default_Check' + '.xlsx',
                                                    index=False)
                            else:
                                add_rows = [tid, "Default_Check", databasename, tablename, columnname, default,
                                            list(y), 'Fail']
                                df_default.loc[x] = add_rows
                                df_default.to_excel(fileDir + '\\' + 'Report for Default_Check' + '.xlsx',
                                                    index=False)
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    return df_default

def default_file(db2, fileDir, cloud,defaultt ):
    ddmain = defaultt
    NR = ddmain.shape[0]
    print(NR)
    NC = ddmain.shape[1]
    df_default = pd.DataFrame(
        columns=['TestCase_ID', 'Test_Type', 'Database_Name/Container_Name', 'Table_Name/Blob_Name', 'Column_Name', 'Expected_Value', 'Actual_Value',
                 'Status'])
    try:
        for x in range(0, NR):
            t = x + 2
            tid = ddmain.iloc[x]["TestCase Id"]
            name = ddmain.iloc[x]["DB"]
            username = ddmain.iloc[x]["User"]
            databasename = ddmain.iloc[x]["Database/Container"]
            tablename = ddmain.iloc[x]["TableName/BlobName"]
            columnname = ddmain.iloc[x]["Column"]
            print(columnname)
            default = ddmain.iloc[x]["Default Value"]
            print('default', default)
            priority_column = ddmain.iloc[x]["Priority Column(Y/N)"]
            if priority_column == "Y":
                start = time.time()
                if name == 'Teradata' or name == 'PostgreSql' or name == 'Snowflake' or name == 'sql':
                    try:
                        print('1')
                        query = "select * from " + tablename
                        print(query)
                        df = pd.read_sql_query(query, db2)
                        x1 = df.groupby(columnname)
                        y = x1.groups
                        default = default.split(',')
                        cnt = 0
                        for i in default:
                            if i in list(y):
                                cnt += 1
                            else:
                                pass
                        if cnt == len(df.groupby(columnname).size()):
                            print('Success')
                            add_rows = [tid, "Default_Check", databasename, tablename, columnname, default,
                                        list(y), 'Success']
                            df_default.loc[x] = add_rows
                            df_default.to_excel(fileDir + '\\' + 'Report for Default_Check' + '.xlsx',
                                                index=False)
                        else:
                            add_rows = [tid, "Default_Check", databasename, tablename, columnname, default,
                                        list(y), 'Fail']
                            df_default.loc[x] = add_rows
                            df_default.to_excel(fileDir + '\\' + 'Report for Default_Check' + '.xlsx',
                                                index=False)
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                else:
                    if (cloud == 's3'):
                        print("no")
                        obj = cloud.Bucket(databasename).Object(tablename).get()
                        df = pd.read_csv(obj['Body'], index_col=0)
                    else:
                        print(cloud)
                        print("hi")
                        blob_client = cloud.get_blob_client(container=databasename, blob=tablename)
                        stream = blob_client.download_blob()
                        df = pd.read_csv(StringIO(stream.content_as_text()))
                        print(df.head())
                    x1 = df.groupby(columnname)
                    y = x1.groups
                    default = default.split(',')
                    cnt = 0
                    for i in default:
                        if i in list(y):
                            cnt += 1
                        else:
                            pass
                    if cnt == len(df.groupby(columnname).size()):
                        print('Success')
                        add_rows = [tid, "Default_Check",databasename, tablename, columnname, default, list(y),
                                    'Success']
                        df_default.loc[x] = add_rows
                        df_default.to_excel(fileDir + '\\' + 'Report for Default_Check' + '.xlsx', index=False)
                    else:
                        add_rows = [tid, "Default_Check",databasename, tablename, columnname, default, list(y),
                                    'Fail']
                        df_default.loc[x] = add_rows
                        df_default.to_excel(fileDir + '\\' + 'Report for Default_Check' + '.xlsx', index=False)
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection")
        return render_template("home.html")
    print("Time: ", end - start)
    return df_default

