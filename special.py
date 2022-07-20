import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
from io import StringIO
import xlwings as xw

def special_file(db2, fileDir, cloud,spe):
    smain = spe
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    df_special = pd.DataFrame(
        columns=['TestCase ID', 'Test_Type', 'Container_Name', 'Blob_Name', 'Column_name', 'Character', 'Status'])
    try:
        for x in range(0, NR):
            t = x + 2
            print('tid', t)
            tid = smain.iloc[x]["TestCase Id"]
            t_type = smain.iloc[x]["Test_Type"]
            name = smain.iloc[x]["DB"]
            username = smain.iloc[x]["User"]
            databasename = smain.iloc[x]["Database/Container"]
            tablename = smain.iloc[x]["Table_Name/Blob_Name"]
            columnname = smain.iloc[x]["Column"]
            special = smain.iloc[x]["Character"]
            priority_column = smain.iloc[x]["Priority Column(Y/N)"]
            if priority_column == "Y":
                start = time.time()
                if name == 'Teradata' or name == 'PostgreSql' or name == 'Snowflake' or name == 'sql':

                    try:
                        print('1')
                        query = "select * from " + tablename
                        print(query)
                        df = pd.read_sql_query(query, db2)
                        char = df.loc[df[columnname].str.contains(special, case=False)]
                        if char.shape[0] == df.shape[0]:
                            add_rows = [tid, t_type, databasename, tablename, columnname, special,
                                        'Success']
                            df_special.loc[x] = add_rows
                            df_special.to_excel(fileDir + '\\' + 'Report for Special_Char_Check' + '.xlsx',
                                                index=False)
                        else:
                            add_rows = [tid, t_type, databasename, tablename, columnname, special, 'Fail']
                            df_special.loc[x] = add_rows
                            df_special.to_excel(fileDir + '\\' + 'Report for Special_Char_Check' + '.xlsx',
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
                    char = df.loc[df[columnname].str.contains(special, case=False)]
                    if char.shape[0] == df.shape[0]:
                        add_rows = [tid, t_type, databasename, tablename, columnname, special,
                                    'Success']
                        df_special.loc[x] = add_rows
                        df_special.to_excel(fileDir + '\\' + 'Report for Special_Char_Check' + '.xlsx',
                                            index=False)
                    else:
                        add_rows = [tid, t_type, databasename, tablename, columnname, special, 'Fail']
                        df_special.loc[x] = add_rows
                        df_special.to_excel(fileDir + '\\' + 'Report for Special_Char_Check' + '.xlsx',
                                            index=False)

                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection.")
        return render_template("home.html")
    print("Time: ", end - start)
    return df_special

def special(db1,db2, fileDir,spe):
    smain = spe
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    df_special = pd.DataFrame(
        columns=['TestCase ID', 'Test_Type', 'Database Name', 'Table Name', 'Column name', 'Character', 'Status'])
    try:
        for x in range(0, NR):
            t = x + 2
            print('tid', t)
            tid = smain.iloc[x]["TestCase Id"]
            t_type = smain.iloc[x]["Test_Type"]
            name = smain.iloc[x]["DB"]
            username = smain.iloc[x]["User"]
            databasename = smain.iloc[x]["Database/Container"]
            tablename = smain.iloc[x]["Table_Name/Blob_Name"]
            columnname = smain.iloc[x]["Column"]
            special = smain.iloc[x]["Character"]
            priority_column = smain.iloc[x]["Priority Column(Y/N)"]
            if priority_column == "Y":
                start = time.time()
                if name == 'Teradata' or name == 'PostgreSql' or name == 'Snowflake' or name == 'sql':
                    try:
                        try:
                            print('1')
                            query = "select * from " + tablename
                            print(query)
                            df = pd.read_sql_query(query, db1)
                            char = df.loc[df[columnname].str.contains(special, case=False)]
                            if char.shape[0] == df.shape[0]:
                                add_rows = [tid, t_type, databasename, tablename, columnname, special, 'Success']
                                df_special.loc[x] = add_rows
                                df_special.to_excel(fileDir + '\\' + 'Report for Special_Char_Check' + '.xlsx',
                                                    index=False)
                            else:
                                add_rows = [tid, t_type, databasename, tablename, columnname, special, 'Fail']
                                df_special.loc[x] = add_rows
                                df_special.to_excel(fileDir + '\\' + 'Report for Special_Char_Check' + '.xlsx',
                                                    index=False)
                        except:
                            print('1')
                            query = "select * from " + tablename
                            print(query)
                            df = pd.read_sql_query(query, db2)
                            char = df.loc[df[columnname].str.contains(special, case=False)]
                            if char.shape[0] == df.shape[0]:
                                add_rows = [tid, t_type, databasename, tablename, columnname, special,
                                            'Success']
                                df_special.loc[x] = add_rows
                                df_special.to_excel(fileDir + '\\' + 'Report for Special_Char_Check' + '.xlsx',
                                                    index=False)
                            else:
                                add_rows = [tid, t_type, databasename, tablename, columnname, special, 'Fail']
                                df_special.loc[x] = add_rows
                                df_special.to_excel(fileDir + '\\' + 'Report for Special_Char_Check' + '.xlsx',
                                                    index=False)
                    except Exception as e:
                        flash(e)
                        return render_template("home.html")
                end = time.time()
    except:
        flash("Connection Error! Please Check Your Connection.")
        return render_template("home.html")
    print("Time: ", end - start)
    return df_special
