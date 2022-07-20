import pandas as pd
from datetime import datetime
import time
from flask import Flask, render_template, flash
import os
import datacompy
import numpy as np
from io import StringIO
from zipfile import ZipFile
import openpyxl
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions
import xlwings as xw

now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

def full_file(db2, fileDir, cloud,full_fil,filedb, databasee):
    zip_list = []
    smain = full_fil
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    df_full = pd.DataFrame(columns=['Test_CaseId', 'Test_Type', 'Source_TableName', 'Target_TableName', 'Status'])
    mismatch_df = pd.DataFrame(
        columns=['TestCase_ID', 'DataMart_Name', 'Source_Table', 'Target_Table',
       'Source_Primary_Key', 'Target_Primary_Key',
       'Testcase_Execution_timestamp', 'Mismatch_Type',
       'Mismatch_Description'])
    df_count1 = pd.DataFrame(columns=['Database', 'Environment', 'TableName', 'Column', 'Rows'])
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
        tcol = target_primary_key.split(",") 
        Join_columns = tcol
        priority_column = smain.iloc[i]["Priority Column(Y/N)"]

        print(type(priority_column))
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
            try:
                Query2 = "select * from " + target_tablename + ";"
                # try:
                pda1 = df_src
                pda2 = pd.read_sql_query(Query2, db2)
                pda1.columns=pda2.columns
                source_col = pda1.columns
                target_col = pda2.columns
                Join_columns = smain.iloc[i]['Primary Column']
                print('Join_columns', Join_columns)
                compare = datacompy.Compare(pda1, pda2, join_columns=Join_columns, df1_name=source_file,
                                            df2_name=target_tablename)

                if not compare.matches(ignore_extra_columns=False):
                    add_row = [tid, 'Full Check', source_file, target_tablename, 'fail']
                    df_full.loc[i] = add_row
                    df_full.to_excel(fileDir + '\\' + 'Report_for_Full_Check' + '.xlsx', index=False)
                    zip_list.append(tid)
                    mis = compare.all_mismatch()
                    second = [col for col in mis.columns if col.endswith('_df2')]
                    first = [col for col in mis.columns if col.endswith('_df1')]
                    miss = mis.copy()
                    miss.reset_index(inplace=True)
                    miss.drop('index', axis=1, inplace=True)

                    for c in range(mis.shape[0]):
                        for (f, s) in zip(first, second):
                            if mis[f].iloc[c] == mis[s].iloc[c]:
                                miss.loc[c, f] = np.nan
                                miss.loc[c, s] = np.nan
                            else:
                                pass
                    excel_name = []
                    excel_df = []
                    miss.columns = miss.columns.str.replace("_df1", "_source")
                    miss.columns = miss.columns.str.replace("_df2", "_target")
                    if miss.shape[0] != 0:
                        miss.dropna(axis='columns', how='all', inplace=True)
                        excel_df.append(miss)
                        excel_name.append('Mismatch')
                    else:
                        pass
                    print("miss", miss)
                    df = pda1.merge(pda2, on=Join_columns.lower(), how='outer', indicator='join')
                    print("df", df)
                    if df[df['join'] == 'right_only'].shape[0] == 0:
                        right = pd.DataFrame()
                    else:
                        right = df[df['join'] == 'right_only'].drop('join', axis=1).dropna(axis=1)
                        right.columns = target_col
                        excel_name.append('Only in Target')
                        excel_df.append(right)

                    if df[df['join'] == 'left_only'].shape[0] == 0:
                        left = pd.DataFrame()
                    else:
                        left = df[df['join'] == 'left_only'].drop('join', axis=1).dropna(axis=1)
                        left.columns = source_col
                        excel_name.append('Only in Source')
                        excel_df.append(left)
                    print("right", right)
                    print("left", left)
                    
                    summary = pd.DataFrame(
                        {
                            "Database": [filedb, databasee],
                            "Enivornment": [source_bucket, target_databasename],
                            "Table_Name": [source_file, target_tablename],
                            "Columns": [pda1.shape[1], pda2.shape[1]],
                            "Rows": [pda1.shape[0], pda2.shape[0]],
                        }
                    )
                    print(summary)
                    only_summary = pd.DataFrame(
                        {
                            "Summary": ['Only in Source Table', 'Only in Target Table'],
                            "Row_Count": [left.shape[0], right.shape[0]],
                        }
                    )
                    print("only", only_summary)
                    stats = [summary, only_summary]
                    print(stats)
                    miss_new = miss.copy()
                    miss_new.columns = miss_new.columns.str.replace(r"_source", "")
                    miss_new.columns = miss_new.columns.str.replace(r"_target", "")
                    print('miss new ke aandar aa gaya ')
                    print(miss_new)
                
                    colst = [Join_columns]
                    joinn = [i.lower() for i in colst]
                    if miss_new.shape[0] != 0:
                        print('miss_new wale if ke aandar')
                        new_df = miss_new.loc[:, ~miss_new.columns.duplicated()].drop(joinn, axis=1)
                        print("new df", new_df)
                        miss_summary = pd.DataFrame(new_df.count(), columns=['Row Count'])
                        miss_summary.index.name = 'Mismatch Column'
                        miss_summary.reset_index(inplace=True)
                        # miss_summary.drop(['index'], axis=1, inplace=True)
                        stats.append(miss_summary)
                        print('exit if loop')
                    else:
                        miss_summary = pd.DataFrame(
                            {
                                "Mismatch": ['None'],
                                "Row_Count": [0],
                            }
                        )
                        stats.append(miss_summary)
                    print(miss_summary)

                    def multiple_dfs(df_list, sheets, file_name, spaces):
                        writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
                        row = 0
                        for dataframe in df_list:
                            dataframe.to_excel(writer, sheet_name=sheets, startrow=row, startcol=0, index=False)
                            row = row + len(dataframe.index) + spaces + 1
                        writer.save()

                    multiple_dfs(stats, 'Summary', fileDir + '\\' + str(tid) + 'Detail_Report_for_Full_Check' + '.xlsx',
                                 3)
                    with pd.ExcelWriter(fileDir + '\\' + str(tid) + 'Detail_Report_for_Full_Check' + '.xlsx',
                                        mode='a', engine='openpyxl', if_sheet_exists='new') as writer:
                        for df, df_name in zip(excel_df, excel_name):
                            df.to_excel(writer, sheet_name=df_name, index=False)
                        # stats_src.to_excel(writer, sheet_name='Source Statistics')
                        # stats_tgt.to_excel(writer, sheet_name='Target Statistics')
                    
                    if mismatch_df.shape[0] == 0:
                        print("entered if cond")
                        shape = 0+left.shape[0]
                        idx = 0
                        for i in range(0, shape):
                            add_row = [tid, datamart, source_file, target_tablename, left.iloc[idx][0], np.nan, dt_string, 'Only in source', 'Record Only in source']
                            mismatch_df.loc[i] = add_row
                            idx = idx + 1
                        tar_shape = shape + right.shape[0]
                        idx = 0
                        for i in range(shape, tar_shape):
                            add_row = [tid, datamart, source_file, target_tablename, np.nan, right.iloc[idx][0], dt_string, 'Only in target', 'Record Only in target']
                            mismatch_df.loc[i] = add_row
                            idx = idx + 1   
                        miss_shape = tar_shape + miss.shape[0]
                        idx = 0
                        for i in range(tar_shape, miss_shape):
                            add_row = [tid, datamart, source_file, target_tablename, miss.iloc[idx][0], miss.iloc[idx][0], dt_string, 'Column data mismatch', 'Data mismatch for '+ miss.columns[1]]
                            mismatch_df.loc[i] = add_row
                            idx = idx + 1
                        print("if part done")
                    else:
                        print("entered else cond")
                        shape = mismatch_df.shape[0]+left.shape[0]
                        idx = 0
                        for i in range(mismatch_df.shape[0], shape):
                            add_row = [tid, datamart, source_file, target_tablename, left.iloc[idx][0], np.nan, dt_string, 'Only in source', 'Record Only in source']
                            mismatch_df.loc[i] = add_row
                            idx = idx + 1
                        tar_shape = shape + right.shape[0]
                        idx = 0
                        for i in range(shape, tar_shape):
                            add_row = [tid, datamart, source_file, target_tablename, np.nan, right.iloc[idx][0], dt_string, 'Only in target', 'Record Only in target']
                            mismatch_df.loc[i] = add_row
                            idx = idx + 1   
                        miss_shape = tar_shape + miss.shape[0]
                        idx = 0
                        for i in range(tar_shape, miss_shape):
                            add_row = [tid, datamart, source_file, target_tablename, miss.iloc[idx][0], miss.iloc[idx][0], dt_string, 'Column data mismatch', 'Data mismatch for '+ miss.columns[1]]
                            mismatch_df.loc[i] = add_row
                            idx = idx + 1
                        print("else part done")

                else:
                    def multiple_dfs(df_list, sheets, file_name, spaces):
                        writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
                        row = 0
                        for dataframe in df_list:
                            dataframe.to_excel(writer, sheet_name=sheets, startrow=row, startcol=0, index=False)
                            row = row + len(dataframe.index) + spaces + 1
                        writer.save()
                    
                    summary = pd.DataFrame(
                        {
                            "Database": [filedb, databasee],
                            "Enivornment": [source_bucket, target_databasename],
                            "Table_Name": [source_file, target_tablename],
                            "Columns": [pda1.shape[1], pda2.shape[1]],
                            "Rows": [pda1.shape[0], pda2.shape[0]],
                        }
                    )

                    print("pass")
                    add_row = [tid, 'Full Check', source_file, target_tablename, 'success']
                    df_full.loc[i] = add_row

                    stats = [df_full, summary]
                    multiple_dfs(stats, 'Matched_report', fileDir + '\\' + str(tid) + 'Report_for_Matched_Data_Full_Check' + '.xlsx', 3)
                    
                    df_full.to_excel(fileDir + '\\' + 'Report_for_Full_Check' + '.xlsx', index=False)
            except Exception as e:
                flash(e)
                return render_template("home.html")
            end = time.time()
            print("Time: ", end - start)
        mismatch_df.to_excel(fileDir + '\\' + 'Report_for_newfull_Check' + '.xlsx', index=False)

    with ZipFile('Full_Comparison_Detail_Reports.zip', 'w') as zipObj2:
        for k,l in zip(range(0,NR),zip_list):
            priority_column = str(smain.iloc[k]["Priority Column(Y/N)"])
            if priority_column == "Y":
                filename_summary = str(l) + "Detail_Report_for_Full_Check.xlsx"
                zipObj2.write(filename_summary)
    return df_full

def full(db1, db2, fileDir,fulll, datanamedb1, datanamedb2):
    zip_list = []
    smain = fulll
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = smain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    mismatch_df = pd.DataFrame(
        columns=['TestCase_ID', 'DataMart_Name', 'Source_Table', 'Target_Table',
       'Source_Primary_Key', 'Target_Primary_Key',
       'Testcase_Execution_timestamp', 'Mismatch_Type',
       'Mismatch_Description'])
    df_full = pd.DataFrame(columns=['Test_CaseId', 'Test_Type', 'Source_TableName', 'Target_TableName', 'Status'])
    df_count1 = pd.DataFrame(columns=['Database', 'Environment', 'TableName', 'Column', 'Rows'])
    for i in range(0, NR):
        y = i + 2
        start1_time = time.time()
        tid = str(smain.iloc[i]["Test Case ID"])
        print("Executing TestCaseID - " + tid)
        datamart = str(smain.iloc[i]["Datamart"])
        source_databasename = str(smain.iloc[i]["Source DataBase"])
        source_tablename = str(smain.iloc[i]["Source Table Name"])
        target_databasename = str(smain.iloc[i]["Target Database"])
        target_tablename = str(smain.iloc[i]["Target Table Name"])
        source_primary_key = str(smain.iloc[i]["Primary Source Column"])
        target_primary_key = str(smain.iloc[i]["Primary Target Column"])
        priority_column = smain.iloc[i]["Priority Column(Y/N)"]
        #full_filedir = smain.iloc[i]["Directory_path"]
        if priority_column == "Y":
            start = time.time()
            try:
                if source_databasename != "None" and source_tablename != "None" and target_databasename != "None" and target_tablename != "None":
                    Query1 = "select * from "  + source_tablename 
                    Query2 = "select * from " + target_tablename
                    print('Converting to Dataframe')
                    pda1 = pd.read_sql_query(Query1, db1)
                    pda2 = pd.read_sql_query(Query2, db2)
                    pda1= pda1.astype('object')
                    pda2= pda2.astype('object')
                    print('Conversion Done')
                    source_col = pda1.columns
                    target_col = pda2.columns


                    scol = source_primary_key.split(",")
                    tcol = target_primary_key.split(",")

                    res = [i +'/'+ j for i, j in zip(scol, tcol)]

                    if scol==tcol:
                        print('Primary Columns is/are equal')
                        res = scol
                    else:
                        try:
                            print('Columns not equal')
                            for i in range(len(scol)):
                                print('Renaming of source')
                                pda1=pda1.rename(columns = {scol[i]:res[i]})
                            for i in range(len(tcol)):
                                print('Renaming of target')
                                pda2=pda2.rename(columns = {tcol[i]:res[i]})
                        except Exception as e:
                            print(e)
                    Join_columns = res

                    print('Join_columns', Join_columns)
                    print('Source', pda1.columns)
                    print('Target', pda2.columns)

                    print('Comparing report')

                    
                    compare = datacompy.Compare(pda1.copy(), pda2.copy(), join_columns=Join_columns, df1_name=source_tablename,
                                                df2_name=target_tablename)
                    
                    if not compare.matches(ignore_extra_columns=False):
                        print('Mismatch there')
                        add_row = [tid, 'Full Check', source_tablename, target_tablename, 'fail']
                        df_full.loc[i] = add_row
                        df_full.to_excel(fileDir + '\\' + 'Report_for_Full_Check' + '.xlsx', index=False)
                        zip_list.append(tid)
                        print('Actual Mismatch Calculating...')
                        mis = compare.all_mismatch()
                        second = [col for col in mis.columns if col.endswith('_df2')]
                        first = [col for col in mis.columns if col.endswith('_df1')]
                        miss = mis.copy()
                        miss.reset_index(inplace=True)
                        miss.drop('index', axis=1, inplace=True)
                        print('Done with mismatch')
                        for c in range(mis.shape[0]):
                            for (f, s) in zip(first, second):
                                if mis[f].iloc[c] == mis[s].iloc[c]:
                                    miss.loc[c, f] = np.nan
                                    miss.loc[c, s] = np.nan
                                else:
                                    pass
                        excel_name = []
                        excel_df = []
                        miss.columns = miss.columns.str.replace("_df1", "_source")
                        miss.columns = miss.columns.str.replace("_df2", "_target")
                        
                        if miss.shape[0] != 0:
                            print('No mimatch')
                            miss.dropna(axis='columns', how='all', inplace=True)
                            excel_df.append(miss)
                            excel_name.append('Mismatch')
                        else:
                            print('Found')
                            pass
                        joinn = [i.lower() for i in Join_columns]
                        df = pda1.merge(pda2, on=res, how='outer', indicator='join')
                        print("df", df)
                        if df[df['join'] == 'right_only'].shape[0] == 0:
                            right = pd.DataFrame()
                        else:
                            right = df[df['join'] == 'right_only'].drop('join', axis=1).dropna(axis=1,how='all')
                            right.columns = target_col
                            excel_name.append('Only in Target')
                            excel_df.append(right)

                        if df[df['join'] == 'left_only'].shape[0] == 0:
                            left = pd.DataFrame()
                        else:
                            left = df[df['join'] == 'left_only'].drop('join', axis=1).dropna(axis=1,how='all')
                            left.columns = source_col
                            excel_name.append('Only in Source')
                            excel_df.append(left)
                        print("right",right)
                        print("left", left)

                        print("db1=" + datanamedb1 +"db2=" + datanamedb2)
                        summary = pd.DataFrame(
                            {
                                "Database" : [datanamedb1, datanamedb2],
                                "Enivornment" : [source_databasename, target_databasename],
                                "Table_Name": [source_tablename, target_tablename],
                                "Columns": [pda1.shape[1], pda2.shape[1]],
                                "Rows": [pda1.shape[0], pda2.shape[0]],
                            }
                        )
                        print(summary)
                        only_summary = pd.DataFrame(
                            {
                                "Summary": ['Only in Source Table', 'Only in Target Table'],
                                "Row_Count": [left.shape[0], right.shape[0]],
                            }
                        )
                        print("only", only_summary)
                        stats = [summary, only_summary]
                        miss_new = miss.copy()
                        miss_new.columns = miss_new.columns.str.replace(r"_source", "")
                        miss_new.columns = miss_new.columns.str.replace(r"_target", "")
                        print(miss_new)
                        colst = Join_columns
                        joinn = [i.lower() for i in colst]
                        if miss_new.shape[0] != 0:
                            new_df = miss_new.loc[:, ~miss_new.columns.duplicated()].drop(joinn, axis=1)
                            print("new df", new_df)
                            miss_summary = pd.DataFrame(new_df.count(), columns=['Row Count'])
                            miss_summary.index.name = 'Mismatch Column'
                            miss_summary.reset_index(inplace=True)
                            # miss_summary.drop(['index'], axis=1, inplace=True)
                            stats.append(miss_summary)
                        else:
                            miss_summary = pd.DataFrame(
                                {
                                    "Mismatch": ['None'],
                                    "Row_Count": [0],
                                }
                            )
                            stats.append(miss_summary)
                        print(miss_summary)
                        df_report = [df_full, df_count1]
        
                        def multiple_dfs(df_list, sheets, file_name, spaces):
                            print("multiple entered")
                            writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
                            row = 0
                            for dataframe in df_list:
                                dataframe.to_excel(writer, sheet_name=sheets, startrow=row, startcol=0, index=False)
                                row = row + len(dataframe.index) + spaces + 1
                            writer.save()
                            print("multiple exit")
                        print(fileDir)
                        #multiple_dfs(df_report, 'Summary',fileDir + '\\' + str(tid) + 'Report_for_Full_Check' + '.xlsx',3)
                        multiple_dfs(stats, 'Summary', fileDir + '\\' + str(tid) + 'Detail_Report_for_Full_Check' + '.xlsx',3)
                        with pd.ExcelWriter(fileDir + '\\' + str(tid) + 'Detail_Report_for_Full_Check' + '.xlsx',
                                            mode='a', engine='openpyxl',if_sheet_exists='new') as writer:
                            print("462 line entered")
                            for df, df_name in zip(excel_df, excel_name):
                                df.to_excel(writer, sheet_name=df_name, index=False)
                            #stats_src.to_excel(writer, sheet_name='Source Statistics')
                            #stats_tgt.to_excel(writer, sheet_name='Target Statistics')

                        if mismatch_df.shape[0] == 0:
                            print("entered if cond")
                            shape = 0+left.shape[0]
                            idx = 0
                            for i in range(0, shape):
                                add_row = [tid, datamart, source_tablename, target_tablename, left.iloc[idx][0], np.nan, dt_string, 'Only in source', 'Record Only in source']
                                mismatch_df.loc[i] = add_row
                                idx = idx + 1
                            tar_shape = shape + right.shape[0]
                            idx = 0
                            for i in range(shape, tar_shape):
                                add_row = [tid, datamart, source_tablename, target_tablename, np.nan, right.iloc[idx][0], dt_string, 'Only in target', 'Record Only in target']
                                mismatch_df.loc[i] = add_row
                                idx = idx + 1   
                            miss_shape = tar_shape + miss.shape[0]
                            idx = 0
                            for i in range(tar_shape, miss_shape):
                                add_row = [tid, datamart, source_tablename, target_tablename, miss.iloc[idx][0], miss.iloc[idx][0], dt_string, 'Column data mismatch', 'Data mismatch for '+ miss.columns[1]]
                                mismatch_df.loc[i] = add_row
                                idx = idx + 1
                            print("if part done")
                        else:
                            print("entered else cond")
                            shape = mismatch_df.shape[0]+left.shape[0]
                            idx = 0
                            for i in range(mismatch_df.shape[0], shape):
                                add_row = [tid, datamart, source_tablename, target_tablename, left.iloc[idx][0], np.nan, dt_string, 'Only in source', 'Record Only in source']
                                mismatch_df.loc[i] = add_row
                                idx = idx + 1
                            tar_shape = shape + right.shape[0]
                            idx = 0
                            for i in range(shape, tar_shape):
                                add_row = [tid, datamart, source_tablename, target_tablename, np.nan, right.iloc[idx][0], dt_string, 'Only in target', 'Record Only in target']
                                mismatch_df.loc[i] = add_row
                                idx = idx + 1   
                            miss_shape = tar_shape + miss.shape[0]
                            idx = 0
                            for i in range(tar_shape, miss_shape):
                                add_row = [tid, datamart, source_tablename, target_tablename, miss.iloc[idx][0], miss.iloc[idx][0], dt_string, 'Column data mismatch', 'Data mismatch for '+ miss.columns[1]]
                                mismatch_df.loc[i] = add_row
                                idx = idx + 1
                            print("else part done")
                        
                        
                    else:
                        summary = pd.DataFrame(
                            {
                                "Database" : [datanamedb1, datanamedb2],
                                "Enivornment" : [source_databasename, target_databasename],
                                "Table_Name": [source_tablename, target_tablename],
                                "Columns": [pda1.shape[1], pda2.shape[1]],
                                "Rows": [pda1.shape[0], pda2.shape[0]],
                            }
                        )
                        add_row = [tid, 'Full Check', source_tablename, target_tablename, 'success']

                        df_full.loc[i] = add_row

                        df_full.to_excel(fileDir + '\\' + 'Report_for_Full_Check' + '.xlsx', index=False)
                        
                    def multiple_dfs(df_list, sheets, file_name, spaces):
                            writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
                            row = 0
                            for dataframe in df_list:
                                dataframe.to_excel(writer, sheet_name=sheets, startrow=row, startcol=0, index=False)
                                row = row + len(dataframe.index) + spaces + 1
                            writer.save()

                    df_full_pass = df_full[df_full['Status']=='success']

                    stats = [df_full_pass, summary]

                    multiple_dfs(stats, 'Matched_report', fileDir + '\\' +'Report_for_Matched_Data_Full_Check' + '.xlsx', 3)
                        
            except Exception as e:
                add_row = [tid, 'Full Check', source_tablename, target_tablename, e]
                df_full.loc[i] = add_row
                df_full.to_excel(fileDir + '\\' + 'Report_for_Full_Check' + '.xlsx', index=False)

            end = time.time()
            print("Time: ", end - start)
            print(zip_list)

        mismatch_df.to_excel(fileDir + '\\' + 'Report_for_newfull_Check' + '.xlsx', index=False)

    with ZipFile('Full_Comparison_Detail_Reports.zip', 'w') as zipObj2:
        for k, l in zip(range(0, NR), zip_list):
            priority_column = str(smain.iloc[k]["Priority Column(Y/N)"])
            if priority_column == "Y":
                filename_summary = str(l) + "Detail_Report_for_Full_Check.xlsx"
                try:
                    zipObj2.write(filename_summary)
                    print(filename_summary)
                except:
                    pass
    return df_full