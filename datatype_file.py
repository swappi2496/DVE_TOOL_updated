# datatype_new********

import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
import time
import regex as re
from io import StringIO
import xlwings as xw
def datatype(db1,db2, fileDir, countt, datanamedb1, datanamedb2):
    lmain = countt
    print("Reading Sheet....", lmain)
    NR = lmain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = lmain.shape[1]
    print("Total no of Cols in sheet : ", NC)
    df_datatype = pd.DataFrame(
        columns=['TestCase_Id', 'Test_Type', 'Source_Table_Name','source_database', 'Target_Table_Name', 'Target_database'
                 , 'Status'])

    print(df_datatype)
    for x in range(0, NR):
        tid = str(lmain.iloc[x]["Test Case ID"])
       # name = str(lmain.iloc[x]["DB"])
        source_database = str(lmain.iloc[x]["Source DataBase"])
        source_schema = str(lmain.iloc[x]["Source_Schema"])
        Target_database=str(lmain.iloc[x]["Target Database"])
        source_Table_Name = str(lmain.iloc[x]["Source Table Name"])
        Target_Table_name=str(lmain.iloc[x]["Target Table Name"])
        target_schema = str(lmain.iloc[x]["Target_Schema"])
        priority_column = str(lmain.iloc[x]["Priority Column(Y/N)"])
        start = time.time()
        print(start)
        if priority_column == "Y":
            print("connected")
            print(db1, db2)
            if datanamedb1 == 'Netezza_db1':
                print("db1 connected")
                try:
                    Query1 = "select COLUMN_NAME,DATA_TYPE from information_schema.columns where table_schema ='"+ source_schema +"' and table_name ='" + source_Table_Name+"'"
                    print(Query1)
                    df_src = pd.read_sql_query(Query1, db1)
                    print(df_src)
                    temp_list=list(df_src['DATA_TYPE'].str.lower())
                    new_list=[]
                    for i in temp_list:
                        if bool(len(re.findall('national character varying\([0-9]+\)|national bpchar\([0-9]+\)',i))>0):
                            new_list.append('nvarchar')
                        elif(bool(len(re.findall('bpchar\([0-9]+\)|character varying\([0-9]+\)',i))>0)):
                            new_list.append('varchar')
                        elif(bool(len(re.findall('character\([0-9]+\)',i))>0)):
                            new_list.append('char')
                        elif(bool(len(re.findall('national character\([0-9]+\)',i))>0)):
                            new_list.append('nchar')
                        elif(bool(len(re.findall('binary varying\([0-9]+\)',i))>0)):
                            new_list.append('varbinary')
                        elif(i=='integer'):
                            new_list.append('int')
                        elif(i=='boolean'):
                            new_list.append('bit')
                        elif(i=='byteint'):
                            new_list.append('smallint')
                        elif(i=='double' or i=="float4" or i=="float8"):
                            new_list.append('float(53)')
                        elif(i=='number'):
                            new_list.append('decimal') 
                        elif(i=='time with time zone'):
                            new_list.append('datetimeoffset')
                        elif(i=='timestamp'):
                            new_list.append('datetime2')    
                        elif(i=='st_geometry'):
                            new_list.append('Not Supported')
                        elif(i=='rowid'):
                            new_list.append('Not Supported')   
                        elif(i=='interval'):
                            new_list.append('Not Supported')
                        elif(i=='dataslice'):
                            new_list.append('Not Supported')
                        elif(i=='transactionid'):
                            new_list.append('Not Supported')    
                        else:
                            new_list.append(i)
                    df_src['DATA_TYPE']=new_list
                except Exception as e:
                    add_row = [tid, 'Datatype_Check', source_Table_Name, source_database,
                                    Target_Table_name, Target_database, e]
                    df_datatype.loc[x] = add_row
                    df_datatype.to_excel(fileDir + '\\' + 'Report_for_Datatype_Check' + '.xlsx', index=False)
                print("db1 connected")
            if datanamedb2 =='SQL_Server_db2':
                try:
                    Query2 = "select COLUMN_NAME,DATA_TYPE from information_schema.columns where table_schema ='"+ target_schema +"' and table_name ='" + Target_Table_name+"'"
                    df_tgt = pd.read_sql_query(Query2, db2)
                    temp_list_t=list(df_tgt['DATA_TYPE'].str.lower())
                    new_list_t=[]
                    for i in temp_list_t:
                        if bool(len(re.findall('nvarchar\([0-9]+\)',i))>0):
                            new_list_t.append('nvarchar')
                        elif(bool(len(re.findall('varchar\([0-9]+\)',i))>0)):
                            new_list_t.append('varchar') 
                        elif(bool(len(re.findall('nchar\([0-9]+\)',i))>0)):
                            new_list_t.append('nchar')
                        elif(bool(len(re.findall('char\([0-9]+\)',i))>0)):
                            new_list_t.append('char')
                        elif(bool(len(re.findall('varbinary\([0-9]+\)',i))>0)):
                            new_list_t.append('varbinary')
                        else:
                            new_list_t.append(i)
                    df_tgt['DATA_TYPE']=new_list_t    
                except Exception as e:
                    add_row = [tid, 'Datatype_Check', source_Table_Name, source_database,
                                    Target_Table_name, Target_database, e]
                    df_datatype.loc[x] = add_row
                    df_datatype.to_excel(fileDir + '\\' + 'Report_for_Datatype_Check' + '.xlsx', index=False)
                print("db2 connected")
            df_res=pd.merge(df_src, df_tgt, on="COLUMN_NAME")
            df_res['status'] = np.where(df_res['DATA_TYPE_x'] == df_res['DATA_TYPE_y'], 'True', 'False')
            print(df_res)
            if 'False' in df_res['status']:
                add_row = [tid, 'Datatype_Check', source_Table_Name, source_database,
                                    Target_Table_name, Target_database, 'fail']
                df_datatype.loc[x] = add_row
                df_datatype.to_excel(fileDir + '\\' + 'Report_for_Datatype_Check' + '.xlsx',
                                            index=False)
    
            else:
                add_row = [tid, 'Datatype_Check', source_Table_Name, source_database,
                                    Target_Table_name, Target_database, 'success']
                df_datatype.loc[x] = add_row
                df_datatype.to_excel(fileDir + '\\' + 'Report_for_Datatype_Check' + '.xlsx',
                                    index=False)
          

        end = time.time()
        print("Time: ", end - start)
        print(len(df_datatype))
        return df_datatype
