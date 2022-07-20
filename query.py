import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
from io import StringIO
from zipfile import ZipFile
import xlwings as xw
def cust_query(db1,db2,fileDir,file_name,queryy):
    zip_list =[]
    qmain = queryy
    print ("Reading Sheet....",qmain)
    NR = qmain.shape[0]
    print("Total no of rows in sheet : ", NR)
    NC = qmain.shape[0]
    print("Total no of Cols in sheet : ", NC)
    df_q = pd.DataFrame(columns = ['TestCase_ID','Test_Type','Source Query','Target Query','Status'])
    for q in range(0,NR):
        print (q)
        tid = str(qmain.iloc[q]["Test Case ID"])
        sname = str(qmain.iloc[q]["Source Database"])
        tname = str(qmain.iloc[q]["Target Database"])
        squery = str(qmain.iloc[q]["Source Query"])
        tquery = str(qmain.iloc[q]["Target Query"])
        oper = str(qmain.iloc[q]["Operator"])
        prior = str(qmain.iloc[q]["Priority Column(Y/N)"])
        print(squery)
        if prior == "Y":
            print("yes")
            if squery == 'None' or sname == 'None':
                print(1)
                try:
                    tdf = pd.read_sql_query(tquery,db2)
                    print(tdf)
                    fileDir = os.path.dirname(os.path.realpath('__file__'))
                    print(fileDir)
                    zip_list.append(tid)
                    f=open(fileDir+'\\'+'user_query_'+str(tid)+'.txt',"w")
                    print(f)
                    print(tdf.head(20), file =f)
                    f.close()
                except Exception as e:
                    flash(e)
                    return render_template("home.html")

            elif tquery=='None' or tname == 'None':
                print(2)
                try:
                    sdf = pd.read_sql_query(squery,db1)
                    fileDir = os.path.dirname(os.path.realpath('__file__'))
                    zip_list.append(tid)
                    f=open(fileDir+'\\'+'user_query_'+str(tid)+'.txt',"w")
                    print(f)
                    print(sdf.head(20), file =f)
                    f.close()
                except Exception as e:
                    flash(e)
                    return render_template("home.html")
            else:
                print(3)
                try:
                    sdf = pd.read_sql_query(squery,db1)
                    print(sdf)
                    tdf = pd.read_sql_query(tquery,db2)
                    print(tdf)
                    sdf = sdf.astype('object')
                    tdf = tdf.astype('object')                
                    df = sdf.merge(tdf, indicator='join', how='outer').query('join == "left_only"').drop('join', axis=1)
                    print(df)
                    if df.shape[0]==0:
                        row = [tid,'Custom Query Check', squery,tquery, 'Success']
                    else:
                        row = [tid,'Custom Query Check',squery,tquery, 'Fail']
                        zip_list.append(tid)
                        fileDir = os.path.dirname(os.path.realpath('__file__'))
                        print(fileDir)
                        # f=open(fileDir+'\\'+'user_query_'+str(tid)+'.to_csv',"w")
                        # print(f)
                        # print(df.head(20), file =f)
                        # f.close()
                        df.to_csv(fileDir + '\\'+str(tid) +'Report for Detail_Query_Check'+'.xlsx', index = False)

                    df_q.loc[q] = row
                    df_q.to_excel(fileDir + '\\' +'Report for Query_Check'+'.xlsx', index = False)
                    file_name.append("Report for Query_Check.xlsx")
                except Exception as e:
                    flash(e)
                    return render_template("home.html")

        with ZipFile('Query_Check_Textfiles.zip', 'w') as zipObj2:
                for k,l in zip(range(0,NR),zip_list):
                    priority_column=str(qmain.iloc[k]["Priority Column(Y/N)"])
                    if priority_column =="Y":
                        filename = str(l)+'Report for Detail_Query_Check'+'.xlsx'
                        zipObj2.write(filename)
    return df_q
