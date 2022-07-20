from logging import exception
from flask import Flask, render_template, redirect, url_for, request, flash, send_file
import teradatasql
import psycopg2
import snowflake.connector
import nzpy
import pandas as pd
import time
import plotly.graph_objs as go
import numpy as np
import datacompy
import os
import re
#import boto3
#from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions
from count import count, count_file
from duplicate_file import duplicate, duplicate_file
from null_file import null, null_file
from default_file import default, default_file
from datatype_file import datatype
from pk import pk_file, pkey
from query import cust_query
from structure import structure, structure_file
from bi import bi
from full_comp_new import full_file,full
from stats import stat, stat_file
from zipfile import ZipFile
from special import special, special_file
from max import max, max_file
from min import min, min_file
from sum import sum, sum_file
import sqlite3
import pyodbc
import xlwings as xw
import pathlib
import cx_Oracle


app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config["UPLOAD_FOLDER1"]= ""
v = False
SQLALCHEMY_TRACK_MODIFICATIONS = False

@app.route('/')
def index():
    return render_template("index.html")


@app.route('/Connection', methods=['GET', 'POST'])
def Connection():

    global db1, db2, hostname, password, userconnect, v, cur, datanamedb1, datanamedb2
    if request.method == 'POST':
        v = False
        userDetail = request.form
        try:
            hostname = userDetail['hostname']
            userconnect = userDetail['username']
            datanamedb1 = userDetail['datanamedb1']
            if datanamedb1 == 'Postgre Sqldb1':
                password = userDetail['password']
                try:
                    db1 = psycopg2.connect(host=hostname, user=userconnect, password=password)
                    #cur = db1.cursor()
                    flash("Successfully connected in Source Database")
                    return render_template("Connection.html")
                except (Exception, psycopg2.DatabaseError) as e:
                    flash(e)
                    return render_template("Connection.html")
                return redirect(url_for('home', db1=db1, v=v,cur=cur,datanamedb1= datanamedb1))

            elif datanamedb1 == 'Netezza_db1':
                password = userDetail['password']
                database = userDetail['database']

                try:
                    db1 = nzpy.connect(user=userconnect, password=password, host=hostname, port=5480, database=database, securityLevel=1, logLevel=0)
                    cur = db1.cursor()
                    flash("Successfully connected in Source Database")
                    return render_template("Connection.html")
                except (Exception, nzpy.DatabaseError) as e:
                    flash(e)
                    return render_template("Connection.html")
                return redirect(url_for('home', db1=db1, v=v, cur=cur,datanamedb1= datanamedb1))

            elif datanamedb1 == 'Snowflakedb1':
                password = userDetail['password']
                try:
                    db1 = snowflake.connector.connect(account=hostname, user=userconnect, password=password)
                    cur = db1.cursor()
                    flash("Successfully connected in Source Database")
                    return render_template("Connection.html")
                except (Exception, snowflake.connector.DatabaseError) as e:
                    if e.errno==250001:
                        flash('Incorrect username or password')
                    return render_template("Connection.html")
                return redirect(url_for('home', db1=db1, v=v,cur=cur))

            elif datanamedb1 == "Oracledb1":
                password = userDetail['password']
                service_name = userDetail['service_name']
                try:
                    dsn_tns = cx_Oracle.makedsn(hostname, '1523', service_name=service_name) # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
                    db1 = cx_Oracle.connect(user=userconnect, password=password, dsn=dsn_tns)    
                    flash("Successfully connected in Source Database")
                    return render_template("Connection.html")
                except (Exception, pyodbc.connect.DatabaseError) as e:
                    flash(e)
                    return render_template("Connection.html")
                return redirect(url_for('home', db1=db1, v=v,datanamedb1= datanamedb1))

            elif datanamedb1 == 'SQL_Server_db1':
                try:
                    database = userDetail['database']
                    driver = str('{ODBC Driver 17 for SQL Server}')
                    key = 'DRIVER=' + driver + ';SERVER=' + hostname + ';DATABASE=' + database + ';UID=' + userconnect + ';trusted_connection=yes;'
                    db1 = pyodbc.connect(key)
                    cursor = db1.cursor()
                    flash("Successfully connected in Source Database")
                    return render_template("Connection.html")
                except (Exception, pyodbc.connect.DatabaseError) as e:
                    flash(e)
                    return render_template("Connection.html")
                return redirect(url_for('home', db1=db1, v=v,datanamedb1= datanamedb1))


            elif datanamedb1 == 'Teradatadb1':
                password = userDetail['password']
                try:
                    db1 = teradatasql.connect(host=hostname, user=userconnect, password=password)
                    cur = db1.cursor()
                    flash("Successfully connected in Source Database")
                    return render_template("Connection.html")
                except (Exception, teradatasql.DatabaseError) as e:
                    flash(e)
                    return render_template("Connection.html")
                return redirect(url_for('home', db1=db1, v=v))
        except:
            try:
                datanamedb2 = userDetail['datanamedb2']
                hostname = userDetail['hostname']
                userconnect = userDetail['username']
                print(hostname, userconnect, datanamedb2)
                if datanamedb2 == 'Postgre Sqldb2':
                    password = userDetail['password']
                    try:
                        db2 = psycopg2.connect(host=hostname, user=userconnect, password=password)
                        flash("Successfully connected in Target Database")
                        return render_template("Connection.html")
                    except (Exception, psycopg2.DatabaseError) as e:
                        flash(e)
                        return render_template("Connection.html")
                    return redirect(url_for('home', db2=db2, v=v))

                elif datanamedb2 == 'Netezza_db2':
                    password = userDetail['password']
                    database = userDetail['database']

                    try:
                        db2 = nzpy.connect(user=userconnect, password=password, host=hostname, port=5480,
                                           database=database, securityLevel=1, logLevel=0)
                        cur = db2.cursor()
                        flash("Successfully connected in Target Database")
                        return render_template("Connection.html")
                    except (Exception, nzpy.DatabaseError) as e:
                        flash(e)
                        return render_template("Connection.html")
                    return redirect(url_for('home', db2=db2, v=v, cur=cur,datanamedb2= datanamedb2))

                elif datanamedb2 == 'SQL_Server_db2':
                    try:
                        database = userDetail['database']
                        driver = str('{ODBC Driver 17 for SQL Server}')
                        key = 'DRIVER=' + driver + ';SERVER=' + hostname + ';DATABASE=' + database + ';UID=' + userconnect + ';trusted_connection=yes;'
                        db2 = pyodbc.connect(key)
                        cursor = db2.cursor()
                        flash("Successfully connected in Target Database")
                        return render_template("Connection.html")
                    except (Exception, pyodbc.connect.DatabaseError) as e:
                        flash(e)
                        return render_template("Connection.html")
                    return redirect(url_for('home', db2=db2, v=v,datanamedb2= datanamedb2))
                elif datanamedb2 == 'Snowflakedb2':
                    try:
                        password = userDetail['password']
                        db2 = snowflake.connector.connect(account=hostname, user=userconnect,
                                                          password=password)
                        flash("Successfully connected in Target Database")
                        return render_template("Connection.html")
                    except (Exception, snowflake.connector.DatabaseError) as e:
                        flash(e)
                        return render_template("Connection.html")
                    return redirect(url_for('home', db2=db2, v=v))
                elif datanamedb2 == 'Teradatadb2':
                    password = userDetail['password']
                    try:
                        db2 = teradatasql.connect(host=hostname, user=userconnect, password=password)
                        flash("Successfully connected in Target Database")
                        return render_template("Connection.html")
                    except (Exception, teradatasql.DatabaseError) as e:
                        flash(e)
                        return render_template("Connection.html")
                    return redirect(url_for('home', db2=db2, v=v))
                return render_template('Connection.html')
            except Exception as e:
                flash(e)
                return render_template("Connection.html")
    return render_template('Connection.html')

@app.route('/File', methods=['GET', 'POST'])
def File():
    global df_src, db2, s3, container_client
    global v, filedb, databasee
    if request.method == 'POST':
        fileDetail = request.form
        try:
            filedb = fileDetail['filedb']
            if fileDetail['filedb'] == 'Azure Blob':
                try:
                    conn_str = fileDetail['conn_str']
                    sas = fileDetail['container']

                    container_client = BlobServiceClient(account_url=conn_str,
                                                         credential=sas)
                    flash("Successfully Connected to Source File")
                    v = True
                    return render_template("File.html")
                except:
                    flash("Connection Error! Please try again.")
                    return render_template("File.html")
                return redirect(url_for('home', container_client=container_client,filedb=filedb))
            elif fileDetail['filedb'] == 'aws':
                try:
                    service = fileDetail['service']
                    region = fileDetail['region']
                    id = fileDetail['id']
                    name = fileDetail['name']
                    s3 = boto3.resource(
                        service_name=service,
                        region_name=region,
                        aws_access_key_id=id,
                        aws_secret_access_key=name)
                    flash("Successfully Connected to Source File")
                    v = True
                    return render_template("File.html")
                except:
                    flash("Connection Error! Please try again.")
                    return render_template("File.html")
                return redirect(url_for('home', s3=s3,filedb=filedb))

        except:
            databasee = fileDetail['database']
            usernaam  =fileDetail['username']
            host = fileDetail['hostname']

            if fileDetail['database'] == 'Snowflake':
                passw = fileDetail['password']
                try:
                    db2 = snowflake.connector.connect(account=host, user=usernaam, password=passw)
                    flash("Successfully connected in Target Database")
                    return render_template("File.html")
                except (Exception, snowflake.connector.DatabaseError) as e:
                    flash("Connection Error! Please try again.")
                    return render_template("File.html")
                return redirect(url_for('home', db2=db2))

            elif fileDetail['database'] == 'Netezza':
                passw = fileDetail['password']
                db = fileDetail['db']
                try:
                    db2 = nzpy.connect(user=usernaam, password=passw, host=host, port=5480, database=db, securityLevel=1, logLevel=0)
                    cur = db1.cursor()
                    flash("Successfully connected in Target Database")
                    return render_template("File.html")
                except (Exception, nzpy.DatabaseError) as e:
                    flash(e)
                    return render_template("File.html")
                return redirect(url_for('home', db2=db2, databasee=databasee))

            elif fileDetail['database']== 'SQL Server':
                try:
                    db = fileDetail['db']
                    driver = str('{ODBC Driver 17 for SQL Server}')
                    key = 'DRIVER=' + driver + ';SERVER=' + host + ';DATABASE=' + db + ';UID=' + usernaam + ';trusted_connection=yes;'
                    db2 = pyodbc.connect(key)
                    cursor = db2.cursor()
                    flash("Successfully connected in Target Database")
                    return render_template("File.html")
                except (Exception, pyodbc.connect.DatabaseError) as e:
                    flash(e)
                    return render_template("File.html")
                return redirect(url_for('home', db2=db2,databasee=databasee))
            elif fileDetail['database'] == 'Postgre Sql':
                passw = fileDetail['password']
                try:
                    db2 = psycopg2.connect(host=host, user=usernaam, password=passw)
                    flash("Successfully connected in Target Database")
                    return render_template("File.html")
                except (Exception, psycopg2.DatabaseError) as e:
                    flash(e)
                    return render_template("File.html")
                return redirect(url_for('home', db2=db2))
            elif fileDetail['database'] == 'Teradata':
                passw = fileDetail['password']
                try:
                    db2 = teradatasql.connect(host=host, user=usernaam, password=passw)
                    flash("Successfully connected in Target Database")
                    return render_template("File.html")
                except (Exception, teradatasql.DatabaseError) as e:
                    flash(e)
                    return render_template("File.html")
                return redirect(url_for('home', db2=db2))

        v = True
        return render_template("File.html")
        return redirect(url_for('home', v=v, db2=db2))
    return render_template("File.html")


@app.route('/upload',methods=['GET', 'POST'])
def upload():
    global upload_file
    global Entryfile,esheets
    if request.method == 'POST':
        upload_file = request.files['file']
        file_path = os.path.join(app.config["UPLOAD_FOLDER1"], upload_file.filename)
        upload_file.save(file_path)
        print(upload_file, ",", file_path)
        if pathlib.Path(file_path).suffix==".xlsx" or pathlib.Path(file_path).suffix==".xls":
            try:
                wb = xw.Book(file_path)
                sheet = wb.sheets['TestCases']
                sheet2 = wb.sheets['File_db']
                sheet3 = wb.sheets['Date']
                sheet4 = wb.sheets['User_Query']
                sheet5 = wb.sheets['default_check']
                sheet6 = wb.sheets['Special_char']
                sheet7 = wb.sheets['PK_check']
                sheet8 = wb.sheets['DataType_check']
                sheet9= wb.sheets['UI_Validation']
                # put ur all sheet in list
                sheets = [sheet, sheet2,sheet3,sheet4,sheet5,sheet6,sheet7,sheet8,sheet9]
                # create empty list
                esheets = []
                for i, j in zip(range(len(sheets)), sheets):
                    name = 'df' + str(i)
                    print(name)
                    name = j['A1:Z100'].options(pd.DataFrame, index=False, header=True).value
                    name = name.dropna(how='all')
                    name = name.loc[:, name.columns.notnull()]
                    esheets.append(name)
                # wb.close()
                os.system('taskkill/IM EXCEL.EXE')
                return redirect(url_for('home',esheets=esheets))
            except Exception as e:
                flash(e)
                return render_template("upload.html")
        else:
            flash("Please upload valid extension file")
            return render_template("upload.html")
    return render_template("upload.html")



@app.route('/home', methods=['GET', 'POST'])
#@login_required
def home():
    if request.method == 'POST':
        f= False
        global Report_for_Count_Check,Report_for_Duplicate_Check,Report_for_Null_Check, data_length, data_date,data_full,data_stats, data_default, data_special
        l=[]
        file_name = []
        fileDir = os.path.dirname(os.path.realpath('__file__'))
        dfs = []
        empty_dfs = []
        graph = pd.DataFrame()

        if request.form.get("bi"):
            report = esheets[8]
            df_bi = bi(report, cur, fileDir)
            data_bi = df_bi[:3].to_html(classes='mystyle', index=False)
            l.append("Report Validation Check")
            l.append(data_bi)
            dfs.append(df_bi)
            file_name.append("Report for BI_Check.xlsx")

        if request.form.get("struct"):
            try:
                structt = esheets[0]
                struct_file = esheets[1]
                if v==True:
                    try:
                        cloud=s3
                        df_struct = structure_file(db2, fileDir, cloud, struct_file)
                    except:
                        cloud = container_client
                        df_struct = structure_file(db2, fileDir, cloud, struct_file)
                else:
                    df_struct = structure(db1, db2, fileDir,structt)
                data_struct = df_struct[:3].to_html(classes='mystyle', index=False)
                l.append("Report For Structure Validation Check")
                l.append(data_struct)
                dfs.append(df_struct)
                file_name.append("Report_for_Structure_Check.xlsx")
            except:
                return render_template("home.html")

        if request.form.get("pk"):
            try:
                pk= esheets[6]
                if v==True:
                    try:
                        cloud=s3
                        df_pk = pk_file(db2, fileDir,cloud,pk)
                    except:
                        cloud= container_client
                        df_pk = pk_file(db2, fileDir, cloud, pk)
                else:
                    df_pk= pkey(db2, fileDir, pk)
                data_pk = df_pk[:3].to_html(classes='mystyle', index=False)
                l.append("Report For Primary Key Check")
                l.append(data_pk)
                empty_dfs.append(df_pk)
                file_name.append("Report for Primary_Key_Check.xlsx")
            except:
                return render_template("home.html")

        if request.form.get("Special"):
            spe = esheets[5]
            if v == True:
                try:
                    cloud = s3
                    df_special = special_file(db2, fileDir, cloud,spe)
                except:
                    cloud = container_client
                    df_special = special_file(db2, fileDir, cloud,spe)
            else:
                df_special = special(db1,db2, fileDir,spe)

            data_special= df_special[:3].to_html(classes='mystyle', index=False)
            l.append("Report For Special Character Check")
            l.append(data_special)
            dfs.append(df_special)
            file_name.append("Report for Special_Char_Check.xlsx")
        
        if request.form.get("length check"):
            
            try:
                data= esheets[7]
                print(data)
                df_length = datatype(db1,db2, fileDir,data, datanamedb1, datanamedb2)
                print(df_length)
                data_length= df_length[:3].to_html(classes='mystyle', index=False)
                print(data_length)
                l.append("Report For Data Type Check")
                l.append(data_length)
                dfs.append(df_length)
                print(dfs)
                file_name.append("Report_for_Datatype_Check.xlsx")
            except Exception as e:
                flash(e)
                return render_template("home.html")
            
        if request.form.get("Date format check"):
            #main = openpyxl.load_workbook(r'C:\Users\dsingh35\OneDrive - Capgemini\Desktop\DV_Testcase.xlsx')
            dmain = esheets[2]
            print ("Reading Sheet....",dmain)
            NR = dmain.shape[0]
            print("Total no of rows in sheet : ", NR)
            NC = dmain.shape[1]
            print("Total no of Cols in sheet : ", NC)
            df_date = pd.DataFrame(columns = ['TestCase_ID','Test_Type','Database_Name', 'Table_Name', 'Column_name', 'Status'])
            try:
                for d in range(0,NR):
                    t = d + 2
                    print('tid',t)
                    tid = str(dmain.iloc[d]["TestCase Id"])
                    name = str(dmain.iloc[d]["DB"])
                    username = str(dmain.iloc[d]["User"])
                    databasename = str(dmain.iloc[d]["Database"])
                    tablename = str(dmain.iloc[d]["TableName"])
                    columnname =str(dmain.iloc[d]["Column"])
                    priority_column=str(dmain.iloc[d]["Priority Column(Y/N)"])
                    if priority_column =="Y":  
                        start=time.time()
                        if name == 'PostgreSql':
                            try:
                                try:
                                    print('1')
                                    query =  "SELECT data_type FROM information_schema.columns WHERE table_catalog = '"+username+"' and table_schema = '"+databasename+"' and table_name = '"+tablename+"' and column_name = '"+columnname+"'"
                                    print(query)
                                    df=pd.read_sql_query(query,db1)
                                    print(df.iloc[0][0])
                                    if df.iloc[0][0]== 'Date'.lower() or df.iloc[0][0] == "Date":
                                        add_rows = [tid,'Date_Check',databasename,tablename, columnname, 'Success']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir +'\\'+'Report for Date_Check'+'.xlsx', index = False)
                                    else:
                                        add_rows = [tid,'Date_Check',databasename,tablename, columnname, 'Fail']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir +'\\'+'Report for Date_Check'+'.xlsx', index = False)
                                except:
                                    query = "SELECT data_type FROM information_schema.columns WHERE table_catalog = '" + username + "' and table_schema = '" + databasename + "' and table_name = '" + tablename + "' and column_name = '" + columnname + "'"
                                    print(query)
                                    df = pd.read_sql_query(query, db2)
                                    print(df.iloc[0][0])
                                    if df.iloc[0][0] == 'Date'.lower() or df.iloc[0][0] == "Date":
                                        add_rows = [tid, 'Date_Check', databasename, tablename, columnname, 'Success']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir + '\\' + 'Report for Date_Check' + '.xlsx',
                                                         index=False)
                                    else:
                                        add_rows = [tid, 'Date_Check', databasename, tablename, columnname, 'Fail']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir + '\\' + 'Report for Date_Check' + '.xlsx',
                                                         index=False)
                            except Exception as e:
                                flash(e)
                                return render_template("home.html")
                        elif name=="Teradata":
                            try:
                                try:
                                    print('2')
                                    query = "SELECT TYPE("+databasename+"."+tablename+"."+columnname+")"
                                    print(query)
                                    df=pd.read_sql_query(query,db2)
                                    print(df.iloc[0][0])
                                    if df.iloc[0][0].replace(" ", "") == 'Date'.upper() or df.iloc[0][0].replace(" ", "") == "Date":
                                        print('Yes')
                                        add_rows = [tid,'Date_Check',databasename,tablename, columnname,  'Success']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir +'\\'+'Report for Date_Check'+'.xlsx', index = False)
                                    else:
                                        add_rows = [tid,'Date_Check',databasename,tablename, columnname, 'Fail']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir +'\\'+'Report for Date_Check'+'.xlsx', index = False)
                                except:
                                    query = "SELECT TYPE(" + databasename + "." + tablename + "." + columnname + ")"
                                    print(query)
                                    df = pd.read_sql_query(query, db1)
                                    print(df.iloc[0][0])
                                    if df.iloc[0][0].replace(" ", "") == 'Date'.upper() or df.iloc[0][0].replace(" ",
                                                                                                                 "") == "Date":
                                        print('Yes')
                                        add_rows = [tid, 'Date_Check', databasename, tablename, columnname, 'Success']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir + '\\' + 'Report for Date_Check' + '.xlsx',
                                                         index=False)
                                    else:
                                        add_rows = [tid, 'Date_Check', databasename, tablename, columnname, 'Fail']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir + '\\' + 'Report for Date_Check' + '.xlsx',
                                                         index=False)
                            except Exception as e:
                                flash(e)
                                return render_template("home.html")
                        elif name == "Snowflake":
                            try:
                                try:
                                    print('3')
                                    query = "desc table "+ databasename + "." + tablename
                                    print(query)
                                    df = pd.read_sql_query(query, db2)
                                    df = df[df['name'] == columnname]
                                    s = df.iloc[0][1]
                                    result = re.sub(r"[^a-zA-Z]", "", s).lower()
                                    if result == 'Date'.upper() or result == "date":
                                        add_rows = [tid, 'Date_Check', databasename, tablename, columnname, 'Success']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir + '\\' + 'Report for Date_Check' + '.xlsx',
                                                         index=False)
                                    else:
                                        add_rows = [tid, 'Date_Check', databasename, tablename, columnname, 'Fail']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir + '\\' + 'Report for Date_Check' + '.xlsx',
                                                         index=False)
                                except:
                                    query = "desc table " + databasename + "." + tablename
                                    print(query)
                                    df = pd.read_sql_query(query, db1)
                                    df = df[df['name'] == columnname]
                                    s = df.iloc[0][1]
                                    result = re.sub(r"[^a-zA-Z]", "", s).lower()
                                    if result == 'Date'.upper() or result == "date":
                                        add_rows = [tid, 'Date_Check', databasename, tablename, columnname, 'Success']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir + '\\' + 'Report for Date_Check' + '.xlsx',
                                                         index=False)
                                    else:
                                        add_rows = [tid, 'Date_Check', databasename, tablename, columnname, 'Fail']
                                        df_date.loc[d] = add_rows
                                        df_date.to_excel(fileDir + '\\' + 'Report for Date_Check' + '.xlsx',
                                                         index=False)
                            except Exception as e:
                                flash(e)
                                return render_template("home.html")
                        end=time.time()
            except:
                flash("Connection Error! Please Check Your Connection")
                return render_template("home.html")
            print("Time: ",end-start) 
            data_date=df_date[:3].to_html(classes='mystyle', index=False)
            l.append("Report For Date Format Check")
            l.append(data_date)
            dfs.append(df_date)
            file_name.append("Report for Date_Check.xlsx")  

        if request.form.get("query check"):
            try:
                queryy = esheets[3]
                df_q = cust_query(db1,db2,fileDir,file_name,queryy)
                if df_q.shape[0]!=0:
                    data_query= df_q[:3].to_html(classes='mystyle', index=False)
                    l.append("Report For Custom Query Check")
                    l.append(data_query)
                    dfs.append(df_q)
                    # file_name.append("Report for Query_Check.xlsx")
                    file_name.append("Query_Check_Textfiles.zip")
                else:
                    file_name.append("Query_Check_Textfiles.zip")
            except Exception as e:
                flash(e)
                return render_template("home.html")


        if request.form.get("default"):
                defaultt = esheets[4]
                if v==True:
                    try:
                        cloud = s3
                        df_default= default_file(db2, fileDir, cloud,defaultt )
                    except:
                        cloud = container_client
                        print(cloud)
                        df_default = default_file(db2, fileDir, cloud, defaultt)
                else:
                    df_default= default(db1, db2, fileDir,defaultt)
                data_default=df_default[:3].to_html(classes='mystyle', index=False)
                l.append("Report for Default Check")
                l.append(data_default)
                dfs.append(df_default)
                file_name.append("Report for Default_Check.xlsx")

                    
        if request.form.get("full comparison"):
            fulll = esheets[0]
            full_fil = esheets[1]
            if v == True:
                try:
                    cloud = s3
                    df_full = full_file(db2, fileDir, cloud,full_fil,filedb, databasee)
                except:
                    cloud = container_client
                    df_full = full_file(db2, fileDir, cloud, full_fil,filedb, databasee)
                    #print("df_full",df_full)
            else:
                df_full = full(db1, db2, fileDir, fulll,datanamedb1, datanamedb2)
            data_full=df_full[:3].to_html(classes='mystyle', index=False)
            l.append("Report for Full Comparison Check")
            l.append(data_full)
            dfs.append(df_full)
            file_name.append("Full_Comparison_Detail_Reports.zip")
            file_name.append("Report_for_Full_Check.xlsx")
            file_name.append("Report_for_newfull_Check.xlsx")

        if request.form.get("Max"):
            try:
                maxx = esheets[0]
                max_fil = esheets[1]
                if v == True:
                    try:
                        cloud=s3
                        df_max=max_file(db2, fileDir, cloud,max_fil,file_name)
                    except:
                        cloud=container_client
                        df_max = max_file(db2, fileDir, cloud,max_fil,file_name)
                        #print(df_stat)
                else:
                    df_max= max(db1, db2, fileDir, maxx,file_name)

                data_max=df_max[:3].to_html(classes='mystyle', index=False)
                l.append("Report for Maximum Value Check")
                l.append(data_max)
                dfs.append(df_max)
                file_name.append("Max_Check_Report.zip")
                #file_name.append("Report for Max. Value.xlsx")
            except Exception as e:
                flash(e)
                return render_template("home.html")

        if request.form.get("Min"):
            try:
                minn = esheets[0]
                min_fil = esheets[1]
                if v == True:
                    try:
                        cloud = s3
                        df_min = min_file(db2, fileDir, cloud, min_fil, file_name)
                    except:
                        cloud = container_client
                        df_min = min_file(db2, fileDir, cloud, min_fil, file_name)
                        # print(df_stat)
                else:
                    df_min = min(db1, db2, fileDir, minn, file_name)

                data_min=df_min[:3].to_html(classes='mystyle', index=False)
                l.append("Report for Minimum Value Check")
                l.append(data_min)
                dfs.append(df_min)
                file_name.append("Min_Check_Report.zip")
                #file_name.append("Report for Min. Value.xlsx")
            except Exception as e:
                flash(e)
                return render_template("home.html")

        if request.form.get("Mean"):
            smain = esheets[0]
            print ("Reading Sheet....",smain)
            NR = smain.shape[0]
            print("Total no of rows in sheet : ", NR)
            NC = smain.shape[1]
            print("Total no of Cols in sheet : ", NC)
            headers = ['Test Case Id','Test Type','Status', 'Source Count', 'Target Count']
            df_report = pd.DataFrame(columns = headers)
            df_report1 = pd.DataFrame(columns = ['TestId','Check Type','Source Tablename', 'Status','Target Tablename', 'Status'])
            df_mean =pd.DataFrame(columns = ['Test_CaseId','Test_Type', 'Source_TableName', 'Target_TableName', 'Status'])
            try:
                for i in range(0,NR):
                    y = i + 2
                    start1_time = time.time()
                    tid = smain.iloc[i]["Test Case ID"]
                    print("Executing TestCaseID - " + tid)
                    #ttype = smain.iloc[i]["Test CaseType"]
                    source_databasename=str(smain.iloc[i]["Source DataBase"])
                    source_tablename=str(smain.iloc[i]["Source Table Name"])
                    target_databasename=str(smain.iloc[i]["Target Database"])
                    target_tablename=str(smain.iloc[i]["Target Table Name"])
                    priority_column=smain.iloc[i]["Priority Column(Y/N)"]
                    if priority_column =="Y":
                        start=time.time()
                        if source_databasename == "None" or source_tablename == "None":
                            try:
                                Query2="select * from " + target_databasename +"." + target_tablename+";"
                                df_tgt = pd.read_sql_query(Query2,db2)
                                tmean = df_tgt.max(numeric_only = True)
                                add_row = [tid,"Mean_Check","None",target_tablename,"None"]
                                df_mean.loc[i] = add_row
                                fileDir = os.path.dirname(os.path.realpath('__file__'))
                                print(fileDir)        
                                f=open(fileDir+'\\'+str(tid)+'_mean'+'.txt',"w")
                                print('Target',file=f)
                                print(tmean.round(2), file=f)
                                f.close()
                            #     df_tgt.to_excel(fileDir +'\\'+'Report for std Value'+'.xlsx')
                            except Exception as e:
                                flash(e)
                                return render_template("home.html")
                        elif target_databasename == "None" or target_tablename == "None":
                            try:
                                Query1="select * from " + source_databasename +"." + source_tablename+";"
                                df_src = pd.read_sql_query(Query1,db1)
                                smean = df_src.max(numeric_only = True)
                                add_row = [tid,"Mean_Check",source_tablename,"None","None"]
                                df_mean.loc[i] = add_row
                                fileDir = os.path.dirname(os.path.realpath('__file__'))
                                print(fileDir)        
                                f=open(fileDir+'\\'+str(tid)+'_mean'+'.txt',"w")
                                print('Source',file=f)
                                print(smean.round(2), file=f)
                                f.close()
                                # df_src.to_excel(fileDir +'\\'+'Report for std Value'+'.xlsx')
                            except Exception as e:
                                flash(e)
                                return render_template("home.html")
                        else:
                            try:
                                Query1="select * from " + source_databasename +"." + source_tablename+";"
                                Query2="select * from " + target_databasename +"." + target_tablename+";"
                                df_src = pd.read_sql_query(Query1,db1)
                                df_tgt = pd.read_sql_query(Query2,db2)
                                smean = df_src.max(numeric_only = True)
                                tmean = df_tgt.max(numeric_only = True)
                                stmean = smean == tmean
                                df = stmean.to_frame()
                                if df[(df.values.ravel() == False).reshape(df.shape).any(1)].shape[0]>0:
                                    add_row = [tid, 'Stats_Mean Check',source_tablename, target_tablename, 'Fail' ]
                                    df_mean.loc[i] = add_row
                                    df_mean.to_excel(fileDir +'\\'+'Report for mean Value'+'.xlsx', index = False)
                                else:
                                    add_row = [tid, 'Stats_Mean Check',source_tablename, target_tablename, 'Success' ]
                                    df_mean.loc[i] = add_row
                                    df_mean.to_excel(fileDir +'\\'+'Report for mean Value'+'.xlsx', index = False)
                                fileDir = os.path.dirname(os.path.realpath('__file__'))
                                print(fileDir)
                                f=open(fileDir+'\\'+str(tid)+'_mean'+'.txt',"w")
                                print('Source',file=f)
                                print('-----------------',file=f)
                                print(smean.round(2), file=f)
                                print('\n',file=f)
                                print('Target',file=f)
                                print('-----------------',file=f)
                                print(tmean.round(2), file=f)
                                f.close()
                                file_name.append("Report for mean Value.xlsx")
                            except Exception as e:
                                flash(e)
                                return render_template("home.html")
                        end=time.time()
            except:
                flash("Connection Error! Please Check Your Connection")
                return render_template("home.html")
            print("Time: ",end-start)
            with ZipFile('Mean_Check_Textfiles.zip', 'w') as zipObj2:   
                for k in range(0,NR):
                    priority_column=smain.iloc[k]["Priority Column(Y/N)"]
                    if priority_column =="Y":
                        id = smain.iloc[k]["Test Case ID"]
                        filename_max = str(id)+"_mean.txt"
                        zipObj2.write(filename_max)
            data_mean=df_mean[:3].to_html(classes='mystyle', index=False)
            l.append("Report for Mean Value Check")
            l.append(data_mean)
            dfs.append(df_mean)
            file_name.append("Mean_Check_Textfiles.zip")
            #file_name.append("Report for mean Value.xlsx")
        
        if request.form.get("Std"):
            smain = esheets[0]
            print ("Reading Sheet....",smain)
            NR = smain.shape[0]
            print("Total no of rows in sheet : ", NR)
            NC = smain.shape[1]
            print("Total no of Cols in sheet : ", NC)
            headers = ['Test Case Id','Test Type','Status', 'Source Count', 'Target Count']
            df_report = pd.DataFrame(columns = headers)
            df_report1 = pd.DataFrame(columns = ['TestId','Check Type','Source Tablename', 'Status','Target Tablename', 'Status'])
            df_std =pd.DataFrame(columns = ['Test_CaseId','Test_Type', 'Source_TableName', 'Target_TableName', 'Status'])
            try:
                for i in range(0,NR):
                    y = i + 2
                    start1_time = time.time()
                    tid = smain.iloc[i]["Test Case ID"]
                    print("Executing TestCaseID - " + tid)
                    #ttype = smain.iloc[i]["Test CaseType"]
                    source_databasename=str(smain.iloc[i]["Source DataBase"])
                    source_tablename=str(smain.iloc[i]["Source Table Name"])
                    target_databasename=str(smain.iloc[i]["Target Database"])
                    target_tablename=str(smain.iloc[i]["Target Table Name"])
                    priority_column=smain.iloc[i]["Priority Column(Y/N)"]
                    if priority_column =="Y":
                        start=time.time()
                        if source_databasename == "None" or source_tablename == "None":
                            try:
                                Query2="select * from " + target_databasename +"." + target_tablename+";"
                                df_tgt = pd.read_sql_query(Query2,db2)
                                tstd1 = df_tgt.std(numeric_only = True)
                                add_row = [tid,"Stddv_Check","None",target_tablename,"None"]
                                df_std.loc[i] = add_row
                                fileDir = os.path.dirname(os.path.realpath('__file__'))
                                print(fileDir)        
                                f=open(fileDir+'\\'+str(tid)+'_std'+'.txt',"w")
                                print('Target',file=f)
                                print(tstd1.round(2), file=f)
                                f.close()
                            #     df_tgt.to_excel(fileDir +'\\'+'Report for std Value'+'.xlsx')
                            except Exception as e:
                                flash(e)
                                return render_template("home.html")
                        elif target_databasename == "None" or target_tablename == "None":
                            try:
                                Query1="select * from " + source_databasename +"." + source_tablename+";"
                                df_src = pd.read_sql_query(Query1,db1)
                                sstd1 = df_src.std(numeric_only = True)
                                add_row = [tid,"Stddv_Check",source_tablename,"None","None"]
                                df_std.loc[i] = add_row
                                fileDir = os.path.dirname(os.path.realpath('__file__'))
                                print(fileDir)        
                                f=open(fileDir+'\\'+str(tid)+'_std'+'.txt',"w")
                                print('Source',file=f)
                                print(sstd1.round(2), file=f)
                                f.close()
                                # df_src.to_excel(fileDir +'\\'+'Report for std Value'+'.xlsx')
                            except Exception as e:
                                flash(e)
                                return render_template("home.html")
                        else:
                            try:
                                Query1="select * from " + source_databasename +"." + source_tablename+";"
                                Query2="select * from " + target_databasename +"." + target_tablename+";"
                                df_src = pd.read_sql_query(Query1,db1)
                                df_tgt = pd.read_sql_query(Query2,db2)
                                sstd = df_src.std(numeric_only = True)
                                tstd = df_tgt.std(numeric_only = True)
                                ststd = sstd == tstd
                                df = ststd.to_frame()
                                if df[(df.values.ravel() == False).reshape(df.shape).any(1)].shape[0]>0:
                                    add_row = [tid, 'Stats_STD Check',source_tablename, target_tablename, 'Fail' ]
                                    df_std.loc[i] = add_row
                                    df_std.to_excel(fileDir +'\\'+'Report for std Value'+'.xlsx', index = False)
                                else:
                                    add_row = [tid, 'Stats_STD Check',source_tablename, target_tablename, 'Success' ]
                                    df_std.loc[i] = add_row
                                    df_std.to_excel(fileDir +'\\'+'Report for std Value'+'.xlsx', index = False)
                                fileDir = os.path.dirname(os.path.realpath('__file__'))
                                print(fileDir)
                                f=open(fileDir+'\\'+str(tid)+'_std'+'.txt',"w")
                                print('Source',file=f)
                                print('-----------------',file=f)
                                print(sstd.round(2), file=f)
                                print('\n',file=f)
                                print('Target',file=f)
                                print('-----------------',file=f)
                                print(tstd.round(2), file=f)
                                f.close()
                                file_name.append("Report for std Value.xlsx")
                            except Exception as e:
                                flash(e)
                                return render_template("home.html")
                        end=time.time()
            except:
                flash("Connection Error! Please Check Your Connection")
                return render_template("home.html")
            print("Time: ",end-start)
            with ZipFile('Std_Check_Textfiles.zip', 'w') as zipObj2:   
                for k in range(0,NR):
                    priority_column=smain.iloc[k]["Priority Column(Y/N)"]
                    if priority_column =="Y":
                        id = smain.iloc[k]["Test Case ID"]
                        filename_std = str(id)+"_std.txt"
                        zipObj2.write(filename_std)
            data_std=df_std[:3].to_html(classes='mystyle', index=False)
            l.append("Report for Standard Deviation Check")
            l.append(data_std)
            dfs.append(df_std)
            file_name.append("Std_Check_Textfiles.zip")
            #file_name.append("new_test3_std.txt")

        if request.form.get("Sum"):
            try:
                summ = esheets[0]
                sum_fil = esheets[1]
                if v == True:
                    try:
                        cloud = s3
                        df_sum = sum_file(db2, fileDir, cloud, sum_fil, file_name)
                    except:
                        cloud = container_client
                        df_sum = sum_file(db2, fileDir, cloud, sum_fil, file_name)
                        # print(df_stat)
                else:
                    df_sum = sum(db1, db2, fileDir, summ, file_name)

                data_sum = df_sum[:3].to_html(classes='mystyle', index=False)
                l.append("Report for Sum Value Check")
                l.append(data_sum)
                dfs.append(df_sum)
                file_name.append("Sum_Check_Report.zip")
                # file_name.append("Report for Sum Value.xlsx")
            except Exception as e:
                flash(e)
                return render_template("home.html")

        if request.form.get("Statistics"):
            try:
                statt = esheets[0]
                stat_fil = esheets[1]
                if v == True:
                    try:
                        cloud=s3
                        df_stat=stat_file(db2, fileDir, cloud,stat_fil,file_name)
                    except:
                        cloud=container_client
                        df_stat = stat_file(db2, fileDir, cloud,stat_fil,file_name)
                        #print(df_stat)
                else:
                    df_stat= stat(db1, db2, fileDir, statt,file_name)

                data_stats=df_stat[:3].to_html(classes='mystyle', index=False)
                l.append("Report for Statistics Check")
                l.append(data_stats)
                dfs.append(df_stat)
                #file_name.append("Report for Statistics.xlsx")
                file_name.append("Statistic_Check_Textfiles.zip")
            except Exception as e:
                flash(e)
                return render_template("home.html")

        if request.form.get("count"):
            try:
                countt = esheets[0]
                count_fil = esheets[1]
                if v == True:
                    try:
                        cloud=s3
                        df_count = count_file(db2, fileDir,cloud, count_fil)
                    except:
                        cloud=container_client
                        df_count = count_file(db2, fileDir, cloud, count_fil)

                else:
                    df_count= count(db1, db2, fileDir,countt)
                    print(df_count)

                Report_for_Count_Check = df_count[:3].to_html(classes='mystyle', index=False)
                l.append("Report for Count Check")
                l.append(Report_for_Count_Check)
                print(df_count)
                dfs.append(df_count)
                file_name.append("Report_for_Count_Check.xlsx")
            except Exception as e:
                flash(e)
                return render_template("home.html")


        if request.form.get("duplicate"):
            try:
                dupp = esheets[0]
                dupp_file = esheets[1]
                if v == True :
                    try:
                        cloud=s3
                        df_duplicate = duplicate_file(db2, fileDir, cloud,dupp_file)
                    except:
                        cloud = container_client
                        df_duplicate = duplicate_file(db2, fileDir, cloud, dupp_file)
                else:
                    df_duplicate = duplicate(db1, db2, fileDir,dupp)
                Report_for_Duplicate_Check = df_duplicate[:3].to_html(classes='mystyle', index=False)
                l.append("Report for Duplicate Check")
                l.append(Report_for_Duplicate_Check)
                dfs.append(df_duplicate)
                file_name.append("Report_for_Duplicate_Check.xlsx")
            except:
                return render_template("home.html")

        if request.form.get("Null check"):
            try:
                nulll=esheets[0]
                null_fil = esheets[1]
                if v==True:
                    try:
                        cloud=s3
                        df_null = null_file(db2, fileDir, cloud, null_fil)
                    except:
                        cloud = container_client
                        df_null = null_file(db2, fileDir, cloud, null_fil)


                else:
                    df_null = null(db1, db2, fileDir,nulll)
                Report_for_Null_Check= df_null[:3].to_html(classes='mystyle', index=False)
                l.append("Report for Null Check")
                l.append(Report_for_Null_Check)
                dfs.append(df_null)
                file_name.append("Report_for_Null_Check.xlsx")
            except:
                return render_template("home.html")

        with ZipFile('Check.zip', 'w') as zipObj2:
            print("files", file_name)
            for k in file_name:
                filename_summary = str(k)
                zipObj2.write(filename_summary)

        print("Dataframes:",dfs) 
        print("length: ", len(dfs))
        if len(dfs)>0:      
            for graphs in dfs:
                graph=graph.append(graphs, ignore_index=True)
            print(graph)
            newdf= graph.groupby(["Test_Type", "Status"]).size().reset_index(name='count')
            sdf = newdf[newdf['Status']=='Success']
            print(sdf)
            fdf = newdf[newdf['Status']=='Fail']
            print(fdf)
            if sdf.shape[0]>0 or fdf.shape[0]>0:
                trace1 = go.Bar(x=sdf['Test_Type'], y=sdf['count'], name = 'Success',width=.4, marker_color='green')
                trace2 = go.Bar(x=fdf['Test_Type'], y=fdf['count'], name = 'Fail',width=.4, marker_color='red')
                data_graph = [trace1,trace2]
                print("data_graph: ",data_graph)
                layout = go.Layout(paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)', title="Graphs for Checks",
                xaxis=dict(
                title="Test Type",
                linecolor="#FFFFFF", # Sets color of X-axis line
                showgrid=False # Removes X-axis grid lines
                ),
                yaxis=dict(
                title="Counts",
                linecolor="#FFFFFF", # Sets color of Y-axis line
                showgrid=False, # Removes Y-axis grid lines
                ))
                fig = go.Figure(data = data_graph, layout = layout)
                print("fig: ", fig)
                xlist1= list(sdf["Test_Type"])
                ylist1 = list(sdf["count"])
                xlist2= list(fdf["Test_Type"])
                ylist2 = list(fdf["count"])
                return render_template("common.html", listt=l, xlist1=xlist1,ylist1=ylist1, xlist2=xlist2,ylist2=ylist2)
            else:
                return render_template("common.html",listt=l)
        elif len(empty_dfs)>0:
            return render_template("common.html", listt=l)
        else:
            return render_template("query.html")
    return render_template("home.html")

@app.route('/common', methods=['GET', 'POST'])
def download_zip():
    print("welcome")
    path = "Check.zip"
    print(path)
    return send_file(os.path.join(os.getcwd(), path), as_attachment=True, cache_timeout=0)

@app.route('/report')
def download_file_report():
    path = "Report.zip"
    return send_file(os.path.join(os.getcwd(), path), as_attachment=True, cache_timeout=0)

if __name__ == "__main__":
    app.run(debug=True, port=5000)