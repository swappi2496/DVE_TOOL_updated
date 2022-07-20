import pandas as pd
import time
from flask import Flask, render_template, flash
import os
import numpy as np
from io import StringIO
def bi(report,cur,fileDir):
    smain = report
    print("Reading Sheet....", smain)
    NR = smain.shape[0]
    print("Total no of rows in sheet : ", NR)
    df_bi = pd.DataFrame(columns=['TestCase_ID', 'Test_Type','Database_Name', 'Status'])
    for i in range(0, NR):
        y = i + 2
        start1_time = time.time()
        tid = smain.iloc[i]["Test Case ID"]
        name = str(smain.iloc[i]["Database"])
        dbquery = str(smain.iloc[i]["Database Query"])
        biquery = str(smain.iloc[i]["Power BI Query"])
        cur.execute(dbquery)
        db = cur.fetchall()
        cur.execute(biquery)
        bi = cur.fetchall()
        if db == bi:
            row = [tid,"Report Validation", name, 'Success']
        else:
            row = [tid,"Report Validation", name, 'Fail']
        df_bi.loc[i] = row
        df_bi.to_excel(fileDir + '\\' + 'Report for BI_Check' + '.xlsx', index=False)
    return df_bi
