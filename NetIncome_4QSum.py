import pandas as pd
import numpy as np
import mysql.connector

cnx = mysql.connector.connect(user='root', password='techx1234', host='127.0.0.1', database='vinance')
cursor = cnx.cursor()


select_bs = "select * from  income where period like 'Q4 2019%' and type like '18.%'"

cursor.execute(select_bs)
incomeq4 = cursor.fetchall()

df = pd.DataFrame(incomeq4)
#print(df)
code_list = df[0]


for codes in code_list :
    #cursor2 = cnx.cursor(perpared=True)
    select_q3 = """select * from income where period like 'Q3 2019%' and type like '18.%' and code = %s"""
    cursor.execute(select_q3,(codes,))
    incomeq3 = cursor.fetchall()
    print(incomeq3)
#print(code_list)