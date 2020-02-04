from selenium import webdriver
import csv
import asyncio
from selenium .webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import pandas as pd
import numpy as np
import mysql.connector
from itertools import cycle
import time

#driver = webdriver.Chrome('C:\ChromeDriver\chromedriver')

async def load_tvsi(ticker):
    stock_url = 'https://finance.tvsi.com.vn/Enterprises/FinancialStatements?symbol={}'.format(ticker)
    driver.get(stock_url)
    driver.find_element_by_id('a_change_en').click()
    await asyncio.sleep(5)


def get_bs(ticker):
    fin_tr = driver.find_elements_by_xpath('//*[@id="table_bcdkt"]/tbody//tr')

    heading = fin_tr[0].find_elements_by_tag_name('td')

    title_array = []
    headercounter=0
    for header in heading:

        title_array.append(header.text)
        headercounter=headercounter+1

    row_data = []
    rowcounter=0
    for index, row in enumerate(fin_tr):
        row_array = []
        if (index > 0):
            for row_fin in row.find_elements_by_tag_name('td'):

                row_array.append(row_fin.text)
            row_data.extend([row_array])
            rowcounter=index
    row_data = np.array(row_data)

    data = pd.DataFrame(data=row_data, columns=title_array)
    data.replace('', np.nan, inplace = True)
    data.dropna(how='all', axis=0, inplace = True)
    data.dropna(how = 'all', axis = 1, inplace = True)
    #data.to_csv("{}_BalanceSheet.csv".format(ticker))

    for columns in range(2,headercounter) :
        for rows in range(1,24) :
            if (data.iloc[rows,columns]==data.iloc[rows,columns]):
                cnx = mysql.connector.connect(user='root', password='techx1234', host='127.0.0.1', database='vinance')
                cursor = cnx.cursor()
                add_bs = ("INSERT INTO bs "
                    "(code, type, period, data)" "VALUES (%s, %s, %s, %s)")

                data_bs = (
                     ticker, data.iloc[rows,0], data.columns[columns], data.iloc[rows,columns])
                print(data_bs)
                cursor.execute(add_bs, data_bs)

                cnx.commit()
                cnx.close()


    driver.close()

async def load_is():
    driver.find_element_by_xpath('//*[@id="analyze"]/div[4]/ul/li[2]').click()
    await asyncio.sleep(5)

def get_is(ticker):
    asyncio.run(load_is())

    fin_tr = driver.find_elements_by_xpath('//*[@id="table_bckqkd"]/tbody//tr')

    heading = fin_tr[0].find_elements_by_tag_name('td')

    title_array = []
    headercounter = 0
    for header in heading:
        title_array.append(header.text)
        headercounter = headercounter + 1

    row_data = []
    rowcounter = 0
    for index, row in enumerate(fin_tr):
        row_array = []
        if (index > 0):
            for row_fin in row.find_elements_by_tag_name('td'):
                row_array.append(row_fin.text)
            row_data.extend([row_array])
            rowcounter = index
    row_data = np.array(row_data)

    data = pd.DataFrame(data=row_data, columns=title_array)
    data.replace('', np.nan, inplace=True)
    data.dropna(how='all', axis=0, inplace=True)
    data.dropna(how='all', axis=1, inplace=True)
    # data.to_csv("{}_BalanceSheet.csv".format(ticker))

    for columns in range(2, headercounter-1):
        for rows in range(0, 22):
            if (data.iloc[rows, columns] == data.iloc[rows, columns]):
                cnx = mysql.connector.connect(user='root', password='techx1234', host='127.0.0.1', database='vinance')
                cursor = cnx.cursor()
                add_is = ("INSERT INTO income"
                          "(code, type, period, data)" "VALUES (%s, %s, %s, %s)")

                data_is = (
                    ticker, data.iloc[rows, 0], data.columns[columns], data.iloc[rows, columns])
                print(data_is)
                cursor.execute(add_is, data_is)

                cnx.commit()
                cnx.close()

    driver.close()

async def load_cf():
    driver.find_element_by_xpath('//*[@id="analyze"]/div[4]/ul/li[4]').click()
    await asyncio.sleep(5)

def get_cf(ticker):
    asyncio.run(load_cf())
    fin_tr = driver.find_elements_by_xpath('//*[@id="table_lctttgiantiep"]/tbody//tr')
    try:
        heading = fin_tr[0].find_elements_by_tag_name('td')
        title_array = []
        headercounter = 0
        for header in heading:
            title_array.append(header.text)
            headercounter = headercounter + 1
        row_data = []
        rowcounter = 0
        for index, row in enumerate(fin_tr):
            row_array = []
            if (index > 0):
                for row_fin in row.find_elements_by_tag_name('td'):
                    row_array.append(row_fin.text)
                row_data.extend([row_array])
                rowcounter = index
        row_data = np.array(row_data)

        data = pd.DataFrame(data=row_data, columns=title_array)
        data.replace('', np.nan, inplace=True)
        data.dropna(how='all', axis=0, inplace=True)
        data.dropna(how='all', axis=1, inplace=True)
        print(headercounter)
        for columns in range(2, headercounter - 1):
            for rows in range(0, 8):
                if (data.iloc[rows, columns] == data.iloc[rows, columns]):
                    cnx = mysql.connector.connect(user='root', password='techx1234', host='127.0.0.1',
                                                  database='vinance')
                    cursor = cnx.cursor()
                    add_cs = ("INSERT INTO cash"
                              "(code, type, period, data)" "VALUES (%s, %s, %s, %s)")

                    data_cs = (
                        ticker, data.iloc[rows, 0], data.columns[columns], data.iloc[rows, columns])
                    print(data_cs)
                    cursor.execute(add_cs, data_cs)

                    cnx.commit()
                    cnx.close()

        driver.close()
    except: pass



with open('D:/Stock_Code.csv','r') as f:
    reader = csv.reader(f)
    code_list = list(reader)

    #print (code_list)
    iterator=0
for stock in code_list  :
    if (iterator>0):
        print("".join(stock))
        driver = webdriver.Chrome('C:\ChromeDriver\chromedriver')
        asyncio.run(load_tvsi("".join(stock)))
        #get_is("".join(stock))
        #get_bs("".join(stock))
        get_cf("".join(stock))
    iterator = iterator + 1

#asyncio.run(load_tvsi('VIC'))
#get_bs('VIC')
#get_is('VIC')
#get_cf('VIC')
