# -*- coding: utf-8 -*-
"""
Created on Fri Oct 26 13:18:53 2018

@author: newness
"""
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import glob

path =r'C:\Users\newness\Desktop\EXPORTS' # use your path
allFiles = glob.glob(path + "/*.csv")
expense = pd.DataFrame()
list_ = []
for file_ in allFiles:
    df = pd.read_csv(file_, engine='python',index_col=None, header=0)
    list_.append(df)
expense = pd.concat(list_, sort=False)
expense.dtypes

#Q1
expenseamt= expense['AMOUNT'].sum()
#Q1 Answer = 13660703793.31



#@@@@## Question 2
expense["START DATE"] = expense["START DATE"].apply(pd.to_datetime,format="%m/%d/%Y",errors='ignore')
expense["END DATE"] = expense["END DATE"].apply(pd.to_datetime,format="%m/%d/%Y",errors='ignore')

expense['DATE'] = pd.to_datetime(expense['DATE'],format="%Y", errors='ignore')


expense['DAYS'] = (expense['END DATE'] - expense['START DATE']).dt.days



expensePos = expense.loc[expense['AMOUNT'] > 0]
expenseDays = expensePos.loc[:, 'DAYS':'DAYS']
expensesDayStd = expenseDays.std()
## Q2 Standard Dev = 74.26066728045652


#Q Average annual
expenseSt2016t = expense.loc[expense['START DATE'] >= '01-01-2016']
expense2016end = expenseSt2016t.loc[expenseSt2016t['START DATE'] <= '12-31-2016']
ann2016TOT = expense2016end['AMOUNT'].sum()

expenseSt2010t = expense.loc[expense['START DATE'] >= '01-01-2010']
expense2010end = expenseSt2010t.loc[expenseSt2010t['START DATE'] <= '12-31-2010']
ann2010TOT = expense2010end['AMOUNT'].sum()

expenseSt2011t = expense.loc[expense['START DATE'] >= '01-01-2011']
expense2011end = expenseSt2011t.loc[expenseSt2011t['START DATE'] <= '12-31-2011']
ann2011TOT = expense2011end['AMOUNT'].sum()

expenseSt2012t = expense.loc[expense['START DATE'] >= '01-01-2012']
expense2012end = expenseSt2012t.loc[expenseSt2012t['START DATE'] <= '12-31-2012']
ann2012TOT = expense2012end['AMOUNT'].sum()

expenseSt2013t = expense.loc[expense['START DATE'] >= '01-01-2013']
expense2013end = expenseSt2013t.loc[expenseSt2013t['START DATE'] <= '12-31-2013']
ann2013TOT = expense2013end['AMOUNT'].sum()

expenseSt2014t = expense.loc[expense['START DATE'] >= '01-01-2014']
expense2014end = expenseSt2014t.loc[expenseSt2014t['START DATE'] <= '12-31-2014']
ann2014TOT = expense2014end['AMOUNT'].sum()

expenseSt2015t = expense.loc[expense['START DATE'] >= '01-01-2015']
expense2015end = expenseSt2015t.loc[expenseSt2015t['START DATE'] <= '12-31-2015']
ann2015TOT = expense2015end['AMOUNT'].sum()


annlist =[ann2010TOT, ann2011TOT, ann2012TOT, ann2013TOT, ann2014TOT, ann2015TOT, ann2016TOT]
annTot = pd.DataFrame({'col':annlist})

annmean = annTot.mean()
#Q3 answer 1192788865.58


##Q4
#shortmethd
bigOfficetry2 = expenseSt2016t.groupby(["OFFICE", "PURPOSE"])["AMOUNT"].sum()
#answer = Government Contibutions FOR fers PURPOSE

#lONG METHOD FOR SAFETY
bigOfficetry = expenseSt2016t.groupby(["OFFICE"])["AMOUNT"].sum()
#Q4 ANS1 Goverment Ofice =Government Contibutions
#The i look for govt PURPOSE
bigOfficebud = expenseSt2016t.loc[expenseSt2016t['OFFICE'] == "GOVERNMENT CONTRIBUTIONS"]
sortOffice = bigOfficebud.sort_values('AMOUNT')




#Q5 2016 only so use expense2016end
perscomp = expense2016end.loc[expense2016end['CATEGORY'] == "PERSONNEL COMPENSATION"]



##SUm by id
#REMOVE COMMA AND DOT

perscomp['PAYEE'] = perscomp['PAYEE'].str.replace(',', ' ')
perscomp['PAYEE'] = perscomp['PAYEE'].str.replace('.', ' ')

#sUM WAGES
peerunique = perscomp.groupby(["PAYEE"])["AMOUNT"].sum()

#Mean by office
officemean = perscomp.groupby('OFFICE')[peerunique['INDEX']].agg([pd.np.mean])

import pandas as pd

df = pd.read_sql(sql, cnxn)

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="secret",
  database="expense"
)

import pyodbc
import pandas.io.sql as psql

cnxn = pyodbc.connect(connection_info) 
cursor = cnxn.cursor()
sql = "SELECT * FROM TABLE"


mycursor = mydb.cursor()

sql = "select DAYS from report where AMOUNT > 0"

mycursor.execute(sql)

myresult = mycursor.fetchall()

for x in myresult:
  print(x)












#frame['START DATE'] = pd.to_datetime(frame['START DATE'], errors='coerce')

#frame["START DATE"] = pandas.to_datetime(frame["START DATE"], dayfirst=False, yearfirst=False,format"%m/%d/%Y",errors='ignore')


'''list2_=[]
posdays =  pd.DataFrame()
for r in frame.iterrows():
    if not frame.iloc[r]["AMOUNT"] == "":
        if frame.iloc[r]["AMOUNT"] >= 0:
            list2_.append(frame.iloc[r]["DAYS"])      
    posdays = pd.concat(list2_, sort=False)'''
    

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
 
    ############
    ###Q3 Need spark to Query fast
from pyspark import SparkConf

import pyspark
import pyspark.sql
from pyspark.sql import SparkSession


spark = SparkSession\
    .builder\
    .appName("INCUB")\
    .getOrCreate()


#frame.dtypes
from pyspark.sql.types import *
mySchema = StructType([ StructField("BIOGUIDE_ID", StringType(), True)\
                       ,StructField("OFFICE", StringType(), True)\
                       ,StructField("QUARTER", StringType(), True)\
                       ,StructField("CATEGORY", StringType(), True)\
                       ,StructField("DATE", TimestampType(), True)\
                       ,StructField("PAYEE", StringType(), True)\
                       ,StructField("START DATE", TimestampType(), True)\
                       ,StructField("END DATE", TimestampType(), True)\
                       ,StructField("PURPOSE", StringType(), True)\
                       ,StructField("AMOUNT", FloatType(), True)\
                       ,StructField("YEAR", FloatType(), True)\
                       ,StructField("TRANSCODE", StringType(), True)\
                       ,StructField("TRANSCODELONG", StringType(), True)\
                       ,StructField("RECORDID", StringType(), True)\
                       ,StructField("RECIP (ORIG.)", StringType(), True)\
                       ,StructField("PROGRAM", StringType(), True)\
                       ,StructField("SORT SEQUENCE", StringType(), True)\
                       ,StructField("DAYS", FloatType(), True)])

    
spframe = spark.createDataFrame(frame,schema=mySchema)   
 
### ALTERNATE
'''
csxv = spark.read.load(r"C:\Users\newness\Desktop\house-office-expenditures-with-readme\house-office-expenditures-with-readme\*disburse-detail.csv",
                     format="csv", sep=",", inferSchema="true", header="true")

csxvNS = spark.read.load(r"C:\Users\newness\Desktop\house-office-expenditures-with-readme\house-office-expenditures-with-readme\*disburse-detail.csv",
                     format="csv", sep=",", inferSchema="false", header="true")

print(csxv.printSchema())

print(csxvNS.printSchema())'''


###q2
#from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)

spframe.registerTempTable("report")

q2temp_tb =  spark.sql("select DAYS from report where AMOUNT > 0")

print(q2temp_tb)
q2temp_tb.write.format("csv").save('q2temper')
#q2temp_df = q2temp_tb.toPandas()
#Find standard dev

#q3
query3a = """select OFFICE, PURPOSE, AMOUNT from report where [START DATE] >= '2010-01-01'and [START DATE] <= 2010-12-31 where AMOUNTS > 0"""
query3b = """select OFFICE, PURPOSE, AMOUNT from report where [START DATE] >= '2011-01-01'and [START DATE] <= 2011-12-31 where AMOUNTS > 0"""
query3c = """select OFFICE, PURPOSE, AMOUNT from report where [START DATE] >= '2012-01-01'and [START DATE] <= 2012-12-31 where AMOUNTS > 0"""
query3d = """select OFFICE, PURPOSE, AMOUNT from report where [START DATE] >= '2013-01-01'and [START DATE] <= 2013-12-31 where AMOUNTS > 0"""
query3e = """select OFFICE, PURPOSE, AMOUNT from report where [START DATE] >= '2014-01-01'and [START DATE] <= 2014-12-31 where AMOUNTS > 0"""
query3f = """select OFFICE, PURPOSE, AMOUNT from report where [START DATE] >= '2015-01-01'and [START DATE] <= 2015-12-31 where AMOUNTS > 0"""
query3g = """select OFFICE, PURPOSE, AMOUNT from report where [START DATE] >= '2016-01-01'and [START DATE] <= 2016-12-31 where AMOUNTS > 0"""

select OFFICE, PURPOSE, AMOUNT from report where [START DATE] 016-01-01 to 2016-12-31 sortby amount office


#Q4
query4= """select OFFICE, PURPOSE, AMOUNT from report where [START DATE] >= '2016-01-01'and [START DATE] <= 2016-12-31 where AMOUNTS > 0 group by OFFICE 
order by AMOUNT(*) desc"""

identify


from pyspark.sql.functions import udf

commaRep = udf(lambda x: x.replace(',',''))
NaNRep = udf(lambda x: x.replace('NaN',''))
valRep = udf(lambda x: x.replace('#value',''))

csxvCM=csxv.withColumn('AMOUNT',commaRep('AMOUNT'))
frameCM=frame.withColumn('AMOUNT',commaRep('AMOUNT'))



###CHANGE SCHEMA
csxv5 = SQLContext.createDataFrame(csxv.rdd.map(tuple), mySchema)





frame['END DATE'] = pd.to_datetime(frame['END DATE'], format='%m-%d-%Y', errors='coerce')

#framed['START DATE'] = frame['START DATE'].astype('datetime64[ns]')

###############################################################
#Q2 TIME
#################################################################





csxvval=csxvCM.withColumn('AMOUNT',NaNRep('AMOUNT'))
csxvval2=csxvval.withColumn('AMOUNT',valRep('AMOUNT'))
csxv2


csxv2.show()
from pyspark.sql.types import DoubleType

csxv2 = csxvval2.withColumn("AMOUNT", csxvCM["AMOUNT"].cast(DoubleType()))


print(csxv2.printSchema()) 

csxv.toPandas()

csxv2.registerTempTable("csxv2table")


csxvdate = csxv2.withColumn("DATE", ("DATE", "format"))

from pyspark.sql.functions import *
csxvdate = csxv2.withColumn("DATE", to_date(csxv2.DATE).alias('date')).collect()
csxvdate = csxv2.withColumn("DATE")

csxvdate = csxv2.select(to_date(csxv2.DATE, 'MM-dd-yyyy').alias('date')).collect()

csxv2.show()
           csxv2.withColumn("START DATE", to_date(unix_timestamp($"date", "M/dd/yyyy").cast("timestamp")))
csxv2.withColumn("END DATE", to_date(unix_timestamp($"date", "M/dd/yyyy").cast("timestamp")))

from pyspark.sql import Row
from pyspark.sql.functions import sum

csxv2.groupBy().sum('amount').collect()
csxv2.filter(csxv2.AMOUNT.isNotNull()).show

from functools import reduce
csxv2.filter(~reduce(lambda x, y: x & y, [csxv2[c].isNull() for c in csxv2.columns])).show()

frame['AMOUNT'] = frame['AMOUNT'].astype(float)
frame[frame['AMOUNT'].str.contains("#VALUE", na=False)]
csxv3.show()

expensetotal = csxv2['AMOUNT'].sum()
