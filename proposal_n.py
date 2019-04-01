# -*- coding: utf-8 -*-
"""
Created on Mon Oct 29 10:06:29 2018

@author: newness
"""

#LINKEDIN PROJECT SAMPLE
#WE WANT TO ANSWER QUESTION: WHICH GEOGRAPHY IS MOST ENGR JOB LISTING\
#HOW BANKS ARE SHIFTING STRATEGIES

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import glob

#A. DETERMINE RATE OF JOB UPDATE PER QUARTER (how often it changes)
#Job frequency

#SPLIT jobs DATA to smaller CSV

#Import into a DB
#Query DB column on 'engineer' per year

path =r'C:\Users\newness\Downloads\temp_datalab_records_job_listings\split\temp' # use your path
allFiles = glob.glob(path + "/*.csv")
joblist = pd.DataFrame()
list_ = []
for file_ in allFiles:
    df = pd.read_csv(file_, engine='python',index_col=None, header=0)
    list_.append(df)
joblist = pd.concat(list_, sort=False)

joblist.dtypes


jobEngr = joblist.loc[joblist.title.str.contains("engineer", na=False)]
jobSales = joblist.loc[joblist.title.str.contains("Sales", na=False)]

jobEngr["posted_date"] = jobEngr["posted_date"].apply(pd.to_datetime,format="%Y-%m/%d",errors='ignore')
jobSales["posted_date"] = jobSales["posted_date"].apply(pd.to_datetime,format="%Y-%m/%d",errors='ignore')

jobEngrCA = jobEngr.loc[jobEngr.region.str.contains("CA", na=False)]
# Visualising the Training set results
jobEngrStC=jobEngr.groupby('region').count()

jobEngrCA.plot(kind='line',x='posted_date',y='region')

plt.xticks( jobEngrStC['title'], jobEngrStC.index.values ) # location, labels
plt.plot( jobEngrStC['title'] )



plt.show()

jobSalesC=jobSales.groupby('region').count()

jobSalesC.plot(kind='line',x='posted_date',y='title')
plt.xticks( jobSalesC['title'], jobSalesC.index.values ) # location, labels
plt.plot( jobSalesC['title'] )
frame1 = plt.gca()
frame1.axes.xaxis.set_ticklabels([])
frame1.axes.yaxis.set_ticklabels([])

plt.show()