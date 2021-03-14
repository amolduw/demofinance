#!/usr/bin/env python
# coding: utf-8

# In[25]:


# import library
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd
import numpy as np


# In[142]:


# Connection string
client = MongoClient('mongodb://localhost:27017/')
# accessing the database  
mydatabase = client['test']
# accessing the daily collection
dailycol = mydatabase['daily']
# accessing the analysis collection
analysiscol = mydatabase['analysis']


# In[49]:


# Insert demo data
rows = [
        { 'date': '1/1/2021', 'symbol': 'TM', 'high':21.4, 'low':18.2, 'close':20, 'volume':222000},
        { 'date': '1/1/2021', 'symbol': 'DM', 'high':121.4, 'low':116.2, 'close':120, 'volume':122000},
        { 'date': '1/2/2021', 'symbol': 'TM', 'high':24.4, 'low':19.2, 'close':23.4, 'volume':333000},
        { 'date': '1/2/2021', 'symbol': 'DM', 'high':124.4, 'low':114.2, 'close':123.4, 'volume':122000},
        { 'date': '1/4/2021', 'symbol': 'TM', 'high':23.4, 'low':16.2, 'close':22.8, 'volume':122000},
        { 'date': '1/4/2021', 'symbol': 'DM', 'high':127.8, 'low':114.2, 'close':126.2, 'volume':122000},
        { 'date': '1/6/2021', 'symbol': 'TM', 'high':26.4, 'low':22.2, 'close':26.1, 'volume':242000},
        { 'date': '1/6/2021', 'symbol': 'DM', 'high':129.4, 'low':116.2, 'close':124.9, 'volume':322000},
        { 'date': '1/7/2021', 'symbol': 'TM', 'high':32.4, 'low':24.2, 'close':29.2, 'volume':422000},
        { 'date': '1/7/2021', 'symbol': 'DM', 'high':131.4, 'low':126.2, 'close':130.8, 'volume':129000},
        { 'date': '1/8/2021', 'symbol': 'TM', 'high':25.4, 'low':22.2, 'close':24.2, 'volume':192000},
        { 'date': '1/8/2021', 'symbol': 'DM', 'high':131.8, 'low':122.2, 'close':124, 'volume':222000},
        { 'date': '1/9/2021', 'symbol': 'TM', 'high':25.9, 'low':19.2, 'close':22.6, 'volume':282000},
        { 'date': '1/9/2021', 'symbol': 'DM', 'high':133.6, 'low':119.2, 'close':1227.4, 'volume':244000},
        { 'date': '1/10/2021', 'symbol': 'TM', 'high':29.4, 'low':23.2, 'close':27.6, 'volume':322000},
        { 'date': '1/10/2021', 'symbol': 'DM', 'high':141.4, 'low':123.8, 'close':140.2, 'volume':122000},
        { 'date': '1/12/2021', 'symbol': 'TM', 'high':31.6, 'low':27.2, 'close':29.8, 'volume':380834},
        { 'date': '1/12/2021', 'symbol': 'DM', 'high':139.6, 'low':132.5, 'close':136.7, 'volume':420000},
        { 'date': '1/13/2021', 'symbol': 'TM', 'high':21.4, 'low':18.2, 'close':20, 'volume':222000},
        { 'date': '1/13/2021', 'symbol': 'DM', 'high':121.4, 'low':116.2, 'close':120, 'volume':122000},
        { 'date': '1/14/2021', 'symbol': 'TM', 'high':24.4, 'low':19.2, 'close':23.4, 'volume':333000},
        { 'date': '1/14/2021', 'symbol': 'DM', 'high':124.4, 'low':114.2, 'close':123.4, 'volume':122000},
        { 'date': '1/16/2021', 'symbol': 'TM', 'high':23.4, 'low':16.2, 'close':22.8, 'volume':122000},
        { 'date': '1/16/2021', 'symbol': 'DM', 'high':127.8, 'low':114.2, 'close':126.2, 'volume':122000},
        { 'date': '1/17/2021', 'symbol': 'TM', 'high':26.4, 'low':22.2, 'close':26.1, 'volume':242000},
        { 'date': '1/17/2021', 'symbol': 'DM', 'high':129.4, 'low':116.2, 'close':124.9, 'volume':322000},
        { 'date': '1/18/2021', 'symbol': 'TM', 'high':32.4, 'low':24.2, 'close':29.2, 'volume':422000},
        { 'date': '1/18/2021', 'symbol': 'DM', 'high':131.4, 'low':126.2, 'close':130.8, 'volume':129000},
        { 'date': '1/19/2021', 'symbol': 'TM', 'high':25.4, 'low':22.2, 'close':24.2, 'volume':192000},
        { 'date': '1/19/2021', 'symbol': 'DM', 'high':131.8, 'low':122.2, 'close':124, 'volume':222000},
        { 'date': '1/22/2021', 'symbol': 'TM', 'high':25.9, 'low':19.2, 'close':22.6, 'volume':282000},
        { 'date': '1/22/2021', 'symbol': 'DM', 'high':133.6, 'low':119.2, 'close':1227.4, 'volume':244000},
        { 'date': '1/23/2021', 'symbol': 'TM', 'high':29.4, 'low':23.2, 'close':27.6, 'volume':322000},
        { 'date': '1/23/2021', 'symbol': 'DM', 'high':141.4, 'low':123.8, 'close':140.2, 'volume':122000},
        { 'date': '1/24/2021', 'symbol': 'TM', 'high':31.6, 'low':27.2, 'close':29.8, 'volume':380834},
        { 'date': '1/24/2021', 'symbol': 'DM', 'high':139.6, 'low':132.5, 'close':136.7, 'volume':420000},  
        { 'date': '1/27/2021', 'symbol': 'TM', 'high':21.4, 'low':18.2, 'close':20, 'volume':222000},
        { 'date': '1/27/2021', 'symbol': 'DM', 'high':121.4, 'low':116.2, 'close':120, 'volume':122000},
        { 'date': '1/28/2021', 'symbol': 'TM', 'high':24.4, 'low':19.2, 'close':23.4, 'volume':333000},
        { 'date': '1/28/2021', 'symbol': 'DM', 'high':124.4, 'low':114.2, 'close':123.4, 'volume':122000},
        { 'date': '1/29/2021', 'symbol': 'TM', 'high':23.4, 'low':16.2, 'close':22.8, 'volume':122000},
        { 'date': '1/29/2021', 'symbol': 'DM', 'high':127.8, 'low':114.2, 'close':126.2, 'volume':122000},
        { 'date': '2/1/2021', 'symbol': 'TM', 'high':26.4, 'low':22.2, 'close':26.1, 'volume':242000},
        { 'date': '2/1/2021', 'symbol': 'DM', 'high':129.4, 'low':116.2, 'close':124.9, 'volume':322000},
        { 'date': '2/2/2021', 'symbol': 'TM', 'high':32.4, 'low':24.2, 'close':29.2, 'volume':422000},
        { 'date': '2/2/2021', 'symbol': 'DM', 'high':131.4, 'low':126.2, 'close':130.8, 'volume':129000},
        { 'date': '2/3/2021', 'symbol': 'TM', 'high':25.4, 'low':22.2, 'close':24.2, 'volume':192000},
        { 'date': '2/3/2021', 'symbol': 'DM', 'high':131.8, 'low':122.2, 'close':124, 'volume':222000},
        { 'date': '2/4/2021', 'symbol': 'TM', 'high':25.9, 'low':19.2, 'close':22.6, 'volume':282000},
        { 'date': '2/4/2021', 'symbol': 'DM', 'high':133.6, 'low':119.2, 'close':1227.4, 'volume':244000},
        { 'date': '2/5/2021', 'symbol': 'TM', 'high':29.4, 'low':23.2, 'close':27.6, 'volume':322000},
        { 'date': '2/5/2021', 'symbol': 'DM', 'high':141.4, 'low':123.8, 'close':140.2, 'volume':122000},
        { 'date': '2/6/2021', 'symbol': 'TM', 'high':31.6, 'low':27.2, 'close':29.8, 'volume':380834},
        { 'date': '2/6/2021', 'symbol': 'DM', 'high':139.6, 'low':132.5, 'close':136.7, 'volume':420000},
       ]
ins = dailycol.insert_many(rows)


# In[46]:


# Drop all rows in collection
#d = dailycol.delete_many({})


# In[36]:


# Loop through all rows in collection, print first 2 rows
i = 0
for row in dailycol.find({}): 
    if (i<2):
        print(row) 
    i+=1


# In[50]:


# Select distinct symbols
symbols = dailycol.distinct('symbol')
print(symbols)


# In[139]:


# VWAP definition
def fn_vwap(df_in):
    df = df_in.copy()
    df['vwap_pandas'] = (df.volume*(df.high+df.low+df.close)/3).cumsum() / df.volume.cumsum()
    vw = df['vwap_pandas'].iloc[-1]
    return vw


# In[143]:


# Loop through each symbol, get data in each symbol
for sym in symbols:
    data = dailycol.find({'symbol':sym})
    #Convert curor object to list
    list_data = list(data)
    #Convert list to dataframe
    df = DataFrame(list_data)
    i = 0
    c = 0
    lookcback_period = 5
    for index, row in df.iterrows():
        subsetdata = None
        
        # Slice subset of data for VWAP lookback period
        if (i<lookcback_period):
            subsetdata = df[i-c:i+1]
            c+=1
        else:
            subsetdata = df[i-lookcback_period+1:i+1]
        
        # Reset index on subsetdata
        subsetdata = subsetdata.reset_index(drop=True)
        # For troubleshooting print below
        #print('subset data')
        #print(subsetdata)
        
        # Find Highest high index and lowest low index in subsetdata
        high_index = subsetdata['high'].idxmax(axis = 0)
        low_index = subsetdata['low'].idxmin(axis = 0)
        total_rows = len(subsetdata)
        print('lookback Subsetdata:', 'highest high day index-', high_index, 'lowest low day index-', low_index)
        
        # Slice subsetdata_from_highest_high index day 
        if (high_index == total_rows-1):
            subsetdata_high = subsetdata[high_index:total_rows]
        else:
            subsetdata_high = subsetdata[high_index:total_rows]
        
        #print('subset data from high')
        #print(subsetdata_high)
        
        # Slice subsetdata_from_lowest_low index day 
        if (low_index == total_rows-1):
            subsetdata_low = subsetdata[low_index:total_rows]
        else:
            subsetdata_low = subsetdata[low_index:total_rows]
    
        #print('subset data from low')
        #print(subsetdata_low)
        
        # Calculate current row VWAPs for three datasets - from lookback, from high, from low     
        vw_lookback = fn_vwap(subsetdata)
        vw_from_high = fn_vwap(subsetdata_high)
        vw_from_low = fn_vwap(subsetdata_low)
        print('date:', row['date'], 'lookback vwap:', vw_lookback, 
              'vwap from high:', vw_from_high, 'vwap from low:', vw_from_low)
        
        # Insert vwap record in analysis table. Upsert if already exits (not coded here). 
        # Coordinate inset/upsert with other calculations in analysis table
        newRow = {'symbol': row['symbol'], 'date': row['date'], 'close': row['close'], 'volume': row['volume'], 
                    'vwap_lookback':vw_lookback, 'vwap_high':vw_from_high, 'vwap_low':vw_from_low}
        x = analysiscol.insert_one(newRow)
        
        # increment counter variable
        i+=1      


# In[146]:


# visualize analysis table in dataframe
data = analysiscol.find({})
df = DataFrame(list(data))
df[0:10]


# In[ ]:




