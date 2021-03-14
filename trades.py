#!/usr/bin/env python
# coding: utf-8

# In[6]:


# import library
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd
import numpy as np


# In[3]:


# Connection string
client = MongoClient('mongodb://localhost:27017/')


# In[4]:


# accessing the database  
mydatabase = client['test']
# accessing the collection
mycollection = mydatabase['demo1']
tradescol = mydatabase['trades']


# In[104]:


# Insert demo data
rows = [
        { 'date': '1/1/2021', 'symbol': 'TM', 'close':20, 'sma20':18.7, 'sma50':18.5, 'vwaphigh':17.8},
        { 'date': '1/1/2021', 'symbol': 'DM', 'close':223, 'sma20':240, 'sma50':238, 'vwaphigh':265},
        { 'date': '1/1/2021', 'symbol': 'QS', 'close':1023, 'sma20':1005, 'sma50':1010, 'vwaphigh':1045},
        { 'date': '1/2/2021', 'symbol': 'TM', 'close':22, 'sma20':19.7, 'sma50':18.7, 'vwaphigh':18.8},
        { 'date': '1/2/2021', 'symbol': 'DM', 'close':198, 'sma20':210, 'sma50':230, 'vwaphigh':245},
        { 'date': '1/2/2021', 'symbol': 'QS', 'close':1045, 'sma20':1055, 'sma50':1050, 'vwaphigh':1055},
        { 'date': '1/4/2021', 'symbol': 'TM', 'close':24, 'sma20':25.7, 'sma50':24.5, 'vwaphigh':24.2},
        { 'date': '1/4/2021', 'symbol': 'DM', 'close':240, 'sma20':225, 'sma50':220, 'vwaphigh':235},
        { 'date': '1/4/2021', 'symbol': 'QS', 'close':1080, 'sma20':1045, 'sma50':1050, 'vwaphigh':1065},
        { 'date': '1/6/2021', 'symbol': 'TM', 'close':30.2, 'sma20':26.7, 'sma50':27.5, 'vwaphigh':27.8},
        { 'date': '1/6/2021', 'symbol': 'DM', 'close':290, 'sma20':280, 'sma50':295, 'vwaphigh':285},
        { 'date': '1/6/2021', 'symbol': 'QS', 'close':1130, 'sma20':1103, 'sma50':1060, 'vwaphigh':1095}
       ]
ins = mycollection.insert_many(rows)


# In[11]:


# Loop through all rows in collection, print first 2 rows
i = 0
for row in mycollection.find({}): 
    if (i<2):
        print(row['date'], row['symbol'], row['close'], row['sma20'], row['sma50']) 
    i+=1


# In[13]:


# OR visualize dataframe
# Convert to dataframe for tabular display
data = mycollection.find({})
df = DataFrame(list(data))
df


# In[106]:


# Select distinct symbols
symbols = mycollection.distinct('symbol')
print(symbols)


# In[109]:


# Strategy 1 - SMA 20 croosver 50
# Loop through each symbol, get data in eahc symbol
amount = 40000
for sym in symbols:
    activebuy = False
    quantity = 0
    x = None
    data = mycollection.find({'symbol':sym})
    for row in data:
        # print (row['symbol'], row['sma20'], row['sma50'])
        if ((activebuy is False) and (row['sma20'] > row['sma50'])):
            # Buy the stock
            activebuy = True
            quantity = round(amount/row['close'])
            buyTrade = {'symbol': row['symbol'], 'buydate': row['date'], 'buyprice': row['close'], 
                        'quantity': quantity, 'strategy':'sma20croosover50', 'status':'Open'}
            x = tradescol.insert_one(buyTrade)
            print('buy activated, symbol', row['symbol'], 'date:', row['date'], 'quantity:', quantity)
        if ((activebuy is True) and (row['sma20'] < row['sma50'])):
            # Sell the stock
            sellTrade = {'selldate':row['date'], 'sellprice':row['close'], 'status':'closed'}
            u = tradescol.update_one({"_id": x.inserted_id}, {"$set": sellTrade}, True) 
            print('Sell activated, symbol:', row['symbol'], 'date:', row['date'], 'quantity:', quantity)
            activebuy = False
            quantity = 0


# In[14]:


# Strategy 2 - Price close above VWAP from High


# In[15]:


# Strategy 3 - SMA 10 crossover 20


# In[16]:


# strategy 4 - EMA 5 crossover EMA 8


# In[111]:


# Loop through all trades
for trade in tradescol.find({'status':'closed'}): 
        print(trade['symbol'], trade['buyprice'], trade['buydate'], trade['quantity'], 
              trade['sellprice'], trade['selldate'], 'profit:', (trade['sellprice'] - trade['buyprice'])*trade['quantity']) 


# In[10]:


# OR visualize trades in dataframe
# Convert to dataframe for tabular display
data = tradescol.find({'status':'closed'})
list_data = list(data)
df = DataFrame(list_data)
df['profit'] = (df['sellprice']-df['buyprice'])*df['quantity']
df[['symbol', 'buydate', 'selldate', 'buyprice', 'sellprice', 'profit']]


# In[ ]:


# Cleanup step
# Drop all rows in mycollection
#d = mycollection.delete_many({})
# Drop all rows in trades
# d = tradecol.delete_many({})

