#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from pymongo import MongoClient
from pandas.io.json import json_normalize as jnz
from datetime import datetime


# In[2]:


conn = MongoClient("mongodb://ewizai:qwersdcvdfdssa@13.56.95.43:27017/ewizai")
db = conn["ewizai"]
Seasonal_colection = db["Seasonality"]
Recommendations=db["Recommendations"]


# In[3]:


seasonal_df=pd.DataFrame(list(Seasonal_colection.find()))
Recommendations_df=pd.DataFrame(list(Recommendations.find()))


# In[4]:


seasonal_df_sort=seasonal_df[['_id', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']]


# In[5]:


Seasonal_products = []
Seasonal_products.append(seasonal_df_sort[seasonal_df_sort.columns[datetime.now().month]].dropna())
Seasonal_products.append(seasonal_df_sort[seasonal_df_sort.columns[datetime.now().month+1]].dropna())
Seasonal_products.append(seasonal_df_sort[seasonal_df_sort.columns[datetime.now().month+2]].dropna())


# In[6]:


flat_list = []
for sublist in Seasonal_products:
    for item in sublist:
        flat_list.append(item)


# In[7]:


def uniq(input):
  output = []
  for x in input:
    if x not in output:
      output.append(x)
  return output

Seasonal_products_flat_unique=uniq(flat_list)


# In[8]:


Recommendations_df_sub=Recommendations_df[['RecommendationGuid','UserGuid', 'TargetProducts']]


# In[9]:


seasonal_flat_list = []
for sublist in Seasonal_products_flat_unique:
    for item in sublist:
        seasonal_flat_list.append(item)


# In[10]:


#Getting seasonal products which belong to user criteria
def common(row):
  return set(row['TargetProducts'].split(",")) & set(seasonal_flat_list)
Recommendations_df_sub['TargetProduct_Seasonal']=Recommendations_df_sub.apply(common,axis=1)


# In[11]:


#Removing seasonal products from target_product
def recom(row):
  return set(row['TargetProducts'].split(",")) - row['TargetProduct_Seasonal']
Recommendations_df_sub['TargetProducts']=Recommendations_df_sub.apply(recom,axis=1)


# In[12]:


Recommendations_df_sub


# In[13]:


def test(row):
  return list(row['TargetProduct_Seasonal'])
Recommendations_df_sub['TargetProduct_Seasonal']=Recommendations_df_sub.apply(test,axis=1)


# In[14]:


Recommendations_df_sub


# In[15]:


Recommendations_df_sub.TargetProduct_Seasonal = Recommendations_df_sub.TargetProduct_Seasonal.apply(lambda y: np.nan if len(y)==0 else y)


# In[16]:


Recommendations_df_sub


# In[ ]:





# In[54]:


len(Recommendations_df_sub['TargetProduct_Seasonal'])


# In[55]:


Recommendations_df_sub.isnull().sum(axis=0)


# In[56]:


71574 - 63002

