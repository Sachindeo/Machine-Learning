#!/usr/bin/env python
# coding: utf-8

# In[1]:


# !pip install pymongo


# In[1]:


import pandas as pd
import numpy as np
from pymongo import MongoClient
from pandas.io.json import json_normalize as jnz


# In[2]:


#CREATING CONNECTION WITH MONGO DB
conn = MongoClient("mongodb://ewizai:qwersdcvdfdssa@13.56.95.43:27017/ewizai")
db = conn["ewizai"]
order_collection = db["Orders"]


# In[3]:


df = jnz(list(order_collection.find()),'ProductList',['OrderDate','UserGuid','WebsiteGuid'])


# In[4]:


df1 = df[['UserGuid',"ProductGuid",
                'OrderDate','WebsiteGuid', "Quantity"]]


# In[5]:


df_PP = df1[df1.WebsiteGuid=='B004168A-6400-4582-824A-CF1ACAC0FAC0']


# In[6]:


# Preprocessing Positive Promotion data
order_data_pos = df[df.WebsiteGuid=='B004168A-6400-4582-824A-CF1ACAC0FAC0']
# order_data_pos_sub = order_data_pos[['OrderDate','EmailAddress', 'UserGuid', 'ProductGuid','Quantity']]
order_data_pos_sub = order_data_pos[['OrderDate', 'UserGuid', 'ProductGuid','Quantity']]
order_data_pos_sub.rename(columns = {'Quantity':'productQuantity', 'ProductGuid': 'ItemSku', 'UserGuid': 'GUID'}, inplace=True)
print(order_data_pos_sub.OrderDate.dtype)
order_data_pos_sub['OrderDate'] = pd.to_datetime(order_data_pos_sub['OrderDate'])
print(order_data_pos_sub.OrderDate.dtype)
order_data_pos_sub['TransactionMonth'] = order_data_pos_sub['OrderDate'].dt.strftime('%m')
order_data_pos_sub['TransactionYear'] = order_data_pos_sub['OrderDate'].dt.strftime('%Y')
order_data_pos_pre = order_data_pos_sub.drop('OrderDate', axis = 1)


# In[7]:


Product_monthly_purchage=order_data_pos_pre.groupby(['TransactionMonth', 'ItemSku']).sum()
Product_monthly_purchage.reset_index(inplace=True)
User_monthly_purchage_12=Product_monthly_purchage[['ItemSku', 'productQuantity']]
Top_products = User_monthly_purchage_12.groupby(['ItemSku', 'productQuantity']).sum()
Top_products_sorted = Top_products.sort_values(by='productQuantity', ascending=False)
Top_products_sorted1=Top_products_sorted.reset_index()
Top_hundred_products=Top_products_sorted1.iloc[ : 5000, 0:1 ]


# In[8]:


Product_monthly_purchage_2=order_data_pos_pre.groupby(['TransactionYear', 'TransactionMonth', 'ItemSku']).sum()
Product_monthly_purchage_2.reset_index(inplace=True)
Year_2017=Product_monthly_purchage_2[Product_monthly_purchage_2['TransactionYear']=='2017']
Year_2018=Product_monthly_purchage_2[Product_monthly_purchage_2['TransactionYear']=='2018']


# In[9]:


df2=Year_2017[[ 'ItemSku', 'TransactionMonth','productQuantity']]
df3=df2.loc[df2['ItemSku'].isin(Top_hundred_products['ItemSku'])]
  
pivot_data=pd.DataFrame(pd.pivot_table(df3, index= 'ItemSku', columns='TransactionMonth', values="productQuantity")).replace(0, None)
pivot_data['median'] = pivot_data[pivot_data.columns].mean(axis=1, skipna=True)
pivot_data=pivot_data.fillna(0)
  
test_dataframe=pivot_data.reset_index()
data_frame1=test_dataframe[['ItemSku', 'median']]
data_frame2=df3
merge_dataframe=pd.merge(data_frame1, data_frame2, on='ItemSku')


# In[10]:


merge_dataframe['Percentage_change']=((merge_dataframe['productQuantity']-merge_dataframe['median'])*100)/merge_dataframe['median']
merge_dataframe['Percentage_change'] = round(merge_dataframe['Percentage_change'], 1)
  
thresshold_dataframe=merge_dataframe[merge_dataframe['Percentage_change']>=100.0]
thresshold_dataframe_sub=thresshold_dataframe[['ItemSku', 'TransactionMonth']]
thresshold_sub_group=thresshold_dataframe_sub.groupby(['ItemSku', 'TransactionMonth'])
  
Final_2017=thresshold_dataframe_sub.groupby('ItemSku')['TransactionMonth'].apply(lambda x: "{%s}" % ', '.join(x)).reset_index(name ='Months_2017')


# In[11]:


df3=Year_2018[[ 'ItemSku', 'TransactionMonth','productQuantity']]
df4=df3.loc[df3['ItemSku'].isin(Top_hundred_products['ItemSku'])]
pivot_data1=pd.DataFrame(pd.pivot_table(df4, index= 'ItemSku', columns='TransactionMonth', values="productQuantity")).replace(0, None)
pivot_data1['median'] = pivot_data1[pivot_data1.columns].median(axis=1, skipna=True)
pivot_data1=pivot_data1.fillna(0)
test_dataframe1=pivot_data1.reset_index()
data_frame3=test_dataframe1[['ItemSku', 'median']]
data_frame4=df4
merge_dataframe1=pd.merge(data_frame3, data_frame4, on='ItemSku')
merge_dataframe1['Percentage_change']=((merge_dataframe1['productQuantity']-merge_dataframe1['median'])*100)/merge_dataframe1['median']
merge_dataframe1['Percentage_change'] = round(merge_dataframe1['Percentage_change'], 1)
thresshold_dataframe1=merge_dataframe1[merge_dataframe1['Percentage_change']>=100.0]
thresshold_dataframe_sub1=thresshold_dataframe1[['ItemSku', 'TransactionMonth']]
Final_2018=thresshold_dataframe_sub1.groupby('ItemSku')['TransactionMonth'].apply(lambda x: "{%s}" % ', '.join(x)).reset_index(name ='Months_2018')


# In[12]:


Seasonal_products= pd.merge(Final_2017, Final_2018, on= 'ItemSku', how = 'left')
Seasonal_products_final=Seasonal_products.dropna()
Seasonal_products_final['Months_2017'] = Seasonal_products_final['Months_2017'].map(lambda x: x.lstrip('{').rstrip('}'))
Seasonal_products_final['Months_2018'] = Seasonal_products_final['Months_2018'].map(lambda x: x.lstrip('{').rstrip('}'))


# In[13]:


def Months(row):
    return set(row['Months_2017'].split(", ")) & set(row['Months_2018'].split(", "))
Seasonal_products_final['Months']=Seasonal_products_final.apply(Months,axis=1)


# In[14]:


def get_element(row):
    return ','.join(s for s in row['Months']) 
Seasonal_products_final['Months']=Seasonal_products_final.apply(get_element,axis=1)


# In[15]:


Seasonal_products_final['Months'].replace('', np.nan, inplace=True)
Seasonal_products_final_2=Seasonal_products_final[['ItemSku', 'Months']]
Seasonal_products_final_2=Seasonal_products_final_2.dropna()


# In[16]:


arg1=['01', '02','03','04','05','06','07','08','09','10','11','12']
arg2=['jan', 'feb', 'mar', 'apr', 'may', 'june', 'july', 'aug', 'sep', 'octub', 'nov', 'dec']

def extract(string):
    string=str(string)
    x =Seasonal_products_final_2[Seasonal_products_final_2['Months'].str.contains(string)][['ItemSku']]
    out=x.values.tolist()
    return out

for i in range(len(arg1)):
    arg2[i]=extract(arg1[i])


# In[17]:


data_frame=pd.DataFrame.from_dict({'January': arg2[0], 'February': arg2[1], 'March': arg2[2], 'April': arg2[3], 'May': arg2[4], 'June': arg2[5] , 'July': arg2[6], 'August': arg2[7], 'September': arg2[8], 'October': arg2[9], 'November': arg2[10], 'December': arg2[11]}, orient='index').T


# In[18]:


data_frame


# In[19]:


mycol = db["Seasonality"]
x = mycol.delete_many({})
print(x.deleted_count, " documents deleted.")


# In[20]:


import json
records = json.loads(data_frame.T.to_json()).values()
db.Seasonality.insert(records)


# In[ ]:




