#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import json
import numpy as np
import re
import pickle
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        return super(NpEncoder, self).default(obj)


# In[2]:


df = pd.read_csv('proj1_ex01.csv')
df.head()
dictlist = []
for column_name in df:
    dict = {}
    dict.update({"name":column_name})
    dict.update({"missing":(round((df[column_name].isnull().sum()/df.shape[0]), 2))})
    type = df[column_name].dtype
    if("float" in str(type)):
        dict.update({"type":"float"})
    elif("int" in str(type)):
        dict.update({"type":"int"})
    else:
        dict.update({"type":"other"})
    dictlist.append(dict)
with open("proj1_ex01_fields.json", "w") as final:
    json.dump(dictlist, final, cls=NpEncoder)


# In[3]:


upper_dict = {}
for column_name in df:
    if("float" in str(df[column_name].dtype) or "int" in str(df[column_name].dtype)): 
        dict = {}
        dict.update({"count": (df.shape[0]-df[column_name].isnull().sum())})
        dict.update({"mean":df[column_name].mean()})
        dict.update({"std":df[column_name].std()})
        dict.update({"min":df[column_name].min()})
        dict.update({"25%":df[column_name].quantile(0.25)})
        dict.update({"50%":df[column_name].quantile(0.5)})
        dict.update({"75%":df[column_name].quantile(0.75)})
        dict.update({"max":df[column_name].max()})
        upper_dict.update({column_name: dict})
    else:
        dict = {}
        dict.update({"count": (df.shape[0]-df[column_name].isnull().sum())})
        dict.update({"unique":df[column_name].nunique()})
        dict.update({"top":df[column_name].value_counts().idxmax()})
        dict.update({"freq":df[column_name].value_counts().max()})
        upper_dict.update({column_name: dict})
with open("proj1_ex02_stats.json", "w") as final2:
    json.dump(upper_dict, final2, cls=NpEncoder)


# In[4]:


for column_name in df:
    name = str(column_name)
    name = name.lower().replace(" ", "_")
    iter = name
    for i in range(0, len(iter)):
        if not iter[i].isalnum() and iter[i]!="_":
            name = iter.replace(name[i],"")
    df.rename(columns={column_name: name}, inplace=True)
df.to_csv("proj1_ex03_columns.csv", index=False)


# In[5]:


#mozliwe ze trzeba korzystac wlasnie z tego nowego dataframa 
df.to_excel("proj1_ex04_excel.xlsx", index=False)


# In[6]:


df.to_json(r'proj1_ex04_json.json', orient="records")


# In[7]:


df.to_pickle("proj1_ex04_pickle.pkl")  


# In[8]:


df2 = pd.read_pickle('proj1_ex05.pkl')
df2.head()


# In[9]:


columns = df2.iloc[:, [1, 2]]
vs = df2[df2.index.str.startswith('v')]
combined = vs.iloc[:,[1,2]]
combined.head()


# In[10]:


markdown_table = combined.to_markdown().replace('nan', '')
with open("proj1_ex05_table.md", 'w') as file:
    file.write(markdown_table)


# In[11]:


js = json.load(open("proj1_ex06.json"))
js = pd.json_normalize(js)
js.to_pickle("proj1_ex06_pickle.pkl")  

