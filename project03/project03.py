#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import pickle
import json


# In[2]:


df = pd.read_json('proj3_data1.json')
df = pd.concat([df, (pd.read_json('proj3_data2.json')), (pd.read_json('proj3_data3.json'))])
df.index = range(df.shape[0])
df.shape


# In[3]:


df.head(10)


# In[4]:


df.to_json('proj3_ex01_all_data.json', orient='columns')


# In[5]:


num = []
for x in df.columns[df.isna().any()].tolist():
    tab = [x, df[x].isna().sum()]
    num.append(tab)


# In[6]:


print(num)


# In[7]:


df2 = pd.DataFrame(num)
df2.head()


# In[8]:


df2.to_csv('proj3_ex02_no_nulls.csv', index=False, header=False)


# In[9]:


with open('proj3_params.json', 'r') as file:
    param_data = json.load(file)

print(param_data)


# In[10]:


concats = []
for i in range(df.shape[0]):
    con = ""
    for x in param_data['concat_columns']:
        con = con + df[x][i] + " "
    concats.append(con[:-1])


# In[11]:


print(concats)


# In[12]:


df.insert(len(df.columns.tolist()),"description", concats)


# In[13]:


df.head()
df.to_json('proj3_ex03_descriptions.json')


# In[14]:


df_more = pd.read_json('proj3_more_data.json')
df_more.head()


# In[15]:


df = df.join(df_more.set_index(param_data['join_column']), on=param_data['join_column'], how='left')


# In[16]:


df.head()
df.to_json('proj3_ex04_joined.json', orient='columns')


# In[17]:


for i, r in df.iterrows():
    name = r['description']
    name = name.lower()
    name = name.replace(" ", "_")
    print(name)
    filtered_series = r.drop('description')
    result_dict = filtered_series.to_dict()
    conc = "proj3_ex05_" + name + ".json";
    with open(conc, "w") as file: 
        json.dump(result_dict, file)
df.head()


# In[18]:


for i, r in df.iterrows():
    name = r['description']
    name = name.lower()
    name = name.replace(" ", "_")
    print(name)
    filtered_series = r.drop('description').fillna('null')
    result_dict = filtered_series.to_dict()
    conc = "proj3_ex05_" + name + ".json";
    with open(conc, "w") as file: 
        json.dump(result_dict, file)


# In[19]:


for i, r in df.iterrows():
    name = r['description']
    name = name.lower()
    name = name.replace(" ", "_")
    filtered_series = r.drop('description').fillna('null')
    for index, value in filtered_series.items():
        if(index in param_data['int_columns']):
            if(filtered_series[index] == 'null'):
                filtered_series[index] = 0.0
            filtered_series[index] = int(filtered_series[index])
        print(filtered_series[index])
    result_dict = filtered_series.to_dict()
    conc = "proj3_ex05_int_" + name + ".json";
    with open(conc, "w") as file: 
        json.dump(result_dict, file)


# In[20]:


agr = {}
for tup in param_data['aggregations']:
    name = tup[1] + "_" + tup[0]
    agr[name] = df.agg({tup[0]: tup[1]})[tup[0]]
with open("proj3_ex06_aggregations.json", "w") as file: 
        json.dump(agr, file)


# In[21]:


counter = {}
for row in df[param_data['grouping_column']]:
    if row in counter:
        counter[row] += 1
    else:
        counter[row] = 1

print(counter)

grp_column = df[param_data['grouping_column']]
filteredd = df.select_dtypes(include=['number'])
selected_columns = pd.concat([grp_column, filteredd], axis=1)
grouped = selected_columns.groupby(param_data['grouping_column']).mean()

for row in counter:
    if counter[row]<=1:
        grouped = grouped.drop(index=row)

grouped.head(12)
grouped.to_csv('proj3_ex07_groups.csv', header=True, index=True)


# In[22]:


pivot_df = pd.pivot_table(df, 
                           index=param_data['pivot_index'], 
                           columns=param_data['pivot_columns'], 
                           values=param_data['pivot_values'], 
                           aggfunc='max')

pivot_df.head(10)
pivot_df.to_pickle('proj3_ex08_pivot.pkl')


# In[23]:


long_df = pd.melt(df, id_vars=param_data['id_vars'])
long_df.head(12)
long_df.to_csv('proj3_ex08_melt.csv', header=True, index=False)


# In[24]:


dfs = pd.read_csv('proj3_statistics.csv')

pivot2 = pd.wide_to_long(dfs, stubnames=df[param_data['pivot_index']].unique(), i=dfs.columns[0],j='second', sep='_', suffix='\d+')
print(pivot2.index)
pivot2.head(20)
pivot2.to_pickle('proj3_ex08_stats.pkl')


# In[ ]:




