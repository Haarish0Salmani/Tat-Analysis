#!/usr/bin/env python
# coding: utf-8

# # Importing libraries

# In[1]:


import time
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime as dt
import os


# # Importing DataSet

# In[ ]:


start = time.perf_counter()


# In[ ]:


today = input("format:2024-01-01")
input_file_path = input()
output_file_path = input()


# In[ ]:


input_file_path = input_file_path.replace("\\","/")
output_file_path = output_file_path.replace("\\","/")


# In[ ]:


files = os.listdir(input_file_path)


# In[ ]:


Application_cu = os.path.join(input_file_path,files[0])
Branch_master_cu = os.path.join(input_file_path,files[1])
Decen_cu = os.path.join(input_file_path,files[2])
Holiday_list = os.path.join(input_file_path,files[3])
Ftr_mis_cu = os.path.join(input_file_path,files[4])
Tat_cu = os.path.join(input_file_path,files[5])


# In[ ]:


Application = pd.read_csv(Application_cu,encoding='windows-1252')
BM = pd.read_csv(Branch_master_cu,encoding='windows-1252')
Decen = pd.read_csv(Decen_cu,encoding='windows-1252')
Holiday = pd.read_csv(Holiday_list,encoding='windows-1252')
Ftr = pd.read_csv(Ftr_mis_cu,encoding='windows-1252')
Tat = pd.read_csv(Tat_cu,encoding='windows-1252')


# In[ ]:


Tat["Start_Time"] = pd.to_datetime(Tat["Start_Time"],dayfirst=True)
Tat["End_Time"] = pd.to_datetime(Tat["End_Time"],dayfirst=True)
Tat["Desig_date"] = pd.to_datetime(Tat["Desig_date"],dayfirst=True)
Tat["Log_Acc_date"] = pd.to_datetime(Tat["Log_Acc_date"],dayfirst=True)


# In[ ]:


Tat.insert(5,"Desig_date_month",pd.Series())
Tat.insert(6,"Com",pd.Series())
Tat.insert(8,"Tat",int)
Tat.insert(9,"Bucket",str)


# In[ ]:


dates = Tat["Desig_date"]


# In[ ]:


Tat["Log_Acc_date"].fillna("1900-01-01",inplace=True)


# In[ ]:


def dated_na(df):
    for i in range(len(df["Application"])):
        if str(df["Log_Acc_date"].iloc[i]).split()[0] == "1900-01-01":
            df["Log_Acc_date"].iloc[i] = df["Start_Time"].iloc[i]
            
                        
def ext_date_month(series,df):
    proc_date = []
    
    for date_str in series:
        if pd.isna(date_str):
            proc_date.append(None)
        else:
            date_part=str(date_str).split('-')
            if len(date_part)>=2:
                year,month = map(int,date_part[:2])
                first_day = dt(year=year,month=month,day=1).strftime('%d-%m-%Y')
                proc_date.append(first_day)
            else:
                proc_date.append(None)
    df["Desig_date_month"] = proc_date
    return df["Desig_date_month"]
            

    
def compare(series):
    zipped = list(zip(series["Application"],series["Application"].shift(1)))
    l = [0 if value[0] == value[1] else 1 for value in zipped]
    return l

def Branchs(df,odf):
    merged = df.merge(odf[["Branch","Branch_1"]],how='left')
    return merged


def Scheme_mapper(df):
    scheme = [str(i) for i in df["Scheme"]]
    for i in range(len(scheme)):
        if "Formal" in str(scheme[i]):
            df["Occupation"].iloc[i] = "Formal Sal"
        if "Cash Sal" in str(scheme[i]):
            df["Occupation"].iloc[i] = "Cash Sal"
        if "Self Employ" in str(scheme[i]):
            df["Occupation"].iloc[i] = "Self Employ"
        
    return df

def Treat_occ(df):
    Occu = [i for i in df["Occupation"]]
    for i in range(len(Occu)):
        if Occu[i] == "Salaried":
            df["Occupation"].iloc[i] = "Cash Sal"
        if pd.isna(df["Occupation"].iloc[i]):
            df["Occupation"].iloc[i] = "Self Employ"
    return df


def mixed(df):
    mask1 = df["App Ocu"] == "Formal Sal"
    mask2 = df["Co App Ocu"].isin(["Cash Sal","Self Employ","Agr","other"])
    df.loc[mask1&mask2,"Occupation"] = "Sal Mixed"
    
    mask3 = df["App Ocu"] == "Cash Sal"
    mask2 = df["Co App Ocu"].isin(["Cash Sal","Formal Sal","Self Employ","Agr","other"])
    df.loc[mask1&mask2,"Occupation"] = "Sal Mixed"
    
    mask1 = df["App Ocu"] == "Self Employ"
    mask2 = df["Co App Ocu"].isin(["Cash Sal","Formal Sal","Self Employ","Agr","other"])
    df.loc[mask1&mask2,"Occupation"] = "Sal Mixed"
    
    return df

def Salaried_mis(df,odf):
    merged = df.merge(odf[["Application","ocupation"]],how='left')
    return merged

def S_M(df):
    df["Occupation"] = ["Sal Mixed" if df["Occupation"][i] == "Formal Sal" and df["ocupation"][i] == "Sal Mixed" else df["Occupation"][i] for i in range(len(df))]
    return df

def mapper(df,decen):
    df["NonCU"] = ["NonCU" if df["Application"].iloc[i] in decen else "CU" for i in range(len(df["Application"]))]
    return df

def Merger(df,odf,column1,column2):
    merge = df.merge(odf[[column1,column2]],how='left')
    return merge
    
def compare_2(ser):
    zipped = list(zip(ser["Application"],ser["Application"].shift(1)))
    compare =[0 if value[0] == value[1] else 1 for value in zipped]
    return compare
    
    


# In[ ]:


Tat = dated_na(Tat)


# In[ ]:


Tat["Desig_date_month"] = ext_date_month(dates,Tat)


# In[ ]:


Tat["Desig_date_month"] = pd.to_datetime(Tat["Desig_date_month"],dayfirst=True)


# In[ ]:


Tat = Tat.sort_values(["Application","Desig_date","Current_App_stage"],ascending=[True,True,True])


# In[ ]:


Tat["Com"] = compare(Tat)


# In[ ]:


Tat_data1 = Tat[Tat["Com"]==1]


# In[ ]:


Tat_d1 = Tat_data1[Tat_data1["Desig_date"].between("2019-01-01",today,inclusive='both')]


# In[ ]:


Tat_d1 = Tat_d1.drop(Tat_d1[Tat_d1["Desig_date_month"] == "1900-01-01"].index)


# In[ ]:


Tat_d1 = Tat_d1.rename(columns={"Branch Name":"Branch"})
BM = BM[["branch","branch_1"]]
BM = BM.rename(columns={"branch":"Branch","branch_1":"Branch_1"})


# In[ ]:


Tat_d1 = Branchs(Tat_d1,BM)


# In[ ]:


Tat_d1 = Tat_d1.drop([i for i in range(len(Tat_d1)) if str(Tat_d1["Branch_1"][i])= "nan" and str(Tat_d1["Branch"][i])== "nan"])
Tat_d1 = Tat_d1.reset_index()
Tat_d1["Branch_1"] = [Tat_d1["Branch"][i].title() if str(Tat_d1["Branch_1"][i]) == "nan" else Tat_d1["Branch_1"][i] for i in range(len(Tat_d1))]


# In[ ]:


Tat_d1 = mapper(Tat_d1)
Tat_d1 = Treat_occ(Tat_d1)


# In[ ]:


formal_case = Tat_d1[Tat_d1["Occupation"] == "Formal Sal"][["Application","Occupation","Scheme","Desig_date"]]
formal_case = formal_case[formal_case["Desig_date"].between("2023-05-01",today,inclusive='both')]


# In[ ]:


Ftr = Ftr.rename(columns={"Application Id":"Application"})
Ftr.reset_index(inplace=True)
Ftr["App Ocu"] = ["Formal Sal" if i =="Sal" else i for i in Ftr["App Ocu"]]
Ftr["Co App Ocu"] = ["Formal Sal" if i == "Sal" else i for i in ftr["Co App Ocu"]]


# In[ ]:


Mixed_data = Mixed(Ftr)
Tat_d1 = Salaried_mis(Tat_d1,Mixed_data)


# In[ ]:


Tat_d1 = S_M(Tat_d1)


# In[ ]:


Decen.rename(columns={"Application_":"Application"},inplace=True)
Tat_d1["NonCU"] = ""


# In[ ]:


decen = {k:v for k in Decen["Application"] for v in Decen["Flg"]}


# In[ ]:


Tat_d1 = mapper(df,decen)


# In[ ]:


BM1 = BM["branch","region"]
BM1 = BM1.rename(columns={'branch':"Branch1","region","Region"})


# In[ ]:


Tat_d1 = Merger(Tat_d1,BM1,"Branch1","Region")


# In[ ]:


Na = Application[['Application1','L Amount']]
Na.rename(colunns = {'Application1':'Application'},inplace=True)


# In[ ]:


Tat_d1 = Merger(Tat_d1,Na,"Application","L Amount")


# In[ ]:


Tat_d1['com1'] = compare_2(Tat_d1)
Tat_d1 = Tat_d1[Tat_d1['com1']==1]


# In[ ]:


Tat_Data = Tat_d1
Tat_Data.drop(['com1','Branch','index','ocupation'],inplace=True,axis=1)


# In[ ]:


Tat_Data.insert(10,'Branch',str)
Tat_Data['Branch'] = [Tat_Data['Branch1'].iloc[i] for i in range(len(df))]


# In[ ]:


Tat_Data.drop("Branch1",inplace=True,axis=1)


# In[ ]:


Tat_Data.to_excel(os.path.join(output_file_path,'Tat_Data_1.xlsx'))


# In[ ]:


end = time.perf_counter()

elapsed = end-start
print(f"Elapsed:{elapsed:6f} seconds")


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




