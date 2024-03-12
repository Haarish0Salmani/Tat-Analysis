#!/usr/bin/env python
# coding: utf-8

# In[3]:


class Tat_Bucket:
    def __init__(self,df):
        self.df = df
        
    def Ctat(self,start,end,holiday_list):
        tat_values = []
        Tat_dates = []
        between = []
        
        full = []
        self.df[start] = pd.to_datetime(self.df[start],errors='coerce')
        self.df[end] = pd.to_datetime(self.df[end],errors='coerce')
        
        for i in range(len(df)):
            
            if str(self.df[start].iloc[i]) == "NaT" or str(self.df[end].iloc[i]) == "NaT":
                tat_values.append(None)
                continue
            
            start_date = self.df[start].iloc[i].date()
            end_date = self.df[end].iloc[i].date()
            
            days = (end_date-start_date).days
            Tat_dates.append(days)
            
            
            holiday_bn = holiday_list[(holiday_list['date']).between(str(start_date),str(end_date))]
            
            num_holiday = len(holiday_bn['date'])
            between.append(num_holiday)
            
            tat_value = days-num_holiday
            ful.append((start_date,end_date,num_holiday,tat_value))
            tat_values.append(int(tat_value))
            
        return tat_values
     
    def Bucketing(self, column):
        def bucket_value(value):
            if value <= 7:
                return '0-7 days'
            elif value <= 10:
                return '8-10 days'
            elif value <= 15:
                return '11-15 days'
            elif value <= 29:
                return '16-30 days'
            else:
                return '30+ days'
        
        bucket = self.df[column].apply(bucket_value)
        return bucket


# In[ ]:





# In[ ]:




