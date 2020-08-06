#!/usr/bin/env python
# coding: utf-8

# In[1]:

#COMMENT
import pandas as pd
import numpy as np
import pyxlsb
import re
import datetime
from pyxlsb import open_workbook as open_xlsb
from os import listdir
from os.path import isfile, join
from pyxlsb import convert_date
import xlsxwriter
import logging

logging.basicConfig(level=logging.DEBUG, filename="prospecting_tool.log", filemode='a', format='%(asctime)s - %(message)s')

# In[5]:

#extracting the year from the date column
def get_year(df, dictionary_match, dict_rep):
    list_date=[]
    list_month=[]
    temp_match_list=[]
    temp_rep_list = []
    df['DNMM_CATEGORY']= df['DNMM_CATEGORY'].str.upper()
    df = df[df['DNMM_CATEGORY'].notna()]
    
    for date in df[['FILING_DT_TM','DNMM_CATEGORY', 'PROSPECT_NAME']].itertuples():
        try:
            if type(date[1])==float:
                if date[1]>50000:
                    value= str(date[1])[0:4]
                else:
                    value= str(convert_date(date[1]))[0:4]
            else:
                value= date[1][0:4]
            list_date.append(int(value))
        except:
            list_date.append(-1)
        #try:
        temp_filing_month= str(date[1])[4:6]
        if type(date[1]) == datetime.datetime:
            filing_month= date[1].month
        else:
            filing_month = str(date[1])[4:6]
        list_month.append(filing_month)
#         except:
#             list_month.append('1')
        #temp_match= [dictionary_match[key] for key in dictionary_match.keys() if key.lower()==date[2].lower()]
    
        for key in dictionary_match.keys():
            if key.lower()==str(date[2]).lower():
                temp_match= [dictionary_match[key]]
                break
            else:
                temp_match= ['']
                
        temp_match_list.append(temp_match[0].upper())
        #print(temp_match_list)
        
        try:
            temp_rep= [dict_rep[key] for key in dict_rep.keys() if key==date[3]]
            temp_rep_list.append(temp_rep[0])
        except:
            temp_rep_list.append('')
    
    df['Rep assigned']= np.asarray(temp_rep_list)    
    df['DNMM categories 2'] =  np.asarray(temp_match_list)
    df['Filing Year']= list_date
    df['Filing Month']= list_month
    
    return df


# In[6]:

#creating month,date columns from the input dataframe 
def transform_others(df, dictionary_match, dict_rep):
    temp_match_list=[]; temp_activity_list=[];count=0; temp_rep_list=[]; filing_year_list=[]; filing_month_list=[]
    filing_date_list=[]
    x=0
    df['DNMM_CATEGORY']= df['DNMM_CATEGORY'].str.upper()
    df = df[df['DNMM_CATEGORY'].notna()]
    
    #df= df.dropna()
    for line in df[['DNMM_CATEGORY', 'PROSPECT_NAME', 'FILING_DT_TM']].itertuples():
        filing_date=''
        #temp_match= [dictionary_match[key] for key in dictionary_match.keys() if key.lower()==line[1].lower()]
        temp_rep= [dict_rep[key] for key in dict_rep.keys() if key==line[2]]
        
        for key in dictionary_match.keys():
            if key.lower()==line[1].lower():
                temp_match= [dictionary_match[key]]
        
        try:
            #temp_filing_month= str(line[3])[4:6]
            if type(line[3]) == datetime.datetime:
                filing_month= line[3].month
            else:
                filing_month = str(line[3])[4:6] 
        except:
            filing_month = ''
        
        #print(temp_match)
        #print(temp_match[0])
        temp_match_list.append(temp_match[0].upper())
        #temp_rep_list.append(temp_rep)
        filing_month_list.append(filing_month)
        filing_date_list.append(filing_date)

    df['DNMM categories 2']= np.asarray(temp_match_list)
    #df['Rep assigned']= np.asarray(temp_rep_list)
    df['Filing Month'] = np.asarray(filing_month_list)
    df['Rank Filing date'] = np.asarray(filing_date_list)
    
    return df


# In[7]:
#creating the 'Entity status categories 2' column for sumbysf tab

def transform_entity_stat(df, entity_status):
    temp_activity_list=[]
    df['index']= range(len(df))
    dict_entity= dict(zip(entity_status['ENTITY_STAT'].str.lower(), entity_status['ENTITY_STAT2'].str.lower()))
    for value in dict_entity.keys():
        if value == '(blank)':
            dict_entity['unresolved_1'] = dict_entity.pop('(blank)')
    df['ENTITY_STAT']= df['ENTITY_STAT'].fillna('unresolved_1')
    df['ENTITY_STAT'] = df['ENTITY_STAT'].str.lower() 
    
    index=[]
    l= list(set(list(df['ENTITY_STAT'].str.lower()))- set(list(dict_entity.keys())))
    for line in df[['index', 'ENTITY_STAT']].itertuples():
        if line[2] in l:
            index.append(line[1])
    
    df_entity= df.drop(index, axis=0)
    df_entity['ENTITY_STAT']= df_entity['ENTITY_STAT'].fillna('unresolved_1')
    df_entity['ENTITY_STAT'] = df_entity['ENTITY_STAT'].str.lower()
    
    for line in df_entity['ENTITY_STAT']:
        temp_activity= [dict_entity[key] for key in dict_entity.keys() if key==line]
        temp_activity_list.append(temp_activity[0].upper())
    
    df_entity['Entity status categories 2']= np.asarray(temp_activity_list)  
    return df_entity


# Sum By SF

# In[8]:

#preparing the data for sum_by_sf tab

def create_sum_by_sf(df, df_entity, dict_rep):
    try:
        df_sumbysf = pd.DataFrame(columns=['Prospect Name'])
        df_sumbysf=df_sumbysf.append({'Prospect Name': df['PROSPECT_NAME'].unique()[0]} ,ignore_index=True)

        rep_list=[]
        #for line in df_sumbysf[['Prospect Name']].itertuples():
            #temp_rep= [dict_rep[key] for key in dict_rep.keys() if key==line[1]]
            #rep_list.append(temp_rep[0])

        #df_sumbysf['Rep Assigned']= np.asarray(rep_list)

        df_sumbysf['Filing Sets Count (for Business Liens)']= df.drop_duplicates('FILING_NUM').shape[0]
        df_sumbysf['JMM total']= 0
        df_sumbysf['DNMM total'] = 0
        df_sumbysf['Entity Status Total'] = 0

        #EMM Category
        df_match = df.loc[df['JMM_CATEGORY']=='MATCH'].drop_duplicates('FILING_NUM')
        df_unresolved = df.loc[df['JMM_CATEGORY']=='UNRESOLVED'].drop_duplicates('FILING_NUM')
        df_mismatch = df.loc[df['JMM_CATEGORY']=='JURISDICTION MISMATCH'].drop_duplicates('FILING_NUM')
        df_sumbysf['JMM - Need Attention']= df_mismatch.shape[0]
        df_sumbysf['JMM-OK Filings'] = df_match.shape[0]
        df_sumbysf['JMM-Unresolved Filings'] = df_unresolved.shape[0]

        #DNMM Category
        df_match = df.loc[df['DNMM_CATEGORY']=='MATCH'].drop_duplicates('FILING_NUM')
        df_nomatch = df.loc[df['DNMM_CATEGORY']=='NO MATCH'].drop_duplicates('FILING_NUM')
        df_unresolved = df.loc[df['DNMM_CATEGORY']=='UNRESOLVED'].drop_duplicates('FILING_NUM')
        df_1= df.loc[df['DNMM_CATEGORY']=='SPECIAL CHARACTERS AND SPACES']
        df_2 = df.loc[df['DNMM_CATEGORY']=='NON-COMPLIANT NAME']
        df_3 = df.loc[df['DNMM_CATEGORY']=='PRIOR NAME']
        df_4= df.loc[df['DNMM_CATEGORY']=='MISSPELLINGS AND CORPORATE ENDINGS']
        df_need_attention= pd.concat([df_1, df_2, df_3, df_4]).drop_duplicates('FILING_NUM')
        df_sumbysf['DNMM - Need Attention']= df_need_attention.shape[0]
        df_sumbysf['DNMM - No Match'] = df_nomatch.shape[0]
        df_sumbysf['DNMM - Unresolved'] = df_unresolved.shape[0]
        df_sumbysf['DNMM - OK Filings'] = df_match.shape[0]

        #Entity Status Category
        df_match = df_entity.loc[df_entity['Entity status categories 2']=='ACTIVE'].drop_duplicates('FILING_NUM')
        df_need_attention = df_entity.loc[df_entity['Entity status categories 2']=='NEED ATTENTION'].drop_duplicates('FILING_NUM')
        df_dissolved = df_entity.loc[df_entity['Entity status categories 2']=='DISSOLVED'].drop_duplicates('FILING_NUM')
        df_unresolved = df_entity.loc[df_entity['Entity status categories 2']=='UNRESOLVED'].drop_duplicates('FILING_NUM')
        df_sumbysf['Entity Status - Entities that need attention']= df_need_attention.shape[0]
        df_sumbysf['Entity Status - Entities dissolved'] = df_dissolved.shape[0]
        df_sumbysf['Entity Status - Unresolved'] = df_unresolved.shape[0]
        df_sumbysf['Entity Status - Active'] = df_match.shape[0]
        logging.info('Returning results')
        return df_sumbysf
    except Exception:
        logging.error("Error in get_examples", exc_info=True)
        return {"description": "Error occured in get_examples!!"}
        
    
    


# In[15]:

#final processing of the DNMM,JMM,Entity columns

def final_calculation(df_sumbysf):
    df_sumbysf['JMM total']= df_sumbysf.apply(lambda row : row['JMM - Need Attention']+row['JMM-OK Filings']+row['JMM-Unresolved Filings'], axis = 1)
    df_sumbysf['DNMM total'] = df_sumbysf.apply(lambda row : row['DNMM - Need Attention']+row['DNMM - No Match']+row['DNMM - Unresolved']+row['DNMM - OK Filings'], axis = 1)
    
    df_sumbysf['Entity Status Total'] = df_sumbysf.apply(lambda row : row['Entity Status - Entities that need attention']+row['Entity Status - Entities dissolved']+row['Entity Status - Unresolved']+row['Entity Status - Active'], axis = 1)
    
    df_sumbysf['JMM - Need Attention%']= df_sumbysf.apply(lambda row : (row['JMM - Need Attention']/(row['JMM - Need Attention']+row['JMM-OK Filings']+row['JMM-Unresolved Filings'])), axis = 1)
    
    df_sumbysf['JMM-OK Filings%']= df_sumbysf.apply(lambda row : (row['JMM-OK Filings']/(row['JMM - Need Attention']+row['JMM-OK Filings']+row['JMM-Unresolved Filings'])), axis = 1)
    
    df_sumbysf['JMM-Unresolved Filings%']= df_sumbysf.apply(lambda row : (row['JMM-Unresolved Filings']/(row['JMM - Need Attention']+row['JMM-OK Filings']+row['JMM-Unresolved Filings'])), axis = 1)
    
    df_sumbysf['DNMM - Need Attention%']= df_sumbysf.apply(lambda row : (row['DNMM - Need Attention']/(row['DNMM - Need Attention']+row['DNMM - No Match']+row['DNMM - Unresolved']+row['DNMM - OK Filings'])), axis = 1)
    
    df_sumbysf['DNMM - No Match%']= df_sumbysf.apply(lambda row : (row['DNMM - No Match']/(row['DNMM - Need Attention']+row['DNMM - No Match']+row['DNMM - Unresolved']+row['DNMM - OK Filings'])), axis = 1)
    
    df_sumbysf['DNMM - Unresolved%']= df_sumbysf.apply(lambda row : (row['DNMM - Unresolved']/(row['DNMM - Need Attention']+row['DNMM - No Match']+row['DNMM - Unresolved']+row['DNMM - OK Filings'])), axis = 1)
    
    df_sumbysf['DNMM - OK Filings%']= df_sumbysf.apply(lambda row : (row['DNMM - OK Filings']/(row['DNMM - Need Attention']+row['DNMM - No Match']+row['DNMM - Unresolved']+row['DNMM - OK Filings'])), axis = 1)
    
    df_sumbysf['Entity Status - Entities that need attention%']= df_sumbysf.apply(lambda row : (row['Entity Status - Entities that need attention']/(row['Entity Status - Entities that need attention']+row['Entity Status - Entities dissolved']+row['Entity Status - Unresolved']+row['Entity Status - Active'])), axis = 1)
    
    df_sumbysf['Entity Status - Entities dissolved%']= df_sumbysf.apply(lambda row : (row['Entity Status - Entities dissolved']/(row['Entity Status - Entities that need attention']+row['Entity Status - Entities dissolved']+row['Entity Status - Unresolved']+row['Entity Status - Active'])), axis = 1)
    
    df_sumbysf['Entity Status - Unresolved%']= df_sumbysf.apply(lambda row : (row['Entity Status - Unresolved']/(row['Entity Status - Entities that need attention']+row['Entity Status - Entities dissolved']+row['Entity Status - Unresolved']+row['Entity Status - Active'])), axis = 1)
    
    df_sumbysf['Entity Status - Active%']= df_sumbysf.apply(lambda row : (row['Entity Status - Active']/(row['Entity Status - Entities that need attention']+row['Entity Status - Entities dissolved']+row['Entity Status - Unresolved']+row['Entity Status - Active'])), axis = 1)
    
    return df_sumbysf, df_sumbysf.columns


# In[16]:

#getting four batches of examples for each prospect
# def get_examples(df, dictionary_match, dict_rep):
#     #try:
#     df= get_year(df, dictionary_match, dict_rep)
#     df= df.sort_values(by=['Filing Year'], ascending=False)
#     df= df.drop(columns=['index'])

#     df_temp_ex1= df[df['DNMM_CATEGORY']=='MISSPELLINGS AND CORPORATE ENDINGS']  
#     df_temp_ex1_1 = df_temp_ex1[df_temp_ex1['JMM_CATEGORY']=='MATCH']
#     df_ex1= df_temp_ex1_1[df_temp_ex1_1['Entity status categories 2']=='ACTIVE'][:10]

#     df_temp_ex2= df[df['DNMM_CATEGORY']=='MATCH']  
#     df_temp_ex2_1 = df_temp_ex2[df_temp_ex2['JMM_CATEGORY']=='JURISDICTION MISMATCH']
#     df_ex2= df_temp_ex2_1[df_temp_ex2_1['Entity status categories 2']=='ACTIVE'][:10]

#     df_temp_ex3= df[df['DNMM_CATEGORY']=='MATCH']  
#     df_temp_ex3_1 = df_temp_ex3[df_temp_ex3['JMM_CATEGORY']=='MATCH']
#     df_ex3= df_temp_ex3_1[df_temp_ex3_1['Entity status categories 2']=='NEED ATTENTION'][:10]

#     df_temp_ex4= df[df['DNMM_CATEGORY']=='MATCH']  
#     df_temp_ex4_1 = df_temp_ex4[df_temp_ex4['JMM_CATEGORY']=='MATCH']
#     df_ex4= df_temp_ex4_1[df_temp_ex4_1['Entity status categories 2']=='DISSOLVED'][:10]

#     final_df= pd.concat([df_ex1, df_ex2, df_ex3, df_ex4])
#     final_df.rename(columns = {'PROSPECT_NAME': 'Prospect Name'}, inplace = True)

#     #logging.info('Returning results')
#     return final_df.columns, final_df

def get_examples(df, dictionary_match, dict_rep):
    #try:
    df= get_year(df, dictionary_match, dict_rep)
    df= df.sort_values(by=['Filing Year'], ascending=False)
    df= df.drop(columns=['index'])
    df['INCORPORATION_STATE_CD']= df['INCORPORATION_STATE_CD'].fillna('')
    df['CHT_FILING_STATE_CD']= df['CHT_FILING_STATE_CD'].fillna('')
    
    df_temp_ex1= df[df['DNMM_CATEGORY']=='MISSPELLINGS AND CORPORATE ENDINGS']  
    df_temp_ex1_1 = df_temp_ex1[df_temp_ex1['JMM_CATEGORY']=='MATCH']
    df_temp_ex1_1 = df_temp_ex1_1[df_temp_ex1_1['INCORPORATION_STATE_CD']!='']
    df_temp_ex1_1 = df_temp_ex1_1[df_temp_ex1_1['CHT_FILING_STATE_CD']!='']
    df_temp_ex1_1 = df_temp_ex1_1[df_temp_ex1_1['INCORPORATION_STATE_CD']!='XX | YY']
    df_ex1= df_temp_ex1_1[df_temp_ex1_1['Entity status categories 2']=='ACTIVE'][:10]

    df_temp_ex2= df[df['DNMM_CATEGORY']=='MATCH']  
    df_temp_ex2_1 = df_temp_ex2[df_temp_ex2['JMM_CATEGORY']=='JURISDICTION MISMATCH']
    df_temp_ex2_1 = df_temp_ex2_1[df_temp_ex2_1['INCORPORATION_STATE_CD']!='']
    df_temp_ex2_1 = df_temp_ex2_1[df_temp_ex2_1['CHT_FILING_STATE_CD']!='']
    df_temp_ex2_1 = df_temp_ex2_1[df_temp_ex2_1['INCORPORATION_STATE_CD']!='XX | YY']
    #print(df_temp_ex2_1)
    df_ex2= df_temp_ex2_1[df_temp_ex2_1['Entity status categories 2']=='ACTIVE'][:10]

    df_temp_ex3= df[df['DNMM_CATEGORY']=='MATCH']  
    df_temp_ex3_1 = df_temp_ex3[df_temp_ex3['JMM_CATEGORY']=='MATCH']
    df_temp_ex3_1 = df_temp_ex3_1[df_temp_ex3_1['INCORPORATION_STATE_CD']!='']
    df_temp_ex3_1 = df_temp_ex3_1[df_temp_ex3_1['CHT_FILING_STATE_CD']!='']
    df_temp_ex3_1 = df_temp_ex3_1[df_temp_ex3_1['INCORPORATION_STATE_CD']!='XX | YY']
    df_ex3= df_temp_ex3_1[df_temp_ex3_1['Entity status categories 2']=='NEED ATTENTION'][:10]

    df_temp_ex4= df[df['DNMM_CATEGORY']=='MATCH']  
    df_temp_ex4_1 = df_temp_ex4[df_temp_ex4['JMM_CATEGORY']=='MATCH']
    df_temp_ex4_1 = df_temp_ex4_1[df_temp_ex4_1['INCORPORATION_STATE_CD']!='']
    df_temp_ex4_1 = df_temp_ex4_1[df_temp_ex4_1['CHT_FILING_STATE_CD']!='']
    df_temp_ex4_1 = df_temp_ex4_1[df_temp_ex4_1['INCORPORATION_STATE_CD']!='XX | YY']
    df_ex4= df_temp_ex4_1[df_temp_ex4_1['Entity status categories 2']=='DISSOLVED'][:10]

    final_df= pd.concat([df_ex1, df_ex2, df_ex3, df_ex4])
    final_df.rename(columns = {'PROSPECT_NAME': 'Prospect Name'}, inplace = True)

    logging.info('Returning results')
    return final_df.columns, final_df

#     except Exception:
#         logging.error("Error in get_examples", exc_info=True)
#         return {"description": "Error occured in get_examples!!"}
        




# In[17]:




