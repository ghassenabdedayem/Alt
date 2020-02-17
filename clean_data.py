#Clean_dataset

file ="https://raw.githubusercontent.com/ghassenabdedayem/AltCoins/master/consolidated_coin_data.csv"

import pandas as pd
import numpy as np
from IPython.display import Markdown, display
from datetime import timedelta

df=pd.read_csv(file,sep=',')

'''Prépartion: après analyse du dataset, il s'avère que certaines valeurs manquent à certaines dates 
pour certaines monnaies,et qu'à l'inverse à certaine sdates, certaines monnaies ont deux valeurs. 
Ces valeurs doublons semblent fausses.
Elles sont donc supprimées, et remplacées par la moyenne des valeurs à J+1 et J-1.'''

#Convertir les numériques en numérique, et les dates en date

def convert_to_numeric_and_date(df):
    num=['Open','High','Low','Close','Volume','Market Cap']

    for n in num:
        if type(df[n]) == str:
            df[n]=df[n].str.replace(',','')
            df[n]=pd.to_numeric(df[n])

    try:
        df['Date'] = pd.to_datetime(df['Date'])
    except:
        pass
    return(df)

#Rechercher les doublons et les manquants
def trouver_doublons_and_missing(df):
    indexes=df.Date.value_counts()[df.Date.value_counts()!=12].index
    df_pb=df[df.Date.isin(indexes)]
    dates_doublons=sorted(indexes[0:2])
    dates_manquants=sorted(indexes[2:4])
    dates_manquants_df=df[df.Date.isin(dates_manquants)]
    currency_diff=list(set(df.Currency.unique())-set(dates_manquants_df.Currency.unique()))
    df_doublons=df[df.Date.isin(dates_doublons)]
    df_doublons=df_doublons[df_doublons.Currency.isin(currency_diff)]
    return dates_doublons,dates_manquants,currency_diff,df_doublons.index,df_doublons
    
#Convert val
def convert_val(string):
    count=0
    string=list(string)
    for i in range(len(string)-1):
        if string[i]=='.':
            count+=1
        if string[i]=='.' and count>1:
            del(string[i])
    string=''.join(string)
    string=string.replace(',','')
    string=float(string)
    return(string)

#Rajouter missing _ valeur moyenne à J-1 et J+1
def prepare_missing_value(df,date,currency_list):
    num=['Open','High','Low','Close','Volume','Market Cap']
    my_df=pd.DataFrame(columns=num)
    for curr in currency_list:
        a=df[(df.Date==(date-timedelta(1)))][df.Currency==curr]
        b=df[(df.Date==(date+timedelta(2)))][df.Currency==curr]
        c=a.append(b)
        c=c[num]
        means=[]
        for col in c.columns:
            values=[]
            for val in c[col]:
                values.append(convert_val(val))
            means.append(np.mean(values))
        d=[curr,date]
        e=[curr,date+timedelta(1)]
        for element in means:
            d.append(element)
            e.append(element)
        my_df=my_df.append(pd.DataFrame([d,e],columns=df.columns))
    return(my_df)

#Ajouter les valeurs manquantes au dataframe
def add_missing_value(df,date,currency_list):
    values=prepare_missing_value(df,date,currency_list)
    df=df.append(values)
    return(df)      
    
#Supprimer les doublons et remplacer les manquants
def supprimer_doublons(df):
    drop=trouver_doublons_and_missing(df)[3]
    df=df.drop(drop)
    return(df)
    
#All_together

def clean_df(df):
    num=['Open','High','Low','Close','Volume','Market Cap']
    df_clean=convert_to_numeric_and_date(df)
    infos=trouver_doublons_and_missing(df_clean)
    df_clean=supprimer_doublons(df_clean)
    df_clean=add_missing_value(df_clean,infos[1][0],infos[2])
    df_clean=df_clean[['Currency','Date','Open','High','Low','Close','Volume','Market Cap']]
    return(df_clean)
