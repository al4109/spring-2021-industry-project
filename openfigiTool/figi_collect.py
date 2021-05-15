import sys, os
sys.path.insert(0, '/content/drive/MyDrive/IEORIndustrialProject/figi_yahoo_model')

from figi_api import *

import pandas as pd
import numpy as np
import time
from datetime import datetime
import pickle

DIR = os.path.abspath(os.getcwd())

"""
Security Id type used
"""
idTypes = {
    'Cusip': 'ID_CUSIP',
    'Isin': 'ID_ISIN',
    'TickerSymbol': 'TICKER'
}

"""
A helper function to construct a request to query data based on Ids (CUSIP/ISIN/SEDOL)
"""
def construct_jobs_mapping(Ids):
    """
    Ids: dict
        dictionary of list of ids. Key is the id type, and value is list of ids
    Return
        dict: job request used to query data from openfigi
    """
    jobs = []
    for IdType in Ids:
        for id in Ids[IdType]:
            job = dict()
            job['idType'] = IdType
            job['idValue'] = id
            jobs.append(job)
    return jobs

"""
read output from openfigi data
"""
def parse_output_extract_first(output):
    """
    output: dict
        Output dict from reading openfigi json file
    Return 
        list: the first chunk of info as a list
    """
    ret = []
    if output:
        info = output[0]['data'][0]
        return [info['securityType'], info['securityType2']]

"""
A helper function to get the type based on Ids
"""
def getOutput(IdType, idList):
    """
    IdType: str
        String that indicates the id type. Use defined idTypes
    idList: list
        list that contains ids of a specific type
    Return
        dict: key=>id , value=> list of two types from openfigi
    """
    idInfo = {}
    jobsList = []
    counter = 0
    notwork = []
    for id in idList:
        counter += 1
        jobsList.append(id)
        if counter % 10 == 0:
            print(counter)
            time.sleep(2)
        try:
            requestJobs = construct_jobs_mapping({IdType:jobsList})
            output = map_jobs(requestJobs)
            idInfo[jobsList[0]] = parse_output_extract_first(output)
        except:
            idInfo[jobsList[0]] = ["NotFound", "NotFound"]
            # notwork.append(id)
        # for i in len(output):
        # cusipinfo[jobsList[i]] = parse_output_extract_first(output)
        jobsList = []
    return idInfo

"""
Get figi type from pandas dataframe
"""
def figi_from_pd(df, IdType):
    """
    df: pandas dataframe
        A pandas dataframe with three columns of different ids
    IdType: list
        list of IdType. Prefer 'Cusip', 'Isin', 'TickerSymbol'.
    Return
        
    """
    d = df[['Cusip', 'Isin', 'TickerSymbol']].drop_duplicates()

    cusip_list = df['Cusip'].unique()
    isin_list = df['Isin'].unique()
    ticker_symbols = df['TickerSymbol'].unique()

    cusipinfo = getOutput('ID_CUSIP', cusip_list.tolist())
    isinInfo = getOutput('ID_ISIN', isin_list.tolist())
    tickerSymbolInfo = getOutput('TICKER', ticker_symbols.tolist())

    cusip_df = construct_df('cusip', cusipinfo)
    isin_df = construct_df('isin', isinInfo)
    ticker_df = construct_df('tickerSymbol', tickerSymbolInfo)

    current_time = datetime.now().strftime("%H%M%S")
    cusip_df.to_csv(DIR + "/cusip_type_figi" + current_time + ".csv")
    isin_df.to_csv(DIR + "/isin_type_figi"+current_time+".csv")
    ticker_df.to_csv(DIR + "/ticker_type_figi"+current_time+".csv")

    return [cusip_df, isin_df, ticker_df]


def construct_df(idType, InfoDict):
    df_init = {idType:[], 
           'type1':[], 
           'type2':[]}
    for id in InfoDict:
        df_init[idType].append(id)
        temp = InfoDict[id]
        df_init['type1'].append(temp[0])
        df_init['type2'].append(temp[1])
    return pd.DataFrame(data=df_init)