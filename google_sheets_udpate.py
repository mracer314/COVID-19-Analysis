#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 01:28:15 2020

@author: Marvin Riley
"""

import pandas as pd
import pygsheets
#import matplotlib.pyplot as plt

def transfer_covid_data(df_global,df_uscases):
    #Authorize sheet credentials
    gc = pygsheets.authorize(service_file='/Users/admin/Documents/GitHub/COVID-19 Analysis/sheets-api-creds.json')
    
    sh = gc.open('COVID-19 Corona Virus Tracker')
    
    #Transfer World Wide Data
    wks_ww = sh.worksheet('title','Worldwide')
    wks_ww.set_dataframe(df_global['2020-04-25':],(98,2),copy_index=False,copy_head=False)
    
    #Transfer World Wide Data
    wks_us = sh.worksheet('title','US Cases')
    wks_us.set_dataframe(df_uscases['2020-04-25':],(95,3),copy_index=False,copy_head=False)
    
    
def read_covid_data():
    
    counties = [('Bergen','New Jersey'),
                ('Los Angeles','California'),
                ('Orange','California'),
                ('Santa Clara','California')]
    
    bay_area_counties = [('Santa Clara','California'),
                         ('Alameda','California'),
                         ('San Francisco','California'),
                         ('San Mateo','California'),
                         ('Contra Costa','California'),
                         ('Solano','California'),
                         ('Sonoma','California'),
                         ('Marin','California'),
                         ('Napa','California')]
    
    #Get the datasets from Github
    df_confirmed_us = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv')
    df_deaths_us = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv')
    df_confirmed_global = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
    df_deaths_global = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
    df_recovered_global = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')
    
    
    #Process Worldwide data
    clabels = df_confirmed_global.columns
    clabels = clabels[4:]
    
    df_countries_cf = df_confirmed_global.groupby(['Country/Region'])[clabels].sum().transpose()
    df_countries_dt = df_deaths_global.groupby(['Country/Region'])[clabels].sum().transpose()
    df_countries_rc = df_recovered_global.groupby(['Country/Region'])[clabels].sum().transpose()
    
    df_countries_cf['Cases WW'] = df_countries_cf.sum(axis=1)
    df_countries_dt['Deaths WW'] = df_countries_dt.sum(axis=1)
    df_countries_rc['Recovered WW'] = df_countries_rc.sum(axis=1)
    
    df_global = pd.DataFrame([])
    df_global['Cases WW'] = df_countries_cf['Cases WW']
    df_global['Deaths WW'] = df_countries_dt['Deaths WW']
    df_global['Recovered WW'] = df_countries_rc['Recovered WW']
    df_global['Cases China'] = df_countries_cf['China']
    df_global['Deaths China'] = df_countries_dt['China']
    df_global['Recovered China'] = df_countries_rc['China']
    df_global['Cases US'] = df_countries_cf['US']
    df_global['Deaths US'] = df_countries_dt['US']
    df_global['Recovered US'] = df_countries_rc['US']
    
    df_index = pd.to_datetime(df_global.index)
    df_global = df_global.set_index(df_index)
    
    #Process US Cases data
    clabels = df_confirmed_us.columns
    clabels = clabels[11:]
    
    df_counties_cf = df_confirmed_us.set_index(['Admin2','Province_State'])
    df_counties_dt = df_deaths_us.set_index(['Admin2','Province_State'])
    
    df_bay_counties_cf = df_counties_cf.loc[bay_area_counties]
    df_counties_cf = df_counties_cf.loc[counties]
    df_counties_dt = df_counties_dt.loc[counties]
    
    df_bay_counties_cf = df_bay_counties_cf[clabels].transpose()
    df_counties_cf = df_counties_cf[clabels].transpose()
    df_counties_dt = df_counties_dt[clabels].transpose()
    
    df_states_cf = df_confirmed_us.groupby(['Province_State'])[clabels].sum().transpose()
    df_states_dt = df_deaths_us.groupby(['Province_State'])[clabels].sum().transpose()
    
    df_uscases = pd.DataFrame([])
    df_uscases['Bay Area Cases'] = df_bay_counties_cf.sum(axis=1)
    df_uscases['Santa Clara Cases'] = df_counties_cf['Santa Clara']
    df_uscases['Santa Clara Deaths'] = df_counties_dt['Santa Clara']
    df_uscases['Orange Cases'] = df_counties_cf['Orange']
    df_uscases['Orange Deaths'] = df_counties_dt['Orange']
    df_uscases['Los Angeles Cases'] = df_counties_cf['Los Angeles']
    df_uscases['Los Angeles Deaths'] = df_counties_dt['Los Angeles']
    df_uscases['California Cases'] = df_states_cf['California']
    df_uscases['California Deaths'] = df_states_dt['California']
    df_uscases['New York Cases'] = df_states_cf['New York']
    df_uscases['New York Deaths'] = df_states_dt['New York']
    df_uscases['Bergen Cases'] = df_counties_cf['Bergen']
    df_uscases['Bergen Deaths'] = df_counties_dt['Bergen']
    df_uscases['New Jersey Cases'] = df_states_cf['New Jersey']
    df_uscases['New Jersey Deaths'] = df_states_dt['New Jersey']
    
    df_index = pd.to_datetime(df_uscases.index)
    df_uscases = df_uscases.set_index(df_index)
    
    return df_global, df_uscases

df_global, df_uscases = read_covid_data()
transfer_covid_data(df_global, df_uscases)


#plt.plot(df_uscases['Bay Area Cases'])