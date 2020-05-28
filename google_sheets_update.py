#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 01:28:15 2020

@author: mracer.codes
"""

import pandas as pd
import numpy as np
import pygsheets
import matplotlib.pyplot as plt
import matplotlib.dates as mpldates

def transfer_covid_data(df_global,df_uscases):
    """This function moves data to a Google sheets


    Parameters
    ----------
    df_global : Pandas Dataframe
        Dataframe of global cases from parse_data_from_sheet function.
    df_uscases : Pandas Dataframe
        Dataframe of global cases from parse_data_from_sheet function.

    Returns
    -------
    None.

    """

    #Authorize sheet credentials
    gc = pygsheets.authorize(service_file='/Users/admin/Documents/GitHub/' +
                             'COVID-19 Analysis/sheets-api-creds.json')

    sh = gc.open('COVID-19 Corona Virus Tracker')

    #Transfer World Wide Data
    wks_ww = sh.worksheet('title','Worldwide')
    wks_ww.set_dataframe(df_global['2020-04-25':],(98,2),
                         copy_index=False,copy_head=False)

    #Transfer World Wide Data
    wks_us = sh.worksheet('title','US Cases')
    wks_us.set_dataframe(df_uscases['2020-04-25':],(95,3),
                         copy_index=False,copy_head=False)

    print('Transfer successful!')

    return

def parse_df(df,numColumnsToIgnore,groupCol):
    """This function parses the CSVs from Johns Hopkins and drops the columns
    at the beginning, and sets the index to date time.

    Parameters
    ----------
    df : Pandas DataFrame
        DataFrame of COVID-19 timeseries data read from read_covid_data.
    numColumnsToIgnore : Int
        Number of initial columns to ignore before time sereies data starts.
    groupCol : Str or List
        Name of the column (or list of columns) used to group and return the
        data.

    Returns
    -------
    df : Pandas Dataframe
        Parsed data with the first columns dropped, data transposed, and the
        data grouped and summed.

    """


    clabels = df.columns
    clabels = clabels[numColumnsToIgnore:]
    df = df.groupby(groupCol)[clabels].sum().transpose()

    df_index = pd.to_datetime(df.index)
    df = df.set_index(df_index)

    return df

def read_covid_data():
    """Reads data from the Johns Hopkins CSSEGISandData COVID-19 repository
    and parses it to only show US counties and states, and countries data in
    the time domain. It also converts the dates to datetime format makes it
    the index.

    Returns
    -------
    df_dict : Dictionary
        Returns a dictionary o fdataframes with 8 entires as follows:
        Global Cases - Index:DateTime, Columns:Countries, Values:Cases
        Global Deaths - Index:DateTime, Columns:Countries, Values:Deaths
        Global Recoveries - Index:DateTime, Columns:Countries, Values:Recoveries
        States Cases - Index:DateTime, Columns:States, Values:Cases
        States Deaths - Index:DateTime, Columns:States, Values:Deaths
        State Pop - Index:States, Values:Population
        County Cases - Index:DateTime, Columns:Counties, Values:Cases
        County Deaths - Index:DateTime, Columns:Counties, Values:Deaths
        County Pop - Index:Counties, Values:Population.

    """

    #Website path
    gh_path_csv = ('https://raw.githubusercontent.com/CSSEGISandData/' +
                   'COVID-19/master/csse_covid_19_data/' +
                   'csse_covid_19_time_series/')
    #File names of CSVs
    file_confirmed_us = 'time_series_covid19_confirmed_US.csv'
    file_deaths_us = 'time_series_covid19_deaths_US.csv'
    file_confirmed_global = 'time_series_covid19_confirmed_global.csv'
    file_deaths_global = 'time_series_covid19_deaths_global.csv'
    file_recovered_global = 'time_series_covid19_recovered_global.csv'

    #Get the datasets from Github
    df_confirmed_us = pd.read_csv(gh_path_csv + file_confirmed_us)
    df_deaths_us = pd.read_csv(gh_path_csv + file_deaths_us)
    df_confirmed_global = pd.read_csv(gh_path_csv + file_confirmed_global)
    df_deaths_global = pd.read_csv(gh_path_csv + file_deaths_global)
    df_recovered_global = pd.read_csv(gh_path_csv + file_recovered_global)

    #Fix poorly named columns
    df_deaths_us = df_deaths_us.rename(columns={'Admin2':'County'})
    df_confirmed_us = df_confirmed_us.rename(columns={'Admin2':'County'})


    #Parse the data frames
    df_countries_cf = parse_df(df_confirmed_global,4,['Country/Region'])
    df_countries_dt = parse_df(df_deaths_global,4,['Country/Region'])
    df_countries_rc = parse_df(df_recovered_global,4,['Country/Region'])

    #Add world-wide totals
    df_countries_cf['Cases WW'] = df_countries_cf.sum(axis=1)
    df_countries_dt['Deaths WW'] = df_countries_dt.sum(axis=1)
    df_countries_rc['Recovered WW'] = df_countries_rc.sum(axis=1)

    #Get county and state populations
    df_states_pop = df_deaths_us.set_index('Province_State')
    df_states_pop = df_states_pop.groupby(
        ['Province_State'])['Population'].sum()

    df_county_pop = df_deaths_us.set_index(['Province_State',
                                            'County'])['Population']
    df_county_pop = df_county_pop[df_county_pop >0]

    #Process US Cases data at the county level
    df_counties_cf = df_confirmed_us.set_index(['Province_State','County'])
    df_counties_dt = df_deaths_us.set_index(['Province_State','County'])
    df_counties_cf = parse_df(df_confirmed_us,11,['Province_State','County'])
    df_counties_dt = parse_df(df_deaths_us,12,['Province_State','County'])

    #Process US Cases at the state level
    df_states_cf = parse_df(df_confirmed_us,11,['Province_State'])
    df_states_dt = parse_df(df_deaths_us,12,['Province_State'])

    #Organize the data in a dictionary for return
    df_dict ={'Global Cases':df_countries_cf,
              'Global Deaths':df_countries_dt,
              'Global Recoveries':df_countries_rc,
              'State Cases':df_states_cf,
              'State Deaths':df_states_dt,
              'County Cases':df_counties_cf,
              'County Deaths':df_counties_dt,
              'State Pop':df_states_pop,
              'County Pop':df_county_pop}

    return df_dict

def parse_data_for_sheet():
    """This function makes a read call for the latest COVID-19 data and then
    organizes the data into two data frames (1 global, and 1 of US cases)
    for eventual update of a Google Doc.

    Returns
    -------
    df_global : Pandas DataFrame
        Parsed dataframe of global data meant for the Worldwide tab.
    df_uscases : Pandas DataFrame
        Parsed dataframe of US data meant for the US Cases tab.

    """

    counties = [('New Jersey','Bergen'),
                ('California','Los Angeles'),
                ('California','Orange'),
                ('California','Santa Clara')]

    bay_area_counties = [('California','Santa Clara'),
                         ('California','Alameda'),
                         ('California','San Francisco'),
                         ('California','San Mateo'),
                         ('California','Contra Costa'),
                         ('California','Solano'),
                         ('California','Sonoma'),
                         ('California','Marin'),
                         ('California','Napa')]

    #Read the data from the Johns Hopkins site and assign to global,state,
    #   county etc.
    df_dict = read_covid_data()
    df_countries_cf = df_dict['Global Cases']
    df_countries_dt = df_dict['Global Deaths']
    df_countries_rc = df_dict['Global Recoveries']

    df_states_cf = df_dict['State Cases']
    df_states_dt = df_dict['State Deaths']

    df_counties_cf = df_dict['County Cases']
    df_counties_dt = df_dict['County Deaths']

    #Setup the Worldwide Page data frame
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

    #Filter selected counties
    df_bay_counties_cf = df_counties_cf[bay_area_counties]
    df_counties_cf = df_counties_cf[counties]
    df_counties_dt = df_counties_dt[counties]

    #Setup the US Cases Page data frame
    df_uscases = pd.DataFrame([])
    df_uscases['Bay Area Cases'] = df_bay_counties_cf.sum(axis=1)
    df_uscases['Santa Clara Cases'] = df_counties_cf['California',
                                                     'Santa Clara']
    df_uscases['Santa Clara Deaths'] = df_counties_dt['California',
                                                      'Santa Clara']
    df_uscases['Orange Cases'] = df_counties_cf['California','Orange']
    df_uscases['Orange Deaths'] = df_counties_dt['California','Orange']
    df_uscases['Los Angeles Cases'] = df_counties_cf['California',
                                                     'Los Angeles']
    df_uscases['Los Angeles Deaths'] = df_counties_dt['California',
                                                      'Los Angeles']
    df_uscases['California Cases'] = df_states_cf['California']
    df_uscases['California Deaths'] = df_states_dt['California']
    df_uscases['New York Cases'] = df_states_cf['New York']
    df_uscases['New York Deaths'] = df_states_dt['New York']
    df_uscases['Bergen Cases'] = df_counties_cf['New Jersey','Bergen']
    df_uscases['Bergen Deaths'] = df_counties_dt['New Jersey','Bergen']
    df_uscases['New Jersey Cases'] = df_states_cf['New Jersey']
    df_uscases['New Jersey Deaths'] = df_states_dt['New Jersey']

    df_index = pd.to_datetime(df_uscases.index)
    df_uscases = df_uscases.set_index(df_index)

    return df_global, df_uscases

def covid_data_summary():
    """Reads data from the Johns Hopkins github site and prints a summary of
    the current data to the command line.


    Returns
    -------
    None.

    """

    #Get data
    df_dict = read_covid_data()
    df_countries_cf = df_dict['Global Cases']
    df_countries_dt = df_dict['Global Deaths']
    df_countries_rc = df_dict['Global Recoveries']
    df_countries_cf = df_countries_cf.drop(columns='Cases WW')


    df_states_cf = df_dict['State Cases']
    df_states_dt = df_dict['State Deaths']
    df_state_pop = df_dict['State Pop']

    df_counties_cf = df_dict['County Cases']
    df_counties_dt = df_dict['County Deaths']
    df_county_pop = df_dict['County Pop']

    #Organize the data for display
    A = df_counties_cf.iloc[-1,:].sort_values(ascending=False)
    B = df_counties_cf.iloc[-1,:]/df_county_pop*100
    B = B.dropna().sort_values(ascending=False)
    C = df_counties_cf.iloc[-1,:]-df_counties_cf.iloc[-7,:]
    C = C.dropna().sort_values(ascending=False)
    D = C/df_county_pop*100
    D = D.dropna().sort_values(ascending=False)
    county_deaths_by_pop = df_counties_dt.iloc[-1,:]/df_county_pop*100
    county_deaths_by_pop = county_deaths_by_pop.dropna().sort_values(
        ascending=False)
    global_death_rates = df_countries_dt.iloc[-1,:]/df_countries_cf.iloc[-1,:]*100
    global_death_rates = global_death_rates.dropna().sort_values(ascending=False)

    #Output the information to the command line
    print('The Top 5 Most infected Counties in New Jersey by pop%')
    print(B['New Jersey'].head())
    print('')
    print('The Top 5 Most infected Counties in California by pop%')
    print(B['California'].head())
    print('')
    print('The Top 5 Most infected Counties in the US by pop%')
    print(B.head())
    print('')
    print('The Top 10 most infected counties in the US')
    print(A.head(10))
    print('')
    print('The Top 10 Trending Counties in the US [7 day change]')
    print(C.head(10))
    print('')
    print('The Top 10 Trending Counties in the US by pop%')
    print(D.head(10))
    print('')
    print('The  Top 10 Trending County Death Rates By Pop% in US')
    print(county_deaths_by_pop.head(10))


    #Calculate the time time to complete infection
    timeToInfectAll = (df_county_pop-A)/C*7
    timeToInfectAll = timeToInfectAll.replace([np.inf, -np.inf], np.nan)
    timeToInfectAll = timeToInfectAll.dropna().sort_values()
    timeToInfectAll = timeToInfectAll[timeToInfectAll>0]
    print('')
    print('The Top 10 Trending Counties in the US in population infection rate')
    print('[number of days to reach 100% spread]')
    print(timeToInfectAll.head(10))

    #Find out the countries with the fasting moving spread globally
    ln_df_countries_cf = np.log(df_countries_cf).diff()
    ln_df_countries_cf = np.mean(ln_df_countries_cf[-5:])/5
    ln_df_countries_cf = ln_df_countries_cf.transpose().sort_values(
        ascending=False)
    print('')
    print('The Top 10 Trending Countries')
    print(ln_df_countries_cf.head(10))

    #Find out the countries with the fasting moving spread globally
    diff_countries_cf = df_countries_cf.reset_index(drop=True).diff().dropna()
    diff_countries_cf = diff_countries_cf.iloc[-1].sort_values(ascending=False)
    print('')
    print('The Top 10 Countries with Most Added Cases')
    print(diff_countries_cf.head(10))

    #Find out which contries have the highest mortality rates.
    print('')
    print('The Top 10 Contries with highest death rates:')
    print(global_death_rates.head(10))


    return

def update_google_doc():
    """Updates a Google Doc with data taken from Johns Hopkins COVID dataset.


    Returns
    -------
    None.

    """

    df_global, df_uscases = parse_data_for_sheet()
    transfer_covid_data(df_global, df_uscases)

    return

def plot_key_statistics(df,key,title_suffix,color='b',varTitle='Cases'):
    """


    Parameters
    ----------
    df : TYPE
        DESCRIPTION.
    key : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """

    df_diff = df.diff().dropna()

    date_form = mpldates.DateFormatter("%b-%d")

    fig,ax = plt.subplots(2,1,sharex=True)

    ax[0].plot(df[key],color=color)
    ax[0].set_ylabel(varTitle)
    ax[0].set_title('Summary for ' + title_suffix)

    ax[1].bar(df_diff.index,df_diff[key],color=color)
    ax[1].set_xlabel('Date')
    ax[1].set_ylabel('Change in ' + varTitle)
    ax[1].xaxis.set_major_formatter(date_form)

    fig.tight_layout()
    plt.show(fig)

    return

def plot_worst_cases(df,varTitle='Cases',color='b'):
    """

    Parameters
    ----------
    df : TYPE
        DESCRIPTION.
    key : TYPE
        DESCRIPTION.
    title varTitle : TYPE, optional
        DESCRIPTION. The default is 'Cases'.

    Returns
    -------
    None.

    """

    df_diff = df.diff().dropna()

    date_form = mpldates.DateFormatter("%b-%d")

    fig,ax = plt.subplots(2,4)
    keys = df.columns

    for i in range(0,8):
        key = keys[i]
        ax[np.divmod(i,4)].plot(df[key],color=color)
        ax[np.divmod(i,4)].set_ylabel(varTitle)
        ax[np.divmod(i,4)].set_xlabel('Date')
        ax[np.divmod(i,4)].set_title(key)
        ax[np.divmod(i,4)].xaxis.set_major_formatter(date_form)

    #fig.tight_layout()
    plt.show(fig)

    fig2,ax2 = plt.subplots(2,4)

    for i in range(0,8):
        key = keys[i]
        ax2[np.divmod(i,4)].bar(df_diff.index,df_diff[key],color=color)
        ax2[np.divmod(i,4)].set_xlabel('Date')
        ax2[np.divmod(i,4)].set_ylabel('Change in ' + varTitle)
        ax2[np.divmod(i,4)].set_title(key)
        ax2[np.divmod(i,4)].xaxis.set_major_formatter(date_form)

    #fig2.tight_layout()
    plt.show(fig2)

    return


def run_daily_report():

    #Get data
    df_dict = read_covid_data()
    df_countries_cf = df_dict['Global Cases']
    df_countries_dt = df_dict['Global Deaths']
    df_countries_rc = df_dict['Global Recoveries']

    df_states_cf = df_dict['State Cases']
    df_states_dt = df_dict['State Deaths']
    df_state_pop = df_dict['State Pop']

    df_counties_cf = df_dict['County Cases']
    df_counties_dt = df_dict['County Deaths']
    df_county_pop = df_dict['County Pop']

    plot_key_statistics(df_counties_cf['2020-03-01':],('New Jersey','Bergen'),
                            'Bergen County, NJ')
    plot_key_statistics(df_counties_dt['2020-03-01':],
                        ('New Jersey','Bergen'),
                        'Bergen County, NJ',color='r',varTitle='Deaths')

    plot_key_statistics(df_counties_cf['2020-03-01':],('California','Santa Clara'),
                            'Santa Clara County, CA')
    plot_key_statistics(df_counties_dt['2020-03-01':],
                        ('California','Santa Clara'),
                        'Santa Clara County, CA',color='r',varTitle='Deaths')

    plot_key_statistics(df_counties_cf['2020-03-01':],('California','Los Angeles'),
                            'Los Angeles County, CA')
    plot_key_statistics(df_counties_cf['2020-03-01':],('California','Orange'),
                            'Orange County, CA')
    plot_key_statistics(df_counties_cf['2020-03-01':],('Illinois','Cook'),
                            'Cook County, IL')

    #State Summaries
    plot_key_statistics(df_states_cf['2020-03-01':],'New Jersey',
                            'New Jersey')
    plot_key_statistics(df_states_dt['2020-03-01':],'New Jersey',
                            'New Jersey',color='r',varTitle='Deaths')
    plot_key_statistics(df_states_cf['2020-03-01':],'California',
                            'California')
    plot_key_statistics(df_states_dt['2020-03-01':],'California',
                            'California',color='r',varTitle='Deaths')

    #Global Summaries
    plot_key_statistics(df_countries_cf['2020-03-01':],'US',
                            'USA')
    plot_key_statistics(df_countries_dt['2020-03-01':],'US',
                            'US',color='r',varTitle='Deaths')
    plot_key_statistics(df_countries_cf['2020-02-01':],'Cases WW',
                            'Cases WW')
    plot_key_statistics(df_countries_dt['2020-02-01':],'Deaths WW',
                            'Deaths WW',color='r',varTitle='Deaths')

    #Show Top 8 Trending Counties based on 7-Day change
    trending_counties = df_counties_cf.iloc[-1,:]-df_counties_cf.iloc[-7,:]
    trending_counties = trending_counties.dropna().sort_values(ascending=False)
    top8_US_county = df_counties_cf[trending_counties.head(8).index]
    plot_worst_cases(top8_US_county['2020-03-01':],varTitle='Cases',color='c')

    #Show Top 8 Trending Counties based on 7-Day change (normalized to pop)
    trending_counties_by_pop = trending_counties/df_county_pop*100
    trending_counties_by_pop = trending_counties_by_pop.dropna().sort_values(
        ascending=False)
    trending_counties_by_pop = df_counties_cf[
        trending_counties_by_pop.head(8).index]
    trending_counties_by_pop = trending_counties_by_pop.transpose().div(
        df_county_pop,axis=0).dropna().transpose()*100
    plot_worst_cases(trending_counties_by_pop['2020-03-01':],
                     varTitle='Cases',color='m')

    #Show Top 8 Trending Countries
    diff_countries_cf = df_countries_cf.reset_index(drop=True).diff().dropna()
    dfTop8countries = diff_countries_cf.iloc[-1,:].sort_values(ascending=False)
    dfTop8countries = df_countries_cf[dfTop8countries.head(9).index]
    dfTop8countries = dfTop8countries.drop(columns='Cases WW')
    plot_worst_cases(dfTop8countries['2020-03-01':],varTitle='Cases',color='g')

    return

