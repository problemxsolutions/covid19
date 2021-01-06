# System: Linux 5.4.0-40-generic, Ubuntu 20.04
# Python: Version 3.8.5 (2020-07-28)
# GCC 9.3.0

# COVID Tracking Project
# URL: https://covidtracking.com/data/api

# World Population Review: US States
# URL: https://worldpopulationreview.com/states

import urllib.request

import pandas as pd

base_url = 'https://api.covidtracking.com'
all_states = '/v1/states/daily.csv'
# current_states = '/v1/states/current.csv'

# state_specific_api = '/v1/states/{state}/daily.csv'
# current_state = '/v1/states/{state}/current.csv'
base_dir = '/home/linux/ProblemXSolutions.com/DataProjects/covid19'
filename = '/data/all_states_daily_covid_py.csv'
urllib.request.urlretrieve(url=base_url + all_states,
                          filename=base_dir + filename)

# ******************************************************************************
# Read in the data

state_data = pd.read_csv(base_dir + filename, low_memory=False)
state_pop_data = pd.read_csv(base_dir + '/data/state_populations.csv')
state_name_lookup_data = pd.read_csv(base_dir + '/data/state_lu.csv')

# # ******************************************************************************
# Data Processing

state_pop_reduced = state_pop_data.filter(items=['State', 'Pop', 'density'])
del state_pop_data

# had to insert this since python merge or read_csv doesn't work the same as in R versions
state_pop_reduced.loc[:, 'State'] = state_pop_reduced.loc[:, 'State'].str.strip()
state_name_lookup_data.loc[:, 'state'] = state_name_lookup_data.loc[:, 'state'].str.strip()

state_pop_reduced = pd.merge(left=state_pop_reduced,
                             right=pd.DataFrame(state_name_lookup_data),
                             how='left',
                             left_on='State',
                             right_on='state')

state_pop_reduced.drop(columns=['State', 'state'],
                       inplace=True)
state_pop_reduced.rename(columns={'state_abr':'state'},
                         inplace=True)

# Adjusting for population to help normalize numbers between states.
# (daily_{cases,deaths} / population of state) * 100,000

state_data.loc[:, 'state'] = state_data.loc[:, 'state'].str.strip()
state_pop_reduced.loc[:, 'state'] = state_pop_reduced.loc[:, 'state'].str.strip()

state_data_enhanced = pd.merge(left=state_data,
                               right=state_pop_reduced,
                               on = 'state',
                               how='left')
del state_data

state_data_enhanced['date'] = pd.to_datetime(state_data_enhanced['date'],
                                             format='%Y%m%d')
state_data_enhanced = \
    state_data_enhanced \
        .sort_values(by=['state', 'date']) \
        .assign(
        daily_recover = state_data_enhanced.groupby('state')['recovered'].transform('diff')
    ) \
        .assign(
        daily_cases_adj = lambda x: (x['positiveIncrease'] / x['Pop']) * 100000,
        daily_recover_adj = lambda x: (x['daily_recover'] / x['Pop']) * 100000,
        daily_deaths_adj = lambda x: (x['deathIncrease'] / x['Pop']) * 100000
    )

state_data_enhanced = \
    state_data_enhanced \
        .assign(active_roll7 = state_data_enhanced.groupby('state')['positiveIncrease']
                .transform(lambda x: x.rolling(window=7, min_periods=1).mean()),
                recovered_roll7 = state_data_enhanced.groupby('state')['daily_recover_adj']
                .transform(lambda x: x.rolling(window=7, min_periods=1).mean()),
                deaths_roll7 = state_data_enhanced.groupby('state')['deathIncrease']
                .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
                ) \
        .assign(active_roll7_adj = state_data_enhanced.groupby('state')['daily_cases_adj']
                .transform(lambda x: x.rolling(window=7, min_periods=1).mean()),
                recovered_roll7_adj = state_data_enhanced.groupby('state')['daily_recover_adj']
                .transform(lambda x: x.rolling(window=7, min_periods=1).mean()),
                deaths_roll7_adj = state_data_enhanced.groupby('state')['daily_deaths_adj']
                .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
                )

state_enhanced_reduced = state_data_enhanced.filter(
    items=[
        'date', 'state',
        'positive', 'recovered', 'death',
        'positiveIncrease', 'daily_cases_adj', 'active_roll7', 'active_roll7_adj',
        'daily_recover', 'daily_recover_adj', 'recovered_roll7', 'recovered_roll7_adj',
        'deathIncrease', 'daily_deaths_adj', 'deaths_roll7', 'deaths_roll7_adj',
        'hospitalizedIncrease', 'hospitalizedCurrently', 'inIcuCurrently'
    ]
)

state_data_enhanced.to_csv(base_dir + '/data/state_data_enhanced_py.csv')
state_enhanced_reduced.to_csv(base_dir + '/data/state_enhanced_reduced_py.csv')
