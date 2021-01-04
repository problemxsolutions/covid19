# System: Linux 5.4.0-40-generic, Ubuntu 20.04
# R: Version 4.0.3 (2020-10-10)
# RStudio: Version 1.3.1093

# COVID Tracking Project
# URL: https://covidtracking.com/data/api

# World Population Review: US States
# URL: https://worldpopulationreview.com/states

library(tidyverse)
library(magrittr)
library(lubridate)

base_url <- 'https://api.covidtracking.com'
all_states <- '/v1/states/daily.csv'
# state_specific_api <- '/v1/states/{state}/daily.csv'

filename <- paste0('./data/all_states_daily_covid.csv')
download.file(url = paste0(base_url, all_states), 
              destfile = filename)

# # ******************************************************************************
# Read in the data

state_data <- read_csv(file = filename)
state_pop_data <- read_csv(file = './data/state_populations.csv')
state_name_lookup_data <- read_csv(file = './data/state_lu.csv')

# # ******************************************************************************
# Data Processing

state_pop_reduced <- 
  state_pop_data %>% 
  select(State, Pop, density) %>% 
  left_join(x = ., y = state_name_lookup_data, by = c('State' = 'state')) %>% 
  select(-1) %>% 
  rename(state = state_abr)


# Adjusting for population to help normalize numbers between states.
# (daily_{cases,deaths} / population of state) * 100,000
state_data_enhanced <- 
  state_data %>% 
  left_join(x = ., y = state_pop_reduced, by = c('state')) %>% 
  mutate(date = ymd(date)) %>% 
  group_by((state)) %>% 
  arrange(state, date) %>% 
  mutate(daily_recover = recovered - lag(recovered, default = first(recovered)),
    # daily_cases = positive - lag(positive, default = first(positive)),
         # daily_deaths = death - lag(death, default = first(death))
    ) %>%
  mutate(daily_cases_adj = (positiveIncrease / Pop) * 100000,
         daily_recover_adj = (daily_recover / Pop) * 100000,
         daily_deaths_adj = (deathIncrease / Pop) * 100000,
         active_roll7 = zoo::rollmean(positiveIncrease, k = 7, fill = NA),
         recovered_roll7 = zoo::rollmean(daily_recover_adj, k = 7, fill = NA),
         deaths_roll7 = zoo::rollmean(deathIncrease, k = 7, fill = NA)) %>% 
  mutate(active_roll7_adj = zoo::rollmean(daily_cases_adj, k = 7, fill = NA),
         recovered_roll7_adj = zoo::rollmean(daily_recover_adj, k = 7, fill = NA),
         deaths_roll7_adj = zoo::rollmean(daily_deaths_adj, k = 7, fill = NA))

state_enhanced_reduced <- 
  state_data_enhanced %>% 
  select(date, state, 
         positive, recovered, death, 
         # daily_cases, daily_deaths, 
         positiveIncrease, daily_cases_adj, active_roll7, active_roll7_adj, 
         daily_recover, daily_recover_adj, recovered_roll7, recovered_roll7_adj,
         deathIncrease, daily_deaths_adj, deaths_roll7, deaths_roll7_adj, 
         hospitalizedIncrease, hospitalizedCurrently, inIcuCurrently
         
         )

write_csv(file = './data/state_data_enhanced.csv', x = state_data_enhanced)
write_csv(file = './data/state_enhanced_reduced.csv', x = state_enhanced_reduced)
