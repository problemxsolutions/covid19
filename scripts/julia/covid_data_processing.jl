# System: Linux 5.4.0-40-generic, Ubuntu 20.04
# Julia: Version 1.4.1 (2020-04-14)

# COVID Tracking Project
# URL: https://covidtracking.com/data/api

# World Population Review: US States
# URL: https://worldpopulationreview.com/states

using CSVFiles
using DataFrames
using Dates
using Chain

base_url = "https://api.covidtracking.com"
all_states = "/v1/states/daily.csv"
# current_states = "/v1/states/current.csv"

# state_specific_api = "/v1/states/{state}/daily.csv"
# current_state = "/v1/states/{state}/current.csv"
base_dir = "/home/linux/ProblemXSolutions.com/DataProjects/covid19"
filename = "/data/all_states_daily_covid_jl.csv"

url = string(base_url, all_states)
download(url, string(base_dir, filename));

# Alternative Method
# import HTTP
# import CSV
# df = HTTP.get(url).body |> CSV.Rows |> DataFrame


# ******************************************************************************
# Read in the data

state_data = load(string(base_dir, filename)) |> DataFrame
state_pop_data = load(string(base_dir,"/data/state_populations.csv")) |> DataFrame
state_name_lookup_data = load(string(base_dir, "/data/state_lu.csv")) |> DataFrame

# # ******************************************************************************
# Data Processing

state_pop_reduced =
    @chain state_pop_data begin
        select(([:State, :Pop, :density]))
        rename(Dict(:State => :state))
        transform(:state => (x -> strip.(x)) => :state)
    end

state_pop_data = Nothing

state_name_lookup_data.state = strip.(state_name_lookup_data.state)

state_pop_reduced =
    @chain state_pop_reduced begin
        leftjoin(
            state_name_lookup_data,
            on =:state)
        select(Not([:state]))
        rename(Dict(:state_abr => :state))
    end

# Since Julia is sensitive when applying a function to a field with missing
# values, I will remove those data points.  I can safely do this because I know
# the values being removed are territories and districts that I am not
# processing.  There are processes you can do to work with missing values but
# I dont need to implement for this effort
dropmissing!(state_pop_reduced)
state_pop_reduced.state = strip.(state_pop_reduced.state)

state_data_enhanced =
    @chain state_data begin
        transform(:state => (x -> strip.(x)) => :state,
        :date => (x -> Date.(string.(x), Dates.DateFormat("yyyymmdd"))) => :date)
        leftjoin(state_pop_reduced,
            on =:state)
        sort([:state,:date])
    end

state_data = Nothing

# Define functions
function deltas(v, k)
    [ 1 <= i-k <= length(v) ? v[i]-v[i-k] : 0 for i=1:length(v) ]
end

function pop_adjusted(x, y)
    # Adjusting for population to help normalize numbers between states.
    # (daily_{cases,deaths} / population of state) * 100,000
    [(x / y) * 100000]
end

function moving_average(v,k)
    [ 1 <= i-k <= length(v) ? sum(@view v[(i-k+1):i])/k : 0 for i in 1:length(v) ]
end

state_data_enhanced =
    @chain state_data_enhanced begin
        groupby(:state)
        transform(:recovered => (x -> deltas(x, 1)) => :daily_recover)
        transform(
            [:positiveIncrease, :Pop] => ByRow(pop_adjusted) => :daily_cases_adj,
            [:daily_recover, :Pop] => ByRow(pop_adjusted) => :daily_recover_adj,
            [:deathIncrease, :Pop] => ByRow(pop_adjusted) => :daily_deaths_adj)
        transform(
            :positiveIncrease => (x -> moving_average(x, 7)) => :active_roll7,
            :daily_recover => (x -> moving_average(x, 7)) => :recovered_roll7,
            :deathIncrease => (x -> moving_average(x, 7)) => :deaths_roll7,
            :daily_cases_adj => (x -> moving_average(x, 7)) => :active_roll7_adj,
            :daily_recover_adj => (x -> moving_average(x, 7)) => :recovered_roll7_adj,
            :daily_deaths_adj => (x -> moving_average(x, 7)) => :deaths_roll7_adj)
    end

# ******************************************************************************
state_enhanced_reduced =
    @chain state_data_enhanced begin
        select(
            :date, :state,
            :positive, :recovered, :death,
            :positiveIncrease, :daily_cases_adj,
            :active_roll7, :active_roll7_adj,
            :daily_recover, :daily_recover_adj,
            :recovered_roll7, :recovered_roll7_adj,
            :deathIncrease, :daily_deaths_adj,
            :deaths_roll7, :deaths_roll7_adj,
            :hospitalizedIncrease, :hospitalizedCurrently,
            :inIcuCurrently
        )
    end

save(string(base_dir, "/data/state_data_enhanced_jl.csv"), state_data_enhanced)
save(string(base_dir, "/data/state_enhanced_reduced_jl.csv"), state_enhanced_reduced)

# Alternative:
# using CSV
# CSV.write(string(base_dir, "/data/state_enhanced_reduced_jl_mod1.csv"),
#     state_enhanced_reduced);
