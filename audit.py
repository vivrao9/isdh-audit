import os
import pandas as pd
from pathlib import Path
import sys

#create a list of all counties
#we do this to have a list of all counties as string, and also to cross-reference in later math where we need the number 92
list_of_counties = []
df = pd.read_excel(r'C:\Users\Vivek Rao\Desktop\IDS\Digital\Coronavirus\py\2020-08-06.xlsx')
for i in range(len(df['namelsad10'])):
    county = df['namelsad10'][i]
    list_of_counties.append(county)

#create a list with all dates from 02-26-2020 through the latest date MULTIPLIED by the number of counties (92)
list_of_dates = []
df = pd.read_csv(r'C:\Users\Vivek Rao\Desktop\IDS\Digital\Coronavirus\py\indiana-covid19-data-aug-7.csv')
for i in range(len(df['report_date'])):
    date = df['report_date'][i]
    list_of_dates.append(date)
list_of_dates = list(dict.fromkeys(list_of_dates))
list_of_dates = list_of_dates*len(list_of_counties)

#define a function to find the number of cases when first/originally noted
def fetch_date_of(county, report_date):
    try:
        original_filename = report_date + '.xlsx'
        original_df = pd.read_excel(Path(r'C:\Users\Vivek Rao\Desktop\IDS\Digital\Coronavirus\py',original_filename))
        #the reason we say col #5 (or iloc #4) is because that's the location of covid_count_cumsum
        return original_df.loc[original_df['namelsad10'] == county].iloc[:,4].item()
    except:
        pass
    
#define a function to find the number of cases on subsequent dates
def fetch_subsequent_data(county, report_date):
    for file in os.listdir(r'C:\Users\Vivek Rao\Desktop\IDS\Digital\Coronavirus\py'):
        if (file.startswith('indiana-covid19-')):
            df = pd.read_csv(file)
            
fetch_subsequent_data('Rush County', '2020-07-07')

county_repeat = []
for i in range(len(list_of_counties)):
    for _ in range(len(list(dict.fromkeys(list_of_dates)))):
        county_repeat.append(list_of_counties[i])

cols = {'Counties': county_repeat,
        'Dates': list_of_dates
        }

#create initial dataset with two columns — one with county names repeated, and one with series of dates per every county, i.e. 92 counties * number of days since 02/26/2020 (I think?)
indiana_master_df = pd.DataFrame(cols, columns = ['Counties', 'Dates'])

#append columns to the right. Each column is a date.
indiana_master_df = pd.concat([indiana_master_df,pd.DataFrame(columns=list(dict.fromkeys(list_of_dates)))],sort=False)

#set up diagonal data, i.e. original data, i.e. as first reported
for i in range(len(indiana_master_df)):
    county = indiana_master_df.iloc[i][0]
    indiana_master_df.loc[i][i%len(list(dict.fromkeys(list_of_dates)))+2] = fetch_date_of(county, indiana_master_df.Dates[i])
    