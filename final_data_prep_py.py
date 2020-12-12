#import dependencies
import pandas as pd
from datetime import datetime
import datetime as dt

#call up tsa data
tsa_data="Resources/tsa_data.csv"
tsa = pd.read_csv(tsa_data)
#change date formatting for consistency
tsa['Date']=pd.to_datetime(tsa['Date'].astype(str), format='%m/%d/%Y')
#changed numbers into floats
tsa["Total Traveler Throughput 2020"] = tsa["Total Traveler Throughput 2020"].str.replace(',','')
tsa["Total Traveler Throughput 2020"] = tsa["Total Traveler Throughput 2020"].astype(float)
tsa["Total Traveler Throughput 2019"] = tsa["Total Traveler Throughput 2019"].str.replace(',','')
tsa["Total Traveler Throughput 2019"] = tsa["Total Traveler Throughput 2019"].astype(float)

#prepare tsa file for merge with NYT data
passenger_numbers_2020 = tsa.loc[:,["Date","Total Traveler Throughput 2020"]]

#change date to number string
numbered_tsa = passenger_numbers_2020
numbered_tsa["Date"]=numbered_tsa["Date"].astype(str)
numbered_tsa['Date']=numbered_tsa['Date'].str.replace('-','')
numbered_tsa['Date']=numbered_tsa['Date'].astype(float)
#limited dates to match with those from nyt dataset
numbered_tsa = numbered_tsa.loc[numbered_tsa["Date"]<20200729]

#load and setup NYT data into dataframe
nyt_data="Resources/covid_19_state_level_data.csv"
nyt = pd.read_csv(nyt_data)

#edit nyt data to only include relevant info
curated_nyt = nyt.loc[:,["date","state","cases"]]
curated_nyt = curated_nyt.rename(columns={"date":"Date"})


numbered_nyt = curated_nyt
#create new column for date and change it into date formatting
numbered_nyt["DateFormat"]=numbered_nyt["Date"]
numbered_nyt["DateFormat"]=numbered_nyt["DateFormat"].apply(lambda x:
                                             dt.datetime.strptime(x,'%Y-%m-%d'))
#change date into float as well
numbered_nyt['Date']=numbered_nyt['Date'].str.replace('-','')
numbered_nyt['Date']=numbered_nyt['Date'].astype(float)

#set up nyt dates to match tsa dates
tsa_numbered_nyt = numbered_nyt.loc[numbered_nyt["Date"]>20200230]

#group by state to get total case number for comparison and make new column of sums
tsa_numbered_nyt["Total Cases"] = tsa_numbered_nyt.groupby(["Date"])["cases"].transform("sum")
#make a new dataframe of just date and total cases and drop duplicates
nyt_nationwide = tsa_numbered_nyt.loc[:,["Date","DateFormat","Total Cases"]]
nyt_nationwide = nyt_nationwide.drop_duplicates()
#Make a new column of rate of change in total cases using .diff
nyt_nationwide["Case Rate of Change"]= nyt_nationwide["Total Cases"].diff()

#create a merged dataframe of tsa data and nationwide nyt data
combined_total_data = pd.merge(numbered_tsa,nyt_nationwide, how="left",on="Date")
#prepare nyt case data to be compared at different time points against travel numbers to explore if the relationship between the two has lag
for i in [-15,-14,-13,-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2,-1,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]:
    combined_total_data[i]=combined_total_data["Total Cases"].shift(i)

#make individual state dataframes for Georgia, California, Massachusetts, and Texas
#also create Case rate of change columns for all states
California = curated_nyt.loc[curated_nyt["state"]=="California"]
California["Case Rate of Change"]= California["cases"].diff()
Georgia = curated_nyt.loc[curated_nyt["state"]=="Georgia"]
Georgia["Case Rate of Change"]= Georgia["cases"].diff()
Massachusetts = curated_nyt.loc[curated_nyt["state"]=="Massachusetts"]
Massachusetts["Case Rate of Change"]= Massachusetts["cases"].diff()
Texas = curated_nyt.loc[curated_nyt["state"]=="Texas"]
Texas["Case Rate of Change"]= Texas["cases"].diff()

#load in airport data
airport_data = "Resources/covid_impact_on_airport_traffic.csv"
airport = pd.read_csv(airport_data)

#remove not relevant columns from airport data
us_airport_data = airport[airport["Country"]=="United States of America (the)"]

#continue removing not relevant columns
curated_airport_data = us_airport_data.loc[:,["Date","AirportName","State","PercentOfBaseline"]]

#create new column for date with datetime format
curated_airport_data['DateFormat']=curated_airport_data['Date']
curated_airport_data["DateFormat"]=curated_airport_data["DateFormat"].apply(lambda x:
                                             dt.datetime.strptime(x,'%Y-%m-%d'))
#set date column as number
curated_airport_data['Date']=curated_airport_data['Date'].str.replace('-','')
curated_airport_data['Date']=curated_airport_data['Date'].astype(float)

#sort by date 
curated_airport_data=curated_airport_data.sort_values(by=["Date"])

#test airport df
LAX = curated_airport_data.loc[curated_airport_data["AirportName"]=="Los Angeles International"]
#create rest of airport dfs
SFO = curated_airport_data.loc[curated_airport_data["AirportName"]=="San Francisco International"]
BOS =  curated_airport_data.loc[curated_airport_data["State"]=="Massachusetts"]
ATL =  curated_airport_data.loc[curated_airport_data["State"]=="Georgia"]
DFW =  curated_airport_data.loc[curated_airport_data["State"]=="Texas"]

#test with 1 airport and city for combined data set
LAXCA = pd.merge(California,LAX,how="inner",on="Date")
#finish rest of airport city merges
SFOCA = pd.merge(California,SFO,how="inner",on="Date")
BOSMA = pd.merge(Massachusetts,BOS,how="inner",on="Date")
ATLGA = pd.merge(Georgia,ATL,how="inner",on="Date")
DFWTX = pd.merge(Texas,DFW,how="inner",on="Date")