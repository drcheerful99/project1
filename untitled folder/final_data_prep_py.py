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
#reduce variance by using rolling mean for traveler numbers
tsa['Average 2019'] = tsa.iloc[:,2].rolling(window=7).mean()
tsa['Average 2020'] = tsa.iloc[:,1].rolling(window=7).mean()

#prepare tsa file for merge with NYT data
passenger_numbers_2020 = tsa.loc[:,["Date","Average 2020"]]

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
#reduce variance by using rolling mean for rate of change numbers
nyt_nationwide['Averaged Rate of Change'] = nyt_nationwide.iloc[:,3].rolling(window=7).mean()

#create a merged dataframe of tsa data and nationwide nyt data
combined_total_data = pd.merge(numbered_tsa,nyt_nationwide, how="left",on="Date")
correlation_total_data = combined_total_data.loc[:,["Average 2020","Averaged Rate of Change"]]
#prepare nyt case data to be compared at different time points against travel numbers to explore if the relationship between the two has lag
for i in range(-30,31):
    correlation_total_data[i]=correlation_total_data["Averaged Rate of Change"].shift(i)

#make individual state dataframes for Georgia, California, Massachusetts, and Texas
#also create Case rate of change columns for all states
California = curated_nyt.loc[curated_nyt["state"]=="California"]
California["Case Rate of Change"]= California["cases"].diff()
California['Averaged Rate of Change'] = California.iloc[:,4].rolling(window=7).mean()
Georgia = curated_nyt.loc[curated_nyt["state"]=="Georgia"]
Georgia["Case Rate of Change"]= Georgia["cases"].diff()
Georgia['Averaged Rate of Change'] = Georgia.iloc[:,4].rolling(window=7).mean()
Massachusetts = curated_nyt.loc[curated_nyt["state"]=="Massachusetts"]
Massachusetts["Case Rate of Change"]= Massachusetts["cases"].diff()
Massachusetts['Averaged Rate of Change'] = Massachusetts.iloc[:,4].rolling(window=7).mean()
Texas = curated_nyt.loc[curated_nyt["state"]=="Texas"]
Texas["Case Rate of Change"]= Texas["cases"].diff()
Texas['Averaged Rate of Change'] = Texas.iloc[:,4].rolling(window=7).mean()

#rate of change in case numbers in states is not a good measurement as there are different population sizes and densities
#balance this out with daily case rate of change and total cases proportionate to the population
TX_population = 29087070
CA_population = 39747267
MA_population = 6976600
GA_population = 10736100
Texas["Case Percent"] = Texas["cases"].div(TX_population)
Texas["Case Percent"] = round(Texas["Case Percent"].multiply(100),2)
Texas["Percent Rate of Change"] = Texas["Averaged Rate of Change"].div(TX_population)
Texas["Percent Rate of Change"] = round(Texas["Percent Rate of Change"].multiply(100),4)
California["Case Percent"] = California["cases"].div(CA_population)
California["Case Percent"] = round(California["Case Percent"].multiply(100),2)
California["Percent Rate of Change"] = California["Averaged Rate of Change"].div(CA_population)
California["Percent Rate of Change"] = round(California["Percent Rate of Change"].multiply(100),4)
Massachusetts["Case Percent"] = Massachusetts["cases"].div(MA_population)
Massachusetts["Case Percent"] = round(Massachusetts["Case Percent"].multiply(100),2)
Massachusetts["Percent Rate of Change"] = Massachusetts["Averaged Rate of Change"].div(MA_population)
Massachusetts["Percent Rate of Change"] = round(Massachusetts["Percent Rate of Change"].multiply(100),4)
Georgia["Case Percent"] = Georgia["cases"].div(GA_population)
Georgia["Case Percent"] = round(Georgia["Case Percent"].multiply(100),2)
Georgia["Percent Rate of Change"] = Georgia["Averaged Rate of Change"].div(GA_population)
Georgia["Percent Rate of Change"] = round(Georgia["Percent Rate of Change"].multiply(100),4)

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
LAX['Averaged POB'] = LAX.iloc[:,3].rolling(window=7).mean()
#create rest of airport dfs
SFO = curated_airport_data.loc[curated_airport_data["AirportName"]=="San Francisco International"]
SFO['Averaged POB'] = SFO.iloc[:,3].rolling(window=7).mean()
BOS =  curated_airport_data.loc[curated_airport_data["State"]=="Massachusetts"]
BOS['Averaged POB'] = BOS.iloc[:,3].rolling(window=7).mean()
ATL =  curated_airport_data.loc[curated_airport_data["State"]=="Georgia"]
ATL['Averaged POB'] = ATL.iloc[:,3].rolling(window=7).mean()
DFW =  curated_airport_data.loc[curated_airport_data["State"]=="Texas"]
DFW['Averaged POB'] = DFW.iloc[:,3].rolling(window=7).mean()

#test with 1 airport and city for combined data set
LAXCA = pd.merge(California,LAX,how="inner",on="Date")
#finish rest of airport city merges
SFOCA = pd.merge(California,SFO,how="inner",on="Date")
BOSMA = pd.merge(Massachusetts,BOS,how="inner",on="Date")
ATLGA = pd.merge(Georgia,ATL,how="inner",on="Date")
DFWTX = pd.merge(Texas,DFW,how="inner",on="Date")