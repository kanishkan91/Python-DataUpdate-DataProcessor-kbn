import DataUpdate
import sqlite3
import pandas as pd
import numpy as np

connection=sqlite3.connect("output/IFsHistSeries.db")
cursor=connection.cursor()

Aquastat=DataUpdate.AQUASTATData()
Aquastat2=Aquastat.drop(["Country Name in IFs","Year"],axis=1)


for col in (Aquastat2):
    data=Aquastat[["Country Name in IFs","Year",str(col)]]
    data1=pd.pivot_table(data,values=[str(col)],index=["Country Name in IFs"], columns=["Year"],aggfunc=[np.sum])

    #Get Most Recent Year
    dataRecent=data.dropna()
    dataRecent=dataRecent.sort_values(["Country Name in IFs","Year"])
    dataRecent=dataRecent.drop_duplicates("Country Name in IFs",keep='last')
    dataRecent.reset_index()
    dataRecent.columns=["Country Name in IFs","MostRecentYear","MostRecentValue"]

    #Get Earliest Year
    dataEarliest = data.dropna()
    dataEarliest= dataEarliest.sort_values(["Country Name in IFs", "Year"])
    dataEarliest = dataEarliest.drop_duplicates("Country Name in IFs", keep='first')
    dataEarliest.reset_index()
    dataEarliest.columns = ["Country Name in IFs", "EarliestYear", "EarliestValue"]

    data1 = pd.DataFrame(data1.to_records())
    val="('sum', '"+str(col)+"', "
    data1.columns = [hdr.replace(val,"").replace(")", "").replace("'", "") \
                    for hdr in data1.columns]

    data1=pd.merge(data1,dataRecent,how='left',left_on="Country Name in IFs",right_on="Country Name in IFs")
    data1=pd.merge(data1,dataEarliest,how='left',left_on="Country Name in IFs",right_on="Country Name in IFs")
    print(data1.head())

    data1.to_sql(name=str("Series").strip()+str(col).strip(), con=connection, if_exists="replace", index=False)




IMFExp=DataUpdate.IMFGFSExpenditureData()
IMFExp.to_sql(name="IMFExpenditure", con=connection, if_exists="replace", index=False)


FAO=DataUpdate.FAOFBS()
FAO.to_sql(name="FAOFoodBalanceSheetAggregated", con=connection, if_exists="replace", index=False)

WDI=DataUpdate.WDIData()
WDI.to_sql(name="WorldDevelopmentIndicators",con=connection,if_exists="replace",index=False)

UIS=DataUpdate.UISData()
UIS.to_sql(name="UISEducationData",con=connection,if_exists="replace",index=False)


IMF=DataUpdate.IMFGFSRevenueData()
IMF.to_sql(name="IMFRevenue", con=connection, if_exists="replace", index=False)

HealthDet=DataUpdate.IHMEDetailedDeathsData()
HealthDet.to_sql(name="HealthDataDetailed",con=connection,if_exists="replace",index=False)

Health=DataUpdate.IHMEHistoricalDeathData(sex_name="Both")
Health.to_sql(name="HealthData",con=connection,if_exists="replace",index=False)

Fish=DataUpdate.FAOFBSFish()
Fish.to_sql(name="FAOFishData", con=connection, if_exists="replace", index=False)



