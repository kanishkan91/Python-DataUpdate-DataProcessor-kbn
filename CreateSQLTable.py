import DataUpdate
import sqlite3


connection=sqlite3.connect("output/MasterDataBase.db")
cursor=connection.cursor()

IMFExp=DataUpdate.IMFGFSExpenditureData()
IMFExp.to_sql(name="IMFExpenditure", con=connection, if_exists="replace", index=False)

Aquastat=DataUpdate.AQUASTATData()
Aquastat.to_sql(name="AQUASTAT", con=connection, if_exists="replace", index=False)

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



