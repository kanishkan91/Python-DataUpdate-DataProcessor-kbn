import DataUpdate
import sqlite3
import time

#Generate database
connection=sqlite3.connect("output/Database/IFsHistSeries.db")
print('Database created at output/Database/IFsHistSeries.db')
cursor=connection.cursor()

#AQUASTAT Data
print('Starting AQUASTAT data')
start=time.time()
Aquastat=DataUpdate.AQUASTATData(write_csv_output=True)
DataUpdate.Write_to_SQL(Aquastat,connection)
end=time.time()
print(end-start)


#IMFGFS Expenditure data
print('Starting IMF GFS expenditure data')
start=time.time()
IMFExp=DataUpdate.IMFGFSExpenditureData(write_csv_output=True)
DataUpdate.Write_to_SQL(IMFExp,connection)
end=time.time()
print(end-start)

#FAO Data
print('Starting FAO Food balance sheet data')
start=time.time()
FAO=DataUpdate.FAOFBS(write_csv_output=True)
DataUpdate.Write_to_SQL(FAO,connection)
end=time.time()
print(end-start)

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



