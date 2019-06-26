#Please note that, this code will create data files in excel that can be passed to any puller for pulling data into IFs. The first chunk will
# will install all the necessary packages for the user. The rest of the chunks will update each data file. It is recommended that the user run these codes as individual chunks.
# To run an individual chunk please select the text in the chunk and right click to select 'Execute Selection in Console'


#1: Select and run- This code will make the neccessary installations

import DataforIFsFirstTimeInstallations
DataforIFsFirstTimeInstallations.installAll()

#2: Select and run- This code will run all scripts to save data fo individual files
#a: WDI Indicators Data file
import DataUpdate
DataUpdate.WDIDataFile()

#b.UNESCO Education Data
import DataUpdate
DataUpdate.UISDataFile()

#c.FAO data excluding fish
import DataUpdate
DataUpdate.FAOFBSFile()

#d.FAO Fish Data

#e. IMF GFS Revenue Data
import DataUpdate
DataUpdate.IMFGFSRevenueDataFile()

#f. IMF GFS Expenditure Data
import DataUpdate
DataUpdate.IMFGFSExpenditureDataFile()

#g. IHME Historical Deaths File
import DataUpdate
DataUpdate.IHMEHistoricalDeathsFile()

#h. IHME DetailedDeaths file (used in initialization)
import DataUpdate
DataUpdate.IHMEDetailedDeathsFile()

#i. AQUASTAT Data
import DataUpdate
DataUpdate.AQUASTATDataFile()

#j. EIA Data

