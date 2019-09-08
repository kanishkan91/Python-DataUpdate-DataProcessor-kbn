# DataUpdate

Author- Kanishka Narayan (kanishkan91@gmail.com)

Description-
The python module can be used to scrape data and process data from different sources. The python module can output data as either as a
dataframe in the country year format or it will output data in excel files
This module has primarily been created for processing data for the International Futures (IFs) Project however, it can be used to process
data in general. The module can be used to process data from the following sources,
1) World Bank World Development Indicators (WDI)
2) UNESCO Education indicators(UIS)
3) FAO Food Balance Sheets (FAO)
4) IMF Global Finance Statistics (IMF GFS)
5) Health data from the Institute for Health and Metric Evaluation (IHME)
6) Water data from FAO AQUASTAT
7) Energy data from EIA

Currently this module can be run as is on Windows. For usage on Macs, the user may have to make changes to the code lines which 
specify paths.

Instructions for users new to Python:
1) Download and install the latest version of Pycharm for your computer here- https://www.jetbrains.com/pycharm/
2) Download and install Python version 3.7 from here- https://www.python.org/downloads/

Instructions on general use:
1) First download the zip file Pythonfiles.zip from this source below- 
https://drive.google.com/file/d/1aD2Zi_CEsunQbJBkhbDvhwNX82EaGCU2/view?usp=sharing
2) Place the zip file as is in the 'input/' folder in the same folder as your DataUpdate.py file
3) Create a new project in your Pycharm and copy the two Python files DataUpdate.py and DataforIFsFirstTimeInstallations.py to this 
 project. (You have the option to copy the third file DataforIfs.py as well. 
 4) I have added a third python file called DataForIFs.py which can be used to run all the commands necessary to process data. However,
 using this file is optional.
 5) First to set up run the below code,
 
import DataforIFsFirstTimeInstallations
DataforIFsFirstTimeInstallations.InstallAll()

This will install all the modules required by the DataUpdate module.Please note that you need to run the installation commands only once.

 6) Now you are set up to run the DataUpdate module
 7) As mentioned above, the DataUpdate module can output direct dataframes. For example, to output data from the FAO Food Balance Sheets,
 and save to a dataframe "AgData", run the code below,
 
import DataUpdate
AgData= DataUpdate.FAOFBS()

 8) Similarly, to save an excel file of the FAOFBS data, run the below,
 
import DataUpdate
DataUpdate.FAOFBSFile()

 9) The users will see that the base data for many of these pulls is located in the PythonFiles folder under the path 
  "C:\Users\Public\Pythonfiles"
 10) Currently, only the WDI data pull and the UIS data pull scrape data using APIs. For the rest, just update the Base data in the Python
 files folder.
 11) Similarly, users can make changes to individual concordance tables as well
 


