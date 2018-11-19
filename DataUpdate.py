
def ImportCommands():
    import numpy as np
    import requests
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import xlsxwriter
    import matplotlib.transforms as mtransforms
    import statsmodels.api as sm
    import glob
    import dask as dd


def IHMEDetailedDeathsData():
    import numpy as np
    import requests
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import xlsxwriter
    import matplotlib.transforms as mtransforms
    import statsmodels.api as sm
    import glob
    import dask as dd
    print(r"Please make sure the zip file Pythonfiles.zip is saved under C:\Users\Public\ and that you have run the installation code in case you are a first time user" )
    print(r"Reading files from C:\Users\Public\Pythonfiles")
    CountryConcord = pd.read_csv (r'C:\\Users\Public\Pythonfiles\CountryConcordIHME.csv')
    SeriesConcord = pd.read_csv (r'C:\\Users\Public\Pythonfiles\SeriesConcordIHME.csv')
    path = (r'C:\Users\Public\Pythonfiles\IHMEDownloads\DetailedDeathFileData')
    filenames = glob.glob (path + "/*.csv")
    GBDData = []
    for filename in filenames:
        filename = pd.read_csv (filename)
        filename = pd.merge (filename, CountryConcord, how='left', left_on='location_name',
                             right_on='Country name in IHME')
        filename = pd.merge (filename, SeriesConcord, how='left', left_on='cause_name', right_on='Series name in IHME')
        filename = filename.dropna (how='any')
        # GBDDalys.append(pd.read_csv(filename,low_memory=False))
        GBDData.append (filename)
    GBDData = pd.concat (GBDData, ignore_index=True)
    GBDDeaths = GBDData
    p = pd.pivot_table (GBDDeaths, index=["Country name in IFs", "sex_id", 'Series name in IFs', 'metric_name'],
                        values=['val'],
                        columns=["age_name"], aggfunc=[np.sum])

    return(p)

def IHMEDetailedDeathsFile():
    import numpy as np
    import requests
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import xlsxwriter
    import matplotlib.transforms as mtransforms
    import statsmodels.api as sm
    import glob
    import dask as dd

    p=IHMEDetailedDeathsData()
    writer = pd.ExcelWriter ('GBDDeathsFile.xlsx', engine='xlsxwriter')
    p.to_excel (writer, sheet_name='Deaths', merge_cells=False)

    writer.save ()
    print(r"File Saved Under C:\Users\Public\PythonScripts")


def IHMEHistoricalDeathData():
    import numpy as np
    import requests
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import xlsxwriter
    import matplotlib.transforms as mtransforms
    import statsmodels.api as sm
    import glob
    import dask as dd

    CountryConcord = pd.read_csv (r'C:\\Users\Public\Pythonfiles\CountryConcordIHME.csv')
    SeriesConcord = pd.read_csv (r'C:\\Users\Public\Pythonfiles\SeriesConcordIHME.csv')

    path = (r'C:\Users\Public\Pythonfiles\IHMEDownloads\HistDeathFileData')

    filenames = glob.glob (path + "/*.csv")
    print("Reading files from path"+str(path))


    GBDData = []
    for filename in filenames:
        filename = pd.read_csv (filename)
        filename = pd.merge (filename, CountryConcord, how='left', left_on='location_name',
                             right_on='Country name in IHME')
        filename = pd.merge (filename, SeriesConcord, how='left', left_on='cause_name', right_on='Series name in IHME')
        filename = filename.dropna (how='any')
        # GBDDalys.append(pd.read_csv(filename,low_memory=False))
        GBDData.append (filename)

    GBDData = pd.concat (GBDData, ignore_index=True)

    GBDDeaths = GBDData
    print (GBDDeaths.head ())
    p = pd.pivot_table (GBDDeaths, index=["Country name in IFs", 'year'], values=['val'],
                        columns=["Series name in IFs"], aggfunc=[np.sum])

    return(p)

def IHMEHistoricalDeathsFile():
    import numpy as np
    import requests
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import xlsxwriter
    import matplotlib.transforms as mtransforms
    import statsmodels.api as sm
    import glob
    import dask as dd

    p=IHMEHistoricalDeathData()
    writer = pd.ExcelWriter ('GBDHistDeathsFile.xlsx', engine='xlsxwriter')
    p.to_excel (writer, sheet_name='Deaths',merge_cells=False)
    writer.save ()

def FAOFBS():
    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import dask.dataframe as dd
    import statsmodels.api as sm

    data = dd.read_csv (r'C:\\Users\Public\Pythonfiles\FoodBalanceSheets_E_All_Data_(Normalized).csv',
                        encoding="ISO-8859-1")
    # data=pd.concat(tp,ignore_index=True)

    data['Code'] = data[str ('Element Code')] + data[str ('Item Code')]
    concord_table = dd.read_csv ('C:\\Users\Public\Pythonfiles\Aggregation for crop type.csv')

    data = dd.merge (data, concord_table, how="left", left_on="Item Code", right_on='Code no')

    data['Series_Name'] = data[str ('Code Name')] + data[str ('Element')]
    series_concord_table = dd.read_csv ('C:\\Users\Public\Pythonfiles\FAOSeriesConcordance.csv')

    data.columns = list (data.columns)
    data = data.drop (
        ['Area Code', 'Item Code', 'Flag', 'Unit', 'Year Code', 'Element', 'Element Code', 'Code', 'Code Name', 'Item',
         'Code no'], axis=1)
    print (data.head ())
    data = data.dropna (how='any')
    print (data.head ())

    data.reset_index ()

    datapanda = data.compute ()
    # data=pd.DataFrame(data)
    # p= datapanda.pivot_table(index=["Area",'Year'],values=['Value'],
    # columns=["Series Name in Ifs"],aggfunc=[np.sum])

    p = pd.pivot_table (datapanda, index=["Area", 'Year'], values=['Value'], columns=["Series_Name"], aggfunc=[np.sum])

    return(p)

def FAOFBSFile():
    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import dask.dataframe as dd
    import statsmodels.api as sm

    p= FAOFBS()
    writer = pd.ExcelWriter ('FAOFBS2019.xlsx', engine='xlsxwriter')
    p.to_excel (writer, sheet_name='FAOFBS')

    writer.save()

def FAOFBSFish():
    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import dask.dataframe as dd
    import statsmodels.api as sm

    data = dd.read_csv ('C:\\Users\Public\Pythonfiles\FoodBalanceSheets_E_All_Data_(Normalized).csv',
                        encoding="ISO-8859-1")
    # data=pd.concat(tp,ignore_index=True)

    data['Code'] = data[str ('Element Code')] + data[str ('Item Code')]
    concord_table = dd.read_csv ('C:\\Users\Public\Pythonfiles\AggregationforFish.csv')

    data = dd.merge (data, concord_table, how="left", left_on="Code", right_on='Code in Source')

    data = data.drop (
        ['Area Code', 'Item Code', 'Flag', 'Unit', 'Year Code', 'Element', 'Element Code', 'Code', 'Item'], axis=1)

    data = data.dropna (how='any')
    data.reset_index ()

    datapanda = data.compute ()
    print (datapanda.head ())
    p = pd.pivot_table (datapanda, index=["Area", 'Year'], values=['Value'], columns=["Variable"], aggfunc=[np.sum])

    return(p)

def FAOFBSFishFile():
    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import dask.dataframe as dd
    import statsmodels.api as sm

    p=FAOFBSFish()
    writer = pd.ExcelWriter ('FAOFBSFish.xlsx', engine='xlsxwriter')
    p.to_excel (writer, sheet_name='Fish', merge_cells=False)

    writer.save ()


def UISData():
    import numpy as np
    import requests
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import xlsxwriter
    import matplotlib.transforms as mtransforms
    import statsmodels.api as sm
    from splinter import Browser
    import glob
    import xml.etree.cElementTree as et
    from bs4 import BeautifulSoup
    from lxml import html

    print(r'Warning: The UIS Pull may take a long time to complete. Please do not close this session')
    dict = pd.read_excel (r'C:\\Users\Public\Pythonfiles\IndicatorNoUIS.xlsx', sheet_name='Master')
    Yeardict = pd.read_excel (r'C:\Users\Public\Pythonfiles\YearDictUIS.xlsx', sheet_name='Sheet1')
    SeriesCode = []
    Countryname = []
    Year = []
    Value = []

    print ("Start reading in URLs")
    for row in dict['Indicator no']:
        wiki1 = ('http://data.uis.unesco.org/RestSDMX/sdmx.ashx/GetData/EDULIT_DS/' + str (row))

        for row in Yeardict["Year"]:

            wiki = (str (wiki1) + '?startTime=' + str (row) + '&endTime=' + str (row))
            r = requests.get (wiki, stream=True)
            data = r.text
            soup = BeautifulSoup (data, "lxml")
            SeriesKey = soup.find_all ('value', concept='LOCATION')
            for element in SeriesKey:
                Country = element['value']
                Countryname.append (Country)
            Codename = soup.find_all ('value', concept='EDULIT_IND')
            for element in Codename:
                Code = element['value']
                SeriesCode.append (Code)
            Time = soup.find_all ('time')
            for element in Time:
                time = element.text
                Year.append (time)
            Val = soup.find_all ('obsvalue')
            for element in Val:
                Val1 = element['value']
                Value.append (Val1)

    print ('loop complete')
    print (len (SeriesCode))
    print (len (Countryname))
    print (len (Year))
    print (len (Value))

    test_df = pd.DataFrame.from_dict ({'SeriesName': SeriesCode,
                                       'Country': Countryname,
                                       'Year': Year,
                                       'Value': Value}, orient='index')
    df = test_df.transpose ()

    CountryconcordUIS = pd.read_excel (r'C:\\Users\Public\Pythonfiles\UISCountryConcord.xlsx', sheet_name='Sheet1')
    SeriesConcordUIS = pd.read_excel (r'C:\\Users\Public\Pythonfiles\UIS.xlsx', sheet_name='Sheet1')
    print (df.head ())

    df = pd.merge (df, CountryconcordUIS, left_on='Country', right_on='Country code name', how='left')
    df = pd.merge (df, SeriesConcordUIS, left_on='SeriesName', right_on='Indicator Number', how='left')

    print (df.head ())

    df = df[pd.notnull (df['Country name in IFs'])]
    df = df[pd.notnull (df['Series Name in IFs'])]

    p = pd.pivot_table (df, index=['Country name in IFs', 'Year'], columns=['Series Name in IFs'], values=['Value'],
                        aggfunc=[np.sum])


    return(p)

def UISDataFile():
    import pandas as pd
    p=UISData()

    writer = pd.ExcelWriter ('UISForPull.xlsx', engine='xlsxwriter')
    p.to_excel (writer, sheet_name='UIS', merge_cells=False)

    writer.save ()

def AQUASTATData():
    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import dask.dataframe as dd
    import statsmodels.api as sm

    data = pd.read_excel ('C:\\Users\Public\Pythonfiles\AQUASTAT.xlsx')

    country_concordance = pd.read_excel ('C:\\Users\Public\Pythonfiles\CountryConcordanceAQUASTAT.xlsx')

    data = pd.merge (data, country_concordance, how="left", left_on="Area", right_on="Area name")

    series_concordance = pd.read_excel ('C:\\Users\Public\Pythonfiles\SeriesConcordanceAQUASTAT.xlsx')

    # print (series_concordance.head())
    # print(data.head())

    data = pd.merge (data, series_concordance, how="left", left_on="Variable Name", right_on="Series name in Aquastat")
    # print(data.head())
    # print(data['Country Name in IFs'].unique)
    data = data.drop (['Variable Id', 'Area Id', 'Symbol', 'Md'], axis=1)

    data = data.dropna (how='any')
    p = pd.pivot_table (data, index=['Country Name in IFs', 'Year'], columns=['Series name in IFs'], values=['Value'],
                        aggfunc=[np.sum])

    return(p)

def AQUASTATDataFile():
    import pandas as pd

    p=AQUASTATData()
    p=p.reset_index()
    writer = pd.ExcelWriter ('AQUASTAT.xlsx', engine='xlsxwriter')
    p.to_excel (writer, sheet_name='1', merge_cells=False
    writer.save ()

def IMFGFSRevenueData():
    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import statsmodels.api as sm
    import dask.dataframe as dd

    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import statsmodels.api as sm
    import dask.dataframe as dd

    data = dd.read_csv ('C:\\Users\Public\Pythonfiles\GFSRevenue.csv')

    data['FuncSector'] = data[str ('Sector Name')] + data[str ('Classification Name')]

    concord_table = pd.read_excel ('C:\\Users\Public\Pythonfiles\CountryConcordanceIMF.xlsx')

    data = data.merge (concord_table, on="Country Name", how='left')
    data = data.loc[data['Unit Name'] == 'Percent of GDP']
    print (data.head ())
    data = data.drop (
        ['Country Code', 'Country Name', 'Classification Code', 'Sector Code', 'Unit Code', 'Status', 'Valuation',
         'Bases of recording (Gross/Net)', 'Nature of data'], axis=1)

    data = data.reset_index ()
    data = data.compute ()

    data = data.reset_index ()

    p = pd.pivot_table (data, index=["Country name in IFs", "Unit Name", 'Time Period'], values=['Value'],
                        columns=['FuncSector'], aggfunc=[np.sum])

    return(p)

def IMFGFSRevenueDataFile():
    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import statsmodels.api as sm
    import dask.dataframe as dd

    p=IMFGFSRevenueData()
    writer = pd.ExcelWriter ('IMFRevenue.xlsx', engine='xlsxwriter')
    p.to_excel (writer, sheet_name='Revenue', merge_cells=False)

    writer.save ()

def IMFGFSExpenditureData():
    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import statsmodels.api as sm

    data = pd.read_csv ('C:\\Users\Public\Pythonfiles\GFSCOFOG_02-23-2018 23-30-06-28.csv')

    data['FuncSector'] = data[str ('COFOG Function Name')] + data[str ('Sector Name')]
    print (data.head ())

    concord_table = pd.read_excel ('C:\\Users\Public\Pythonfiles\CountryConcordanceIMF.xlsx')
    print (concord_table.head ())

    data = data.merge (concord_table, on="Country Name", how='left')
    print (data.head ())

    p = pd.pivot_table (data, index=["Country name in IFs", "Unit Name", 'Time Period'], values=['Value'],
                        columns=['FuncSector'], aggfunc=[np.sum])

    return(p)

def IMFGFSExpenditureDataFile():
    import requests
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import matplotlib.transforms as mtransforms
    import xlsxwriter
    import statsmodels.api as sm

    p=IMFGFSExpenditureData()
    writer = pd.ExcelWriter ('IMFGFSEXP.xls', engine='xlsxwriter')
    p.to_excel (writer, sheet_name='EXP')

    writer.save ()

def WDIData():
    import numpy as np
    import requests
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import xlsxwriter
    import matplotlib.transforms as mtransforms
    import statsmodels.api as sm
    #from urllib.request import urlopen
    from bs4 import BeautifulSoup
    from xml.dom import minidom
    import xml.etree.cElementTree as et
    import dask.dataframe as dd

    # Step 1: First read in the excel/csv with the relevant WDI codes and print out the WDI codes to be sure we have the right number of values
    datadict = pd.read_excel ('C:\\Users\Public\Pythonfiles\WDICodes.xlsx', sheet_name='Sheet1',
                              dtype='str')  # Replace the location of code files for your desktop
    print ('Hey User!, you are importing '+(str(len(datadict['WDI Code']))+' series. To import additional series please update the file WDICodes.xlsx located in the Pythonfiles folder'))

    # print(datadict['WDI Code'])

    print ('Step 1 Complete: All files read in')

    # Step 2: Prepare columns for series name,country,year,value
    Seriesname = []
    Countryname = []
    SeriesCode = []
    Year = []
    Value = []

    print (
        'We will be starting the loop for all codes now.')
    # Step 3: Create a loop for reading in all the codes into the url and parsing the xml file for relevant values.This helps in pulling in 500 series at a time
    for row in datadict['WDI Code']:
        wiki = 'http://api.worldbank.org/v2/countries/all/indicators/' + str (row) + '/?format=xml&per_page=20000'
        r = requests.get (wiki, stream=True)
        root = et.fromstring (r.content)

        for child in root.iter ("{http://www.worldbank.org}indicator"):
            SeriesCode.append (child.attrib['id'])
        for child in root.iter ("{http://www.worldbank.org}country"):
            Countryname.append (child.text)
        for child in root.iter ("{http://www.worldbank.org}date"):
            Year.append (child.text)
        for child in root.iter ("{http://www.worldbank.org}value"):
            Value.append ((child.text))
    print ('Loop is complete.Hard part is over now!')

    # Step 4: Write all the parsed values to the dataframe in Step 2
    test_df = pd.DataFrame.from_dict ({'SeriesName': SeriesCode,
                                       'Country': Countryname,
                                       'Year': Year,
                                       'Value': Value}, orient='index')
    print ('Step 4 complete! You have a data frame now!')
    # Step 5: Read in concordance tables for Countries and series
    countryconcord = pd.read_csv ('C:\\Users\Public\Pythonfiles\CountryConcordanceWDI.csv', encoding="ISO-8859-1")
    seriesconcord = pd.read_csv ('C:\\Users\Public\Pythonfiles\CodeConcordanceWDI.csv', encoding="ISO-8859-1")

    print ('Step 5 complete! We have read in the concordance tables')
    # Step 6: Create a transponsed file using the dataframe
    df = test_df.transpose ()

    print ('Step 6 complete! We have a transposed dataset!')

    # Step 7:Concord country and series names
    df = pd.merge (df, countryconcord, how='left', left_on='Country', right_on='Country')
    df = pd.merge (df, seriesconcord, how='left', left_on='SeriesName', right_on='CodeinIfs')

    print ('Step 7 complete! We have a merged dataset now!')

    # Step 8: Drop null values to keep data managable
    df = df[pd.notnull (df['Value'])]
    df = df[pd.notnull (df['Country name in IFs'])]
    df = df[pd.notnull (df['Series name in IFs'])]

    print ('We have dropped all null values!')
    # Step 9: Write to a pivot table
    data = pd.pivot_table (df, index=['Country name in IFs', 'CodeinIfs'], columns=['Year'], values=['Value'],
                           aggfunc=[np.sum])

    return(data)

def WDIDataFile():
    import numpy as np
    import requests
    import matplotlib.pyplot as plt
    import pandas as pd
    import csv
    import xlrd
    import matplotlib.lines as mlines
    import xlsxwriter
    import matplotlib.transforms as mtransforms
    import statsmodels.api as sm
    #from urllib.request import urlopen
    from bs4 import BeautifulSoup
    from xml.dom import minidom
    import xml.etree.cElementTree as et
    import dask.dataframe as dd

    data=WDIData()
    writer = pd.ExcelWriter ('WDISeries.xlsx', engine='xlsxwriter')
    data.to_excel (writer, sheet_name='WDIData', merge_cells=False)

    writer.save ()

