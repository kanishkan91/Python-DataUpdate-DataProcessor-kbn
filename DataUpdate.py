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
    print(
        r"Please make sure the zip file Pythonfiles.zip is saved under input and that you have run the installation code in case you are a first time user")
    print(r"Reading files from input")
    CountryConcord = pd.read_csv(r'input\CountryConcordIHME.csv')
    SeriesConcord = pd.read_excel(r'input\SeriesConcordIHME.xlsx')
    path = (r'input\\IHMEDownloads\DetailedDeathFileData')
    filenames = glob.glob(path + "/*.csv")
    GBDData = []
    for filename in filenames:
        filename = pd.read_csv(filename)
        filename = pd.merge(filename, CountryConcord, how='left', left_on='location_name',
                            right_on='Country name in IHME')
        filename = pd.merge(filename, SeriesConcord, how='left', left_on='cause_name', right_on='Series name in IHME')
        filename = filename.dropna(how='any')
        # GBDDalys.append(pd.read_csv(filename,low_memory=False))
        GBDData.append(filename)
    GBDData = pd.concat(GBDData, ignore_index=True)
    GBDDeaths = GBDData
    data = pd.pivot_table(GBDDeaths, index=["Country name in IFs", "sex_id", 'Series name in IFs', 'metric_name'],
                       values=['val'],
                       columns=["age_name"], aggfunc=[np.sum])
    data = pd.DataFrame(data.to_records())
    data.columns = [hdr.replace("('sum', 'val',", "").replace(")", "").replace("'", "") \
                    for hdr in data.columns]
    return (data)


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

    p = IHMEDetailedDeathsData()
    writer = pd.ExcelWriter('GBDDeathsFile.xlsx', engine='xlsxwriter')
    p.to_excel(writer, sheet_name='Deaths', merge_cells=False)

    writer.save()
    print(r"File Saved")


def IHMEHistoricalDeathData(sex_name):
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

    CountryConcord = pd.read_csv(r'input\CountryConcordIHME.csv',encoding="ISO-8859-1")
    SeriesConcord = pd.read_excel(r'input\SeriesConcordIHME.xlsx')

    path = (r'input\\IHMEDownloads\HistDeathFileData')

    filenames = glob.glob(path + "/*.csv")
    print("Reading files from path" + str(path))

    GBDData = []
    for filename in filenames:
        filename = pd.read_csv(filename)
        filename = pd.merge(filename, CountryConcord, how='left', left_on='location_name',
                            right_on='Country name in IHME')

        filename = pd.merge(filename, SeriesConcord, how='left', left_on='cause_name', right_on='Series name in IHME')
        filename = filename.dropna(how='any')
        filename = filename.loc[filename['sex_name'] == sex_name]
        GBDData.append(filename)



    GBDData = pd.concat(GBDData, ignore_index=True)


    GBDDeaths = GBDData
    print(GBDDeaths.head())
    data = pd.pivot_table(GBDDeaths, index=["Country name in IFs", 'year'], values=['val'],
                       columns=["Series name in IFs"], aggfunc=[np.sum])

    data = pd.DataFrame(data.to_records())
    data.columns = [hdr.replace("('sum', 'val',", "").replace(")", "").replace("'", "") \
                    for hdr in data.columns]
    return (data)


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

    p = IHMEHistoricalDeathData()
    writer = pd.ExcelWriter('GBDHistDeathsFile.xlsx', engine='xlsxwriter')
    p.to_excel(writer, sheet_name='Deaths', merge_cells=False)
    writer.save()


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

    Country_Concord = pd.read_csv('input\CountryConcordFAO.csv', encoding="ISO-8859-1")
    concord_table = pd.read_csv('input\Aggregation for crop type.csv')
    series_concord_table = pd.read_csv('input\FAOSeriesConcordance.csv')
    data = pd.read_csv(r'input\FoodBalanceSheets_E_All_Data_(Normalized).csv',
                       encoding="ISO-8859-1",chunksize=100000)

    chunk_list=[]
    for chunk in data:
        chunk['Code'] = chunk[str('Element Code')] + chunk[str('Item Code')]
        chunk = pd.merge(chunk, concord_table, how="left", left_on="Item Code", right_on='Code no')
        chunk['Series_Name'] = chunk[str('Code Name')] + chunk[str('Element')]
        chunk = pd.merge(chunk, series_concord_table, how="left", left_on="Series_Name", right_on="Code in file")
        chunk = pd.merge(chunk, Country_Concord, how="left", left_on="Area", right_on='Area Name')
        chunk = chunk.dropna(how='any')
        chunk = chunk.dropna(how='any')
        chunk_list.append(chunk)

    data=pd.concat(chunk_list)

    data.drop(
        ['Area Code', 'Item Code', 'Flag', 'Unit', 'Year Code', 'Element', 'Element Code', 'Code', 'Code Name', 'Item',
         'Code no'], axis=1)

    data = pd.pivot_table(data, index=["Area", 'Year'], values=['Value'], columns=["Series_Name"], aggfunc=[np.sum])
    data = pd.DataFrame(data.to_records())
    data.columns = [hdr.replace("('sum', 'Value',", "").replace(")", "").replace("'", "") \
                    for hdr in data.columns]
    return (data)


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

    p = FAOFBS()
    writer = pd.ExcelWriter('FAOFBS2019.xlsx', engine='xlsxwriter')
    p.to_excel(writer, sheet_name='FAOFBS')

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

    Country_Concord = pd.read_csv('input\CountryConcordFAO.csv', encoding="ISO-8859-1")
    data = pd.read_csv('input\FoodBalanceSheets_E_All_Data_(Normalized).csv',
                       encoding="ISO-8859-1",chunksize=100000)
    concord_table = pd.read_csv('input\AggregationforFish.csv')
    chunk_list = []

    for chunk in data:
        chunk['Code'] = chunk[str('Element Code')] + chunk[str('Item Code')]
        chunk= pd.merge(chunk, concord_table, how="left", left_on="Code", right_on='Code in Source')
        chunk = pd.merge(chunk, Country_Concord, how="left", left_on="Area", right_on='Area Name')
        chunk = chunk.dropna(how='any')
        chunk_list.append(chunk)

    data=pd.concat(chunk_list)

    data = data.drop(
        ['Area Code', 'Item Code', 'Flag', 'Unit', 'Year Code', 'Element', 'Element Code', 'Code', 'Item'], axis=1)

    #data = data.dropna(how='any')
    #print(data.Country.unique())

    #print("Dropped irrelevant columns, Na")
    #data.reset_index()

    #datapanda = data.groupby(["Area","Year","Variable"]).sum().compute()
    #print(datapanda.head())

    data = pd.pivot_table(data, index=["Country name in IFs", 'Year'], values=['Value'], columns=["Variable"], aggfunc=[np.sum])
    data = pd.DataFrame(data.to_records())
    data.columns = [hdr.replace("('sum', 'Value',", "").replace(")", "").replace("'", "") \
                    for hdr in data.columns]
    return (data)


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

    p = FAOFBSFish()
    writer = pd.ExcelWriter('FAOFBSFish.xlsx', engine='xlsxwriter')
    p.to_excel(writer, sheet_name='Fish', merge_cells=False)

    writer.save()


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
    dict = pd.read_excel(r'input\IndicatorNoUIS.xlsx', sheet_name='Master')
    Yeardict = pd.read_excel(r'input\\YearDictUIS.xlsx', sheet_name='Sheet1')
    SeriesCode = []
    Countryname = []
    Year = []
    Value = []

    print("Start reading in URLs")
    for row in dict['Indicator no']:
        wiki1 = ('http://data.uis.unesco.org/RestSDMX/sdmx.ashx/GetData/EDULIT_DS/' + str(row))

        for row in Yeardict["Year"]:

            wiki = (str(wiki1) + '?startTime=' + str(row) + '&endTime=' + str(row))
            r = requests.get(wiki, stream=True)
            data = r.text
            soup = BeautifulSoup(data, "lxml")
            SeriesKey = soup.find_all('value', concept='LOCATION')
            for element in SeriesKey:
                Country = element['value']
                Countryname.append(Country)
            Codename = soup.find_all('value', concept='EDULIT_IND')
            for element in Codename:
                Code = element['value']
                SeriesCode.append(Code)
            Time = soup.find_all('time')
            for element in Time:
                time = element.text
                Year.append(time)
            Val = soup.find_all('obsvalue')
            for element in Val:
                Val1 = element['value']
                Value.append(Val1)

    print('loop complete')


    test_df = pd.DataFrame.from_dict({'SeriesName': SeriesCode,
                                      'Country': Countryname,
                                      'Year': Year,
                                      'Value': Value}, orient='index')
    df = test_df.transpose()

    CountryconcordUIS = pd.read_excel(r'input\UISCountryConcord.xlsx', sheet_name='Sheet1')
    SeriesConcordUIS = pd.read_excel(r'input\UIS.xlsx', sheet_name='Sheet1')


    df = pd.merge(df, CountryconcordUIS, left_on='Country', right_on='Country code name', how='left')
    df = pd.merge(df, SeriesConcordUIS, left_on='SeriesName', right_on='Indicator Number', how='left')



    df = df[pd.notnull(df['Country name in IFs'])]
    df = df[pd.notnull(df['Series Name in IFs'])]

    data = pd.pivot_table(df, index=['Country name in IFs', 'Year'], columns=['Series Name in IFs'], values=['Value'],
                       aggfunc=[np.sum])
    data = pd.DataFrame(data.to_records())
    data.columns = [hdr.replace("('sum', 'Value',", "").replace(")", "").replace("'", "") \
                    for hdr in data.columns]
    return (data)


def UISDataFile():
    import pandas as pd
    p = UISData()

    writer = pd.ExcelWriter('UISForPull.xlsx', engine='xlsxwriter')
    p.to_excel(writer, sheet_name='UIS', merge_cells=False)

    writer.save()


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

    data = pd.read_excel('input\AQUASTAT.xlsx')

    country_concordance = pd.read_excel('input\CountryConcordanceAQUASTAT.xlsx')

    data = pd.merge(data, country_concordance, how="left", left_on="Area", right_on="Area name")

    series_concordance = pd.read_excel('input\SeriesConcordanceAQUASTAT.xlsx')

    # print (series_concordance.head())
    # print(data.head())

    data = pd.merge(data, series_concordance, how="left", left_on="Variable Name", right_on="Series name in Aquastat")
    # print(data.head())
    # print(data['Country Name in IFs'].unique)
    data = data.drop(['Variable Id', 'Area Id', 'Symbol', 'Md'], axis=1)

    data = data.dropna(how='any')
    data = pd.pivot_table(data, index=['Country Name in IFs', 'Year'], columns=['Series name in IFs'], values=['Value'],
                       aggfunc=[np.sum])
    data = pd.DataFrame(data.to_records())
    data.columns = [hdr.replace("('sum', 'Value',", "").replace(")", "").replace("'", "") \
                    for hdr in data.columns]
    return (data)


def AQUASTATDataFile():
    import pandas as pd
    p = AQUASTATData()
    p = p.reset_index()
    writer = pd.ExcelWriter('AQUASTAT.xlsx', engine='xlsxwriter')
    p.to_excel(writer, sheet_name='1', merge_cells=False)
    writer.save()


def IMFGFSRevenueData():
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd
    concord_table = pd.read_excel('input\CountryConcordanceIMF.xlsx')
    data = pd.read_csv('input\GFSRevenue.csv',chunksize=100000)
    chunk_list=[]
    for chunk in data:
        chunk['FuncSector'] = chunk[str('Sector Name')] + chunk[str('Classification Name')]
        chunk = chunk.merge(concord_table, on="Country Name", how='left')
        chunk=chunk.rename(columns={"Time Period":"Year"})
        chunk = chunk.loc[chunk['Unit Name'] == 'Percent of GDP']
        chunk.dropna(how='any')

        chunk_list.append(chunk)
    data=pd.concat(chunk_list)

    data = data.drop(
        ['Country Code', 'Country Name', 'Classification Code', 'Sector Code', 'Unit Code', 'Status', 'Valuation',
         'Bases of recording (Gross/Net)', 'Nature of data'], axis=1)

    data = data.reset_index()

    #data = data.reset_index()

    data = pd.pivot_table(data, index=["Country name in IFs", "Unit Name", 'Year'], values=['Value'],
                       columns=['FuncSector'], aggfunc=[np.sum])
    data = pd.DataFrame(data.to_records())
    data.columns = [hdr.replace("('sum', 'Value',", "").replace(")", "").replace("'", "") \
                    for hdr in data.columns]
    return (data)


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

    p = IMFGFSRevenueData()
    writer = pd.ExcelWriter('IMFRevenue.xlsx', engine='xlsxwriter')
    p.to_excel(writer, sheet_name='Revenue', merge_cells=False)

    writer.save()


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

    data = pd.read_csv('input\GFSCOFOG_02-23-2018 23-30-06-28.csv')

    data['FuncSector'] = data[str('COFOG Function Name')] + data[str('Sector Name')]


    concord_table = pd.read_excel('input\CountryConcordanceIMF.xlsx')


    data = data.merge(concord_table, on="Country Name", how='left')


    data = pd.pivot_table(data, index=["Country name in IFs", "Unit Name", 'Time Period'], values=['Value'],
                       columns=['FuncSector'], aggfunc=[np.sum])
    data = pd.DataFrame(data.to_records())
    data.columns = [hdr.replace("('sum', 'Value',", "").replace(")", "").replace("'", "") \
                    for hdr in data.columns]
    return (data)


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

    p = IMFGFSExpenditureData()
    writer = pd.ExcelWriter('IMFGFSEXP.xls', engine='xlsxwriter')
    p.to_excel(writer, sheet_name='EXP')

    writer.save()


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
    # from urllib.request import urlopen
    from bs4 import BeautifulSoup
    from xml.dom import minidom
    import xml.etree.cElementTree as et
    import dask.dataframe as dd

    # Step 1: First read in the excel/csv with the relevant WDI codes and print out the WDI codes to be sure we have the right number of values
    datadict = pd.read_excel('input\WDICodes.xlsx', sheet_name='Sheet1',
                             dtype='str')  # Replace the location of code files for your desktop
    print('Hey User!, you are importing ' + (str(len(datadict[
                                                         'WDI Code'])) + ' series. To import additional series please update the file WDICodes.xlsx located in the Pythonfiles folder'))

    # print(datadict['WDI Code'])
    countryconcord = pd.read_csv('input\CountryConcordanceWDI.csv', encoding="ISO-8859-1")
    seriesconcord = pd.read_csv('input\CodeConcordanceWDI.csv', encoding="ISO-8859-1")
    print('Step 1 Complete: All files read in')

    # Step 2: Prepare columns for series name,country,year,value
    Seriesname = []

    df=[]
    print(
        'We will be starting the loop for all codes now.')
    # Step 3: Create a loop for reading in all the codes into the url and parsing the xml file for relevant values.This helps in pulling in 500 series at a time
    for row in datadict['WDI Code']:
        Countryname = []
        SeriesCode = []
        Year = []
        Value = []
        wiki = 'http://api.worldbank.org/v2/countries/all/indicators/' + str(row) + '/?format=xml&per_page=20000'
        r = requests.get(wiki, stream=True)
        root = et.fromstring(r.content)

        for child in root.iter("{http://www.worldbank.org}indicator"):
            SeriesCode.append(child.attrib['id'])
        for child in root.iter("{http://www.worldbank.org}country"):
            Countryname.append(child.text)
        for child in root.iter("{http://www.worldbank.org}date"):
            Year.append(child.text)
        for child in root.iter("{http://www.worldbank.org}value"):
            Value.append((child.text))
        test_df = pd.DataFrame.from_dict({'SeriesName': SeriesCode,
                                          'Country': Countryname,
                                          'Year': Year,
                                          'Value': Value}, orient='index')
        test_df = test_df.transpose()
        test_df = pd.merge(test_df, countryconcord, how='left', left_on='Country', right_on='Country')
        test_df = pd.merge(test_df, seriesconcord, how='left', left_on='SeriesName', right_on='CodeinIfs')
        test_df = test_df[pd.notnull(test_df['Value'])]
        test_df = test_df[pd.notnull(test_df['Country name in IFs'])]
        test_df = test_df[pd.notnull(test_df['Series'])]

        df.append(test_df)



    df=pd.concat(df)


     # Step 9: Write to a pivot table
    data = pd.pivot_table(df, index=['Country name in IFs', 'Year'], columns=['Series'], values=['Value'],
                          aggfunc=[np.sum])
    data = pd.DataFrame(data.to_records())
    data.columns = [hdr.replace("('sum', 'Value',", "").replace(")", "").replace("'", "") \
                   for hdr in data.columns]
    #data= df
    return (data)


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
    # from urllib.request import urlopen
    from bs4 import BeautifulSoup
    from xml.dom import minidom
    import xml.etree.cElementTree as et
    import dask.dataframe as dd

    data = WDIData()
    writer = pd.ExcelWriter('WDISeries.xlsx', engine='xlsxwriter')
    data.to_excel(writer, sheet_name='WDIData', merge_cells=False)
    writer.save()