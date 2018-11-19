import pip

def installAll():
  def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        from pip._internal import main as pip2
        pip2(['install', package])


  install('numpy')
  install('requests')
  install('zipfile')
  install('matplotlib')
  install('matplotlib.pyplot')
  install('toolz')
  install('cloudpickle')
  install('PIL')
  install('scipy')
  install('pandas')
  install('csv')
  install('xlrd')
  install('matplotlib.lines')
  install('matplotlib.pyplot')
  install('xlsxwriter')
  install('matplotlib.transforms')
  install('statsmodels')
  install('statsmodels.api')
  install('glob')
  install('dask')
  install('statsmodels.api')
  install('splinter')
  install('xml.etree.cElementTree')
  install('bs4')
  install('lxml')

  import zipfile
#need to download Pythonfiles.zip and place in public path
#this file contains all data - so with data updates just place in folder
#don't extract zip file to public
#could change path below to downloads folder
  zip_ref = zipfile.ZipFile('C:\\Users\Public\Pythonfiles.zip', 'r')
  zip_ref.extractall(r'C:\Users\Public')
  zip_ref.close()

