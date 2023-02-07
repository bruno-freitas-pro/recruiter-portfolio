#Import logging module
import logging
logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', level=logging.INFO)

#Import required modules
logging.info('Importing required modules...')
import pandas as pd
import os
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from oauth2client.service_account import ServiceAccountCredentials
import gspread
logging.info('Imported required modules successfully')

def getDfFromGoogleSheets (sheetId):
    '''
    Retrieves data from Google Sheets based on a Sheet ID and saves that data to a dataframe

    The sheetId can be obtained from a link that looks like "https://docs.google.com/spreadsheets/d/1ZAOAJeoZroFWdkD-3fBl8JPD4SBYyOkpv0i3aPye0aM"
    '''
    logging.info('Retrieving data from the source sheet')
    sheetAsCsv = f"https://docs.google.com/spreadsheets/export?id={sheetId}&exportFormat=csv"
    df = pd.read_csv(sheetAsCsv)
    logging.info('Data retrieved successfully!')
    print('Data has been retrieved as follows:')
    print(df)
    return df

def setChromeOptions(optionsTuple):
    '''
    Configures Google Chrome options for usage with Selenium webdriver

    Takes a tuple as argument, e.g. "options = ('--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--log-level=3')"
    '''
    logging.info('Setting Chrome Browser Options...')
    chromeOptions = webdriver.ChromeOptions()
    for element in optionsTuple:
        chromeOptions.add_argument(element)
    logging.info('Chrome Browser options set successfully')
    return chromeOptions

def openChrome(options):
    '''
    Opens Google Chrome via Selenium Webdriver with the selected options

    You can generate the adequate options variable using the setChromeOptions function
    '''
    logging.info('Opening web browser')
    browserDriverName = ChromeDriverManager().install()
    driver = webdriver.Chrome(service=Service(browserDriverName), options=options)
    logging.info('Web browser opened successfully')
    return driver

def getWebsiteTitle(driver, df, sourceColumn, destinationColumn):
    '''
    Retrieves website title using Selenium
    '''
    logging.info('Acessing websites and retrieving their title. This might take a while, please wait')
    for index in df.index:
        website = df[sourceColumn][index]
        driver.get(website)
        df.loc[index, destinationColumn] = driver.title
    logging.info('All websites have been accessed succesfully')
    print('After acessing the websites, the titles found were:')
    print(df)
    return df

def closeChrome(driver):
    '''
    Closes Google Chrome
    '''
    logging.info('Closing web browser')
    driver.quit()
    logging.info('Web browser closed successfully')

def setGspreadClient (credsPath):
    '''
    Configures gspread client for usage
    '''
    logging.info('Checking required credentials to update the Sheet')
    scope = []
    scope.append('https://spreadsheets.google.com/feeds')
    scope.append('https://www.googleapis.com/auth/drive')

    creds = ServiceAccountCredentials.from_json_keyfile_name(credsPath, scope)
    client = gspread.authorize(creds)
    logging.info('Credentials set successfully')
    return client

def updateSheet(sheetId, df, client, column, destinationColumn, firstRowToWrite):
    '''
    Updates Google Sheet using gspread
    '''
    logging.info('Updating your sheet...')
    sheet = client.open_by_key(sheetId)
    sheetInstance = sheet.get_worksheet(0)
    for index in df.index:
        row = index + firstRowToWrite
        value = df.loc[index, destinationColumn]
        sheetInstance.update_cell(row=row,col=column, value=value)
    logging.info('Your sheet was successfully updated!')


mySheetId = "1gC0p9IRByjSxZq2m5_TDE79W60Jacwsk5OKBld7X6Cc"
myOptions = ('--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--log-level=3')
mySourceColumn = 'Address'
myDestinationColumn = 'Website title'
myCredsLocation = os.path.join(os.path.curdir, 'credentials.json')
myDfColumn = 3
myFirstRowToWrite = 2

mydf1 = getDfFromGoogleSheets(mySheetId)
myChromeOptions = setChromeOptions(myOptions)
myDriver = openChrome(myChromeOptions)
mydf2 = getWebsiteTitle(driver=myDriver,
                        df=mydf1,
                        sourceColumn=mySourceColumn,
                        destinationColumn=myDestinationColumn)
closeChrome(driver=myDriver)
myClient = setGspreadClient(myCredsLocation)
updateSheet(sheetId=mySheetId,
            df=mydf2,
            client=myClient,
            column=myDfColumn,
            destinationColumn=myDestinationColumn,
            firstRowToWrite=myFirstRowToWrite)

logging.info('End of script')
