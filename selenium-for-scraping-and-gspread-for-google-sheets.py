#Import required modules
print('Importing required modules...')
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
print('Imported required modules successfully')

#Set Chromium options
print('Setting Chrome Browser Options...')
chromeOptions = webdriver.ChromeOptions()
chromeOptions.add_argument('--headless')
chromeOptions.add_argument('--no-sandbox')
chromeOptions.add_argument('--disable-dev-shm-usage')
print('Chrome Browser options set successfully')

#Get required data from Google Sheets and save it to a dataframe named 'df'
print('Retrieving data from the source sheet')
sheetId = "1gC0p9IRByjSxZq2m5_TDE79W60Jacwsk5OKBld7X6Cc"
sheetAsCsv = f"https://docs.google.com/spreadsheets/export?id={sheetId}&exportFormat=csv"
df = pd.read_csv(sheetAsCsv)
print('Data has been retrieved as follows:')
print(df)

#Open chromium
browserDriverName = ChromeDriverManager().install()

print('Opening web browser')
driver = webdriver.Chrome(browserDriverName, options=chromeOptions)
print('Web browser opened successfully')

#Get website titles and save them to df
sourceColumn = 'Address'
destinationColumn = 'Website title'

print('Acessing websites and retrieving their title. This might take a while...')
for index in df.index:
    website = df[sourceColumn][index]
    driver.get(website)
    df.loc[index, destinationColumn] = driver.title
print('After acessing the websites, the titles found were:')
print(df)

#Close the browser
print('Closing web browser')
driver.quit()
print('Web browser closed successfully')

#Configure gspread to communicate with Google API
print('Checking required credentials to update the Sheet')
credsLocation = './credentials.json'
scope = []
scope.append('https://spreadsheets.google.com/feeds')
scope.append('https://www.googleapis.com/auth/drive')

creds = ServiceAccountCredentials.from_json_keyfile_name(credsLocation, scope)
client = gspread.authorize(creds)
print('Credentials set successfully')

#Open the sheet, then open the desired tab
print('Saving data to your sheet')
sheet = client.open_by_key(sheetId)
sheetInstance = sheet.get_worksheet(0)

#Update the results column, iteratively. Note: update_cell(self, row, col, value)
col = 3
firstRowToWrite = 2
for index in df.index:
    row = index + firstRowToWrite
    value = df.loc[index, destinationColumn]
    sheetInstance.update_cell(row=row,col=col, value=value)
print('Your sheet was successfully updated!')
print('END OF SCRIPT')
