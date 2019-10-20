from google.cloud import storage
import os
import psycopg2
import gzip
import csv
import pandas as pd
import datetime as dt
from datetime import datetime
from sqlalchemy import create_engine

# Initiate client
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\samle\\PycharmProjects\\fenestraCode\\venv\Lib\\account\\application_default_credentials.json"
client = storage.Client.from_service_account_json('C:\\Users\\samle\\PycharmProjects\\fenestraCode\\venv\\Lib\\account\\fenestracodeassignment-e0bd7fb6aaaa.json')
# client = storage.Client()

# Define bucket name/id
bucketId = 'python_case_study'
# 'fst-python-case-study'
sqlTableName = 'revenueTable'

bucket = client.get_bucket(bucketId)

# DB details
firstRun = True
dbname=''
user=''
password=''
host=''
port=''

timeStamp = datetime.now()
timestampStr = timeStamp.strftime("%d%b%Y_%H%M")

# set the directories for downloading and creating the reports
workingDir = os.getcwd()
fileDir = workingDir + '\\Lib\\downloads\\'
answerDirectory = workingDir +'\\Lib\\answers\\{}\\'.format(timestampStr)

# create directory for answers
try:
    os.mkdir(answerDirectory)
except OSError:
    print ("Creation of the directory %s failed" % answerDirectory)

# Function to list the files in the bucket
def listFiles():
    files = bucket.list_blobs()
    fileList = [file.name for file in files if '.' in file.name]
    print(fileList)
    return fileList

# Download the files identified as being new in the bucket (could add in created by date in future)
# Currently this is only done by checking what has been downloaded locally... not an ideal solution as later processes may fail
def downloadFiles(fileList, downloadDirectory):
    newFilelist = list()
    for fileName in fileList:
        if os.path.isfile(downloadDirectory+fileName)==False:
            blob = bucket.get_blob(fileName)
            blob.download_to_filename(downloadDirectory+fileName)
            newFilelist.append(fileName)
            print('{} downloaded...'.format(fileName))
    print(newFilelist)
    return newFilelist

# unzip the files and create csv version to be loaded into DB
def unZip(fileNames, downloadDirectory):
    rawFileNames = list()
    for fileName in fileNames:
        unzipFileName = downloadDirectory+fileName[:-3]
        if os.path.isfile(unzipFileName)==False:
            fp = open(unzipFileName, "wb")
            try :
                with gzip.open(downloadDirectory+fileName, mode='rb') as file:
                    data = file.read()
                    fp.write(data)
                    fp.close()
                    rawFileNames.append(unzipFileName)
            except Exception as e:
                print('File could not be unzipped: ' + e)
    # return the file directories of the files to be loaded
    return rawFileNames

# This step converts time to valid timestamp and Allowed me to reduce number of rows analysed for testing
def prepareCsvFilesForUpload(files):
    editFileList = list()
    duplicatesCount = list()
    for file in files:
        try :
            df = pd.read_csv(file, encoding='utf8')
            numDuplicates = initLength - postLength # calculate the number of duplicate rows removed
            df['Time'] = df['Time'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d-%H:%M:%S')) # convert time to timestamp
            editFileName = file[:-4] + '_edit.csv'
            df.iloc[:].to_csv(editFileName, index=False, encoding='utf8') # write the edited dataframe to csv
            editFileList.append(editFileName)
            duplicatesCount.append(numDuplicates)
            print('Processed file: {}...'.format(editFileName))
        except Exception as e:
            print(e)

def createTable(tableName, cleanTable, dbName, user, pw, host, port):
    conn = psycopg2.connect(dbname=dbName, user=user, password=pw, host=host, port=port)
    cur = conn.cursor()
    # Have manually created the CREATE TABLE string for now, but ideally this should be made dynamic
    createTableString = """CREATE TABLE IF NOT EXISTS {} (
                Ind SERIAL PRIMARY KEY,
                Time TIMESTAMP,
                AdvertiserId BIGINT,
                OrderId BIGINT,
                LineItemId BIGINT,
                CreativeId BIGINT,
                CreativeVersion INT,
                CreativeSize VARCHAR(9),
                AdUnitId TEXT,
                Domain TEXT,
                CountryId INT,
                RegionId INT,
                MetroId INT,
                CityId INT,
                BrowserId INT,
                OSId INT,
                OSVersion TEXT,
                TimeUsec2 BIGINT,
                KeyPart VARCHAR(9),
                Product TEXT,
                RequestedAdUnitSizes TEXT,
                BandwidthGroupId INT,
                MobileDevice VARCHAR(256),
                IsCompanion bool,
                DeviceCategory TEXT,
                ActiveViewEligibleImpression TEXT,
                MobileCarrier VARCHAR(256),
                EstimatedBackfillRevenue real,
                GfpContentId INT,
                PostalCodeId INT,
                BandwidthId BIGINT,
                AudienceSegmentIds TEXT,
                MobileCapability VARCHAR(256),
                PublisherProvidedID VARCHAR(64),
                VideoPosition INT,
                PodPosition INT,
                VideoFallbackPosition INT,
                IsInterstitial bool,
                EventTimeUsec2 BIGINT,
                EventKeyPart VARCHAR(9),
                YieldGroupCompanyId FLOAT,
                RequestLanguage TEXT,
                DealId TEXT,
                SellerReservePrice real,
                DealType TEXT,
                AdxAccountId INT,
                Buyer TEXT,
                Advertiser VARCHAR(256),
                Anonymous bool,
                ImpressionId TEXT      
            )
            """.format(tableName)
    if cleanTable == True:
        removeTableString = ("""DROP TABLE IF EXISTS {}""".format(tableName))
        cur.execute(removeTableString)
        print('Existing table removed: ' + tableName)
    cur.execute(createTableString)
    conn.commit()
    conn.close()

def createTableStaging(tableName, cleanTable, dbName, user, pw, host, port):
    conn = psycopg2.connect(dbname=dbName, user=user, password=pw, host=host, port=port)
    cur = conn.cursor()
    # Have manually created the CREATE TABLE string, but this needs to be made dynamic
    createTableString = """CREATE TABLE IF NOT EXISTS {} (
                Time TIMESTAMP,
                AdvertiserId BIGINT,
                OrderId BIGINT,
                LineItemId BIGINT,
                CreativeId BIGINT,
                CreativeVersion INT,
                CreativeSize VARCHAR(9),
                AdUnitId TEXT,
                Domain TEXT,
                CountryId INT,
                RegionId INT,
                MetroId INT,
                CityId INT,
                BrowserId INT,
                OSId INT,
                OSVersion TEXT,
                TimeUsec2 BIGINT,
                KeyPart VARCHAR(9),
                Product TEXT,
                RequestedAdUnitSizes TEXT,
                BandwidthGroupId INT,
                MobileDevice VARCHAR(256),
                IsCompanion bool,
                DeviceCategory TEXT,
                ActiveViewEligibleImpression TEXT,
                MobileCarrier VARCHAR(256),
                EstimatedBackfillRevenue real,
                GfpContentId INT,
                PostalCodeId INT,
                BandwidthId BIGINT,
                AudienceSegmentIds TEXT,
                MobileCapability VARCHAR(256),
                PublisherProvidedID VARCHAR(64),
                VideoPosition INT,
                PodPosition INT,
                VideoFallbackPosition INT,
                IsInterstitial bool,
                EventTimeUsec2 BIGINT,
                EventKeyPart VARCHAR(9),
                YieldGroupCompanyId FLOAT,
                RequestLanguage TEXT,
                DealId TEXT,
                SellerReservePrice real,
                DealType TEXT,
                AdxAccountId INT,
                Buyer TEXT,
                Advertiser VARCHAR(256),
                Anonymous bool,
                ImpressionId TEXT      
            )
            """.format(tableName)
    if cleanTable == True:
        removeTableString = ("""DROP TABLE IF EXISTS {}""".format(tableName))
        cur.execute(removeTableString)
        print('Existing table removed: ' + tableName)
    cur.execute(createTableString)
    conn.commit()
    conn.close()

def pg_load_table(file_path, table_name, dbname, host, port, user, pwd):
    '''
    This function uploads csv to a target table
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        print("Connecting to Database")
        cur = conn.cursor()
        f = open(file_path, "r", encoding='utf8')
        # Truncate the table first
        cur.execute("Truncate {} Cascade;".format(table_name))
        print("Truncated {}".format(table_name))
        # Load table from the file with header
        cur.copy_expert("copy {} from STDIN CSV HEADER ENCODING 'utf-8' QUOTE '\"'".format(table_name), f)
        cur.execute("commit;")
        print("Loaded data into {}".format(table_name))
        conn.close()
        print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))

# Insert the staging table with the main table
def combineStagingToOriginalTable(stagingName, tableName, dbName, host, port, user, pwd):
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        cur = conn.cursor()
        cur.execute("""INSERT INTO {} (Time, AdvertiserId, OrderId, LineItemId, CreativeId, CreativeVersion, CreativeSize, AdUnitId, Domain, CountryId, RegionId, MetroId, CityId, BrowserId, OSId, OSVersion, TimeUsec2, KeyPart, Product, RequestedAdUnitSizes, BandwidthGroupId, MobileDevice, IsCompanion, DeviceCategory, ActiveViewEligibleImpression, MobileCarrier, EstimatedBackfillRevenue, GfpContentId, PostalCodeId, BandwidthId, AudienceSegmentIds, MobileCapability, PublisherProvidedID, VideoPosition, PodPosition, VideoFallbackPosition, IsInterstitial, EventTimeUsec2, EventKeyPart, YieldGroupCompanyId, RequestLanguage, DealId, SellerReservePrice, DealType, AdxAccountId, Buyer, Advertiser, Anonymous, ImpressionId)
SELECT Time, AdvertiserId, OrderId, LineItemId, CreativeId, CreativeVersion, CreativeSize, AdUnitId, Domain, CountryId, RegionId, MetroId, CityId, BrowserId, OSId, OSVersion, TimeUsec2, KeyPart, Product, RequestedAdUnitSizes, BandwidthGroupId, MobileDevice, IsCompanion, DeviceCategory, ActiveViewEligibleImpression, MobileCarrier, EstimatedBackfillRevenue, GfpContentId, PostalCodeId, BandwidthId, AudienceSegmentIds, MobileCapability, PublisherProvidedID, VideoPosition, PodPosition, VideoFallbackPosition, IsInterstitial, EventTimeUsec2, EventKeyPart, YieldGroupCompanyId, RequestLanguage, DealId, SellerReservePrice, DealType, AdxAccountId, Buyer, Advertiser, Anonymous, ImpressionId
FROM {}""".format(tableName, stagingName))
        conn.commit()
        conn.close()
        print('Insert {} into {} successful'.format(stagingName, tableName))

    except Exception as e:
        print('Insert {} into {} failed'.format(stagingName, tableName))
        print(e)


def loadCSVtoSQL(files, tableName, dbName, user, pw, host, port):
    if firstRun:
        createTable(tableName, True, dbName, user, pw, host, port)
    tableNo = 0
    for file in files:
        try :
            stagingName = 'revenueTable_staging_{}'.format(tableNo)
            createTableStaging(stagingName, True, dbName, user, pw, host, port)
            pg_load_table(file, stagingName, dbName, host, port, user, pw)
            print('loaded file: {}...'.format(file))
            combineStagingToOriginalTable(stagingName, tableName, dbName, host, port, user, pw)
            tableNo += 1

        except Exception as e:
            print(e)

def processDuplicates(tableName, dbName, user, pwd, host, port):
    conn = psycopg2.connect(dbname=dbname, host=host, port=port, \
                            user=user, password=pwd)
    cur = conn.cursor()
    # query in order to delete rows that aren't distinct
    query = """DELETE FROM {} WHERE {}.ind NOT IN 
(SELECT ind FROM (
    SELECT DISTINCT ON (Time, AdvertiserId, OrderId, LineItemId, CreativeId, CreativeVersion, CreativeSize, AdUnitId, Domain, CountryId, RegionId, MetroId, CityId, BrowserId, OSId, OSVersion, TimeUsec2, KeyPart, Product, RequestedAdUnitSizes, BandwidthGroupId, MobileDevice, IsCompanion, DeviceCategory, ActiveViewEligibleImpression, MobileCarrier, EstimatedBackfillRevenue, GfpContentId, PostalCodeId, BandwidthId, AudienceSegmentIds, MobileCapability, PublisherProvidedID, VideoPosition, PodPosition, VideoFallbackPosition, IsInterstitial, EventTimeUsec2, EventKeyPart, YieldGroupCompanyId, RequestLanguage, DealId, SellerReservePrice, DealType, AdxAccountId, Buyer, Advertiser, Anonymous, ImpressionId) *
  FROM {}) as x)""".format(tableName,tableName,tableName)
    print(query)
    try:
        cur.execute(query)
        print('The number of duplicates deleted:')
        print(cur.statusmessage)
    except Exception as e:
        print('remove duplicates didn\'t run successfully')
        print(e)


# Run through sql queries
def createAnswerCsvs(answerDirectory, tableName, commands, dbName, user, pwd, host, port):
    count = 1
    conn = psycopg2.connect(dbname=dbname, host=host, port=port, \
                            user=user, password=pwd)
    cur = conn.cursor()
    for command in commands:
        with open(answerDirectory+'queryResult_'+str(count)+'.csv', 'w') as f:
            query = command.format(tableName)
            print(query)
            try :
                outputquery = 'copy ({0}) to stdout with csv header'.format(query)
                cur.copy_expert(outputquery, f)
            except Exception as e:
                print('command {} didn\'t run successfully'.format(count))
                print(e)

        count += 1
    conn.close()

fileList = listFiles() # list files in bucket
newFiles = downloadFiles(fileList, fileDir) # check which are new and download
fileNames = unZip(newFiles, fileDir) # unzip csv files
fileToLoad = prepareCsvFilesForUpload(fileNames) # format files
loadCSVtoSQL(fileToLoad,sqlTableName,dbname, user, password, host, port) # load csv's into staging, then merge with table
processDuplicates(sqlTableName,dbname, user, password, host, port) # remove duplicate records

# list of sql queries for questions
questionCommands = ["""SELECT date(time) as days, count(*)
FROM {}
GROUP BY days
ORDER By days""",
"""SELECT date_trunc('hour',time) as hours, count(*)
FROM {}
GROUP BY hours
ORDER By hours""",
"""SELECT AVG(records) as average_counts_perhour
FROM
(SELECT date_trunc('hour',time) as hours, count(*) as records
FROM {}
GROUP BY hours
ORDER By hours) as hourTable""",
"""SELECT date(time) as days, 
	count(*) as records,
 	sum(estimatedbackfillrevenue) as totalEstimatedRevenue
FROM {}
GROUP BY days
ORDER By days""",
"""SELECT date_trunc('hour',time) as hours, 
	count(*) as records,
 	sum(estimatedbackfillrevenue) as totalEstimatedRevenue
FROM {}
GROUP BY hours
ORDER By hours""",
"""SELECT buyer, 
	count(*) as records,
 	sum(estimatedbackfillrevenue) as totalEstimatedRevenue
FROM {}
GROUP BY buyer
ORDER By buyer""",
"""SELECT advertiser, 
	string_agg(distinct devicecategory,', ') as devicecategories
FROM {}
GROUP BY advertiser
ORDER By advertiser"""]

createAnswerCsvs(answerDirectory, sqlTableName, questionCommands,dbname, user, password, host, port) # create csv containing answers
