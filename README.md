# googleCloudInfra
Python Program to pipe data from Cloud Storage to Postgres

Setup:
1. Add in DB credentials, download directory and directory for answers to be exported to
2. Setup and download authentication JSON for service account to bucket and point client to path.
3. Set Boolean on whether it is first run and main table needs creating
4. Customise variables for names of SQL table etc

Current steps are:

1. Download data from Google Cloud Bucket
2. Unzip files from .gzip format to csv
3. Process files using pandas library to reformat (this step currently needs streamlining, ideal solution would skip download and process of files locally)
4. Files are loaded into staging tables to confirm import into postgres
5. Staging table is merged with main Table and duplicates are removed and count of deletion printed
6. SQL queries are carried out on main Table and results exported to CSV file in target directory

(To do/nice to haves.. 
* improve recognition of new files in Bucket
* dynamic creation of tables, less hard coded postgres queries - recognition of headers etc
* Clean up of local files and staging tables
* Look at improving performance of loading CSV to DB... Current process seems slow
* Addition of UI.. )
