## ETL_Board_of_Review_Decisions
Project in progress:
1. copy_data_to_s3.py retrieves data from Cook County's open API and writes the decision history in 2020 and 2021 as 2 json files to a S3 bucket. 
2. the py documents starting with "infrastructure" set up Redshift Clusters to store the data, check its status, and delete the resource, respectively. 
3. the upcoming python files will transforms the json files and loads 2 new tables into Redshift. The transformed data will be a table containing the properties that have appealed in both 2020 and 2021. The Redshift will be connected to visualization tools to draw a map of small businesses that have had >$10k increase in total value assessment determined by the board of review.
