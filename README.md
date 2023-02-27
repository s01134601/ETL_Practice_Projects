## ETL_Board_of_Review_Decisions
Project in progress - the project extracts from S3 buckets in which Cook County Board of Review's Appeal Decisions from 2020 and 2021 is stored,
transforms the data to create tables that show the board of review's total value for properties that appealed in both 2020 and 2021 have changed. 
This value multiplied by the tax rate will be the tax payment for properties. 
Specifically, tables will be created for small residential buildings and small businesses. It will then be joined with the list of properties in several 
communities on the south side, and loaded into Redshift. 
Eventually, it will be loaded into ArcGIS for visualization.
