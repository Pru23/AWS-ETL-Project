 AWS-ETL-Spotify-24 Project Repository
This project demonstrates an end-to-end data pipeline using AWS Glue and S3 for data processing and transformation. The process involves extracting data from CSV files stored in S3, transforming them using AWS Glue, and storing the processed data back into S3 in Parquet format.

![Data Pipeline](https://github.com/user-attachments/assets/3c48ac6d-01d7-4b89-96e5-ed55611fb800)





1. AWS Glue Setup
The project begins with setting up the environment using AWS Glue's GlueContext and SparkContext. The Glue job is initialized with the necessary parameters to facilitate the data extraction, transformation, and loading (ETL) process.


![image](https://github.com/user-attachments/assets/14ddf078-ab08-4677-ba99-38353f57aa28)



![image](https://github.com/user-attachments/assets/bb03bf91-edae-4e89-94ed-30bfaef5aa50)


2. Data Sources
Data for artists, albums, and tracks are stored in CSV files in an S3 bucket at s3://project-spotify-24/staging/. AWS Glue's create_dynamic_frame method is used to load this data into the pipeline for further processing.


![image](https://github.com/user-attachments/assets/1215e4fa-fd3c-445b-8e0a-a05ddffc10b1)


![image](https://github.com/user-attachments/assets/3530fcc7-1ab8-4251-b441-ddc65d08e08b)


![image](https://github.com/user-attachments/assets/7ab568bf-f401-459d-8f72-866385205f65)



3. Data Transformation
The project performs data transformations by joining the artist, album, and track datasets using matching keys (artist_id and track_id). Redundant fields are removed from the dataset using AWS Glue's DropFields transformation.

![image](https://github.com/user-attachments/assets/f785b7cd-5ea2-468b-a637-75f33fe62d26)


![image](https://github.com/user-attachments/assets/65015656-a6f3-4b1e-b51d-c1760304dcd0)




![image](https://github.com/user-attachments/assets/01c131dd-7b9c-4cc0-a1e5-0f02008d682f)

4. Data Output
Once the data is transformed, it is written back to the S3 bucket in the Parquet format. The data is compressed using Snappy and stored in a directory designated as the data warehouse at s3://project-spotify-24/datawarehouse/.

![image](https://github.com/user-attachments/assets/ee014a68-16d1-45ee-8ad4-422fe7ce5b0c)


5. Final Steps
The Glue job commits the transformations and writes the final output to S3, completing the ETL process.









