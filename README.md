# Pinterest Data Pipeline

## The Architecture
The Pinterest data pipeline architecture allows the collection and processing of large amounts of data in real-time. This ensures that they can make efficient data-driven decisions for their business. The pipeline built here emulates Pinterest's own model. It comprises 4 parts:

- **Data ingestion** - Apache Kafka is used here to stream real-time data from the API before being passed to Spark for transformations to be applied. Data is sent to the API from the user_posting_emulation.py script, which provides a real-time stream of data, ready to be ingested and processed. user_posting_emulation.py works by generating an infinite loop of Pinterest posts extracted from a database. These posts are are sent to the server to which the API is listening. project_pin_API.py contains the API, which takes the posts sent to the server and passes them to a Kafka producer, which publishes (writes) events to a Kafka topic.

- **Data processing** - Apache Spark handles the data cleaning and normalisation via PySpark. In this data pipeline architecture, there is both a batch processing pipeline, as well as a streaming pipeline. In the batch processing pipeline, data is read from the topic by the Kafka consumer and sent to be stored in the data lake (Amazon S3). The data is subsequently extracted from the data lake and processed with Spark. In the streaming pipeline, data is read into a Spark streaming data frame from the Kafka consumer, making it ready for transformations to be applied.  

- **Data orchestration** - Apache Airflow is a task scheduling platform which allows tasks to be created and managed. In this project, the Spark cleaning and normalisation tasks for the batch processing job have been automated to run each day.  

- **Data storage** - For the batch processing pipeline, data is stored in the data lake before being subsequently processed. For the streaming pipeline, the transformations are applied and the stream of cleaned data is sent to a local Postgres database for storage. Some further computations were done on the data, including a count of the total number of posts per category and category popularity (mean follower count per category). The results of these were also sent to the Postgres database for permanent storage.

## The Data
The Pinterest data contains information about each pin which a user creates. For each pin, there are multiple categories of information. They are:
- index
- unique_id
- title
- description
- poster_name
- follower_count
- tag_list
- is_image_or_video
- image_src
- downloaded
- save_location
- category

## Data Cleaning
The transformations needing to be applied to this data set included:
- the normalisation of the follower counts, as they included the letters 'k' (meaning thousand) and 'M' (meaning million). These were transformed into a sequence of 3 zeros and 6 zeros respectively and subsequently, the follower counts were cast from text strings to LongType integers.
- the removal of unnecessary preamble in save_location, leaving just the full path name.
- the transformation of any missing data points to null values.
- the dropping of any rows which contained at least 3 key missing data points (title, description, follower count).