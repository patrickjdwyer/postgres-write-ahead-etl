# postgres-write-ahead-etl
Spark job to process Postgres write ahead logs.  The job reads the flat files, parses them, and stores them in Postgres tables.  It's run as a Jupyter Notebook.

## To Run

Note - I assume the user already has Spark installed.  I used version 2.4.0 for this project.

### 1. Set up Postgres

You'll need a Postgres instance for the Spark job to interact with.  I used the Postgres Docker image found here: https://hub.docker.com/_/postgres

You can pull and run the image using:
```
docker pull postgres
docker run --name vanguard -e POSTGRES_PASSWORD=vanguard -p 5432:5432 -d postgres
```

### 2. Install Libraries

This project uses two Python libraries, other than PySpark: Jupyter Notebook and Psycopg.  I installed both using pip as follows:

```
pip install notebook
pip install psycopg2-binary
```

More install instructions can be found at:
- https://jupyter.org/install
- https://pypi.org/project/psycopg2/

### 3. Run Notebook

Once the dependencies are set up, just run `jupyter notebook` to start the server.  The code is run sequentially from top to bottom.

Note that if you are running Postgres in a different way than I described above, you may need to change the Postgres configuration parameters in cell 2.

## Implementation Details

The job has three major steps:
- Read in the data and explode it so there's one change per row
- Create database tables (if needed)
- Process the logs and store in separate tables

Initially, I thought I'd be able to use the Postgres JDBC's `createTableColumnTypes` option to ensure the tables have the proper schema.  But I encountered issues using this option, so I decided to write a separate step to create the tables instead.  This ensures they have the correct schema and since it only has to be run once, I don't think it's big deal to have this separate.

I also encountered issues dealing with null values in the data.  It seems that Spark and the Postgres JDBC don't work well with nulls, so I had to fill in the null values in order for the code to work.  See this Stack Overflow post for more info: https://stackoverflow.com/questions/64671739/pyspark-nullable-uuid-type-uuid-but-expression-is-of-type-character-varying

