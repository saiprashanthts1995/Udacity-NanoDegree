# Analysis of Immigration and Temperature data by State
### Data Engineering Capstone Project

#### Project Summary
Main Purpose of this project is analyse the immigration, demographics, temperature and Airport data by State after performing the required all ETL Operation.

In the process of building it I have designed and developed to create an star schema comprising of 4 dimension table and an fact table

Dimension Table Details
1. DIM_IMMIGRATION
2. DIM_TEMPERATURE
3. DIM_DEMOGRAHICS
4. DIM_AIRPORT

FACT TABLE Details
1. FACT_IMMIGRATION_TEMP_BY_CITY

ETL Process has the following process

##### Extract
Data is obtained from all open sources 

I94 Immigration data is obtained from following source
        [ImmigrationData](https://travel.trade.gov/research/reports/i94/historical/2016.html)

Temperature Data
        [Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data/data?select=GlobalLandTemperaturesByState.csv)

U.S. City Demographic Data: This data comes from OpenSoft. [Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

Airport Code Table: This is a simple table of airport codes and corresponding cities. [Airport Data](https://datahub.io/core/airport-codes#data)

##### Transform

Data is transformed after cleaning the data

##### Load

After performing the cleaning process data is loaded to dimension table and Fact table. These dataframes are written as parquet file
These files are partitioned by I94 port. so that people who query these data will have faster results

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 

Analyse all 4 sources of data. Using the above 4 sources create an Fact table by combining it. The fact table designed is done for an specific state to analyse how that particular state data comes out. For instance, I have considered New York

All the stuff has been done using Pyspark operation. In pyspark most of the operations is done by using the Dataframe utility and SQL operations of pyspark

#### Describe and Gather Data 
1. I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. [Here](https://travel.trade.gov/research/reports/i94/historical/2016.html)
2. World Temperature Data: This dataset came from Kaggle. [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data/data?select=GlobalLandTemperaturesByState.csv)
3. U.S. City Demographic Data: This data comes from OpenSoft. [Here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
4. Airport Code Table: This is a simple table of airport codes and corresponding cities. [Here](https://datahub.io/core/airport-codes#data)

### Step 2: Explore and Assess the Data
#### Explore the Data 
Here using the pyspark sql utility I have created each dimension after performing not null check and by taking an distinct distinct values

#### Cleaning Steps
1. DIM_AIRPORT

    1. upper(iso_country) = "US" 
    2. iso_region is not null 
    3. type = 'heliport'
    4. Performed Casting

2. DIM_TEMPERATURE

    1. trim(upper(Country)) = 'UNITED STATES' 
    2. AverageTemperature IS NOT NULL 
    3. AverageTemperatureUncertainty is not null
    4. Performed Casting

3. DIM_DEMOGRAPHICS
    1. Performed Casting

4. DIM_IMMIGRATION
    1. i94addr is not null
    2. i94port not in ('XXX')
    3. Performed Casting


### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Map out the conceptual data model and explain why you chose that model

##### Dimensions

DIM_AIRPORT
```
    airport_surr_key big int,
    name string,
    state string,
    elevation_ft float,
    coordinates string
```

DIM_TEMPERATURE
```
    temperature_surr_key bigint,
    dtdate,
    AverageTemperature float ,
    AverageTemperatureUncertainty float, 
    state varchar, 
    Country varchar
```

DIM_DEMOGRAPHICS
```
    demographics_surr_key big int, 
    Median_Age float, 
    Male_population float,
    Female_Population float, 
    Total_Population float, 
    Num_Of_Verterans float,
    Foreign_Born float, 
    state string
```

DIM_IMMIGRATION
```
    immigration_surr_key Big int,
    state string,
    i94yr float,
    i94mon float,
    city_port_name string,
    cicid float,
    i94visa float,
    i94mode float,
    occup string
```
##### Fact

FINAL_FACT
```
    state varchar,
    i94yr int,
    i94mon int,
    city_port_name varchar,
    Median_Age float, 
    Male_population float,
    Female_Population float, 
    Total_Population float, 
    Num_Of_Verterans float,
    Foreign_Born float
```


### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.

Loaded the following data by partition by state
    * DIM_AIRPORT
    * DIM_TEMPERATURE
    * DIM_DEMOGRAPHICS
    * DIM_IMMIGRATION

Loaded the following data by partition by state
    * FACT_TABLE


#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks

Created an Data Quality check if an tables are loaded. By checking the count. if count of records is zero then fail out

Stats of Data Quality checks for each table
```
Data quality check passed for DataFrame[airport_surr_key: bigint, name: string, state: string, elevation_ft: float, coordinates: string] with 88907 records

Data quality check passed for DataFrame[temperature_surr_key: bigint, dt: string, AverageTemperature: float, AverageTemperatureUncertainty: float, state: string, Country: string] with 141930 records

Data quality check passed for DataFrame[demographics_surr_key: bigint, Median_Age: int, Male_population: float, Female_Population: float, Total_Population: float, Num_Of_Verterans: int, Foreign_Born: int, state: string] with 2875 records

Data quality check passed for DataFrame[immigration_surr_key: bigint, state: string, i94yr: double, i94mon: double, city_port_name: string, cicid: double, i94visa: double, i94mode: double, occup: string] with 10232 records

Data quality check passed for DataFrame[state: string, i94yr: double, i94mon: double, city_port_name: string, Median_Age: int, Male_population: float, Female_Population: float, Total_Population: float, Num_Of_Verterans: int, Foreign_Born: int] with 1352712 records



```


#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
        Used Python, Jupyter Notebook, Spark, Spark SQL(HIVE)
* Propose how often the data should be updated and why.
        Data will be updated once an month as this can be used for analysis by year,month
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
        We can use of redshift, EMR (WIth Multiple Nodes) if data got increased
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
        We can schedule it to run every morning 7 am if neccessary using AIrflow
 * The database needed to be accessed by 100+ people.
        We can host it in AWS Cloud. By increasing the number of Nodes it can be accessed by 100 people

#### Project Summary



