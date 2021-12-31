# Data Dictionary

#### dim_ports <br>

|Field|Type|Description|
|----|-----|-----------|
|port_id|bigint|Primary Key|
|port_code|varchar(3) not null|3 character code used for I94 ports|
|port_city|varchar(256)| U.S. city of port|
|port_state|varchar(50)|U.S. state of port|
|average_temperature|numeric(16,3)|Average temperature of port city|

## dim_countries

|Field|Type|Description|
|----|-----|-----------|
|country_id|bigint|Primary Key|
|country_code|varchar(3) not null|3 character code used for I94 countries|
|country|varchar(256) not null|Country from I94 countries|
|average_temperature|numeric(16,3)|Average temperature of country|

## dim_time

|Field|Type|Description|
|----|-----|-----------|
|sas_timestamp|int not null| Primary Key - The SAS timestamp (days since 1/1/1960)|
|year|int not null|4 digit year|
|month|int not null|Month (1-12)|
|day|int not null|Day (1-31)|
|week|int not null|Week of Year (1-52)|
|day_of_week|int not null|Day of Week (1-7) starting on Sunday|
|quarter|int not null|Quarter of Year (1-4)|

## dim_demographics

|Field|Type|Description|
|----|-----|-----------|
|demographics_id|bigint|Primary Key|
|port_id|int8|Foreign key to dim_ports|
|median_age|numeric(18,2)|The median age for the demographic|
|male_population|int|Count of male population for city|
|female_population|int|Count of female population for city|
|total_population|bigint|Count of population for city|
|num_of_veterans|int|Count of veterans|
|foreign_born|int|Count of foreign born persons|
|avg_household_size|numeric(18,2)|Average household size in city|
|race|varchar(100)|Race for this demographic|
|demo_count|int|Count for this demographic|

## dim_airports

|Field|Type|Description|
|----|-----|-----------|
|airport_id|int|Primary Key|
|port_id|int|Foreign key to dim_ports|
|airport_type|varchar(256)|Short description of airport type|
|airport_name|varchar(256)|Airport Name|
|elevation_ft|int|Airport elevation|
|municipality|varchar(256)|Airport municipality|
|gps_code|varchar(256)|Airport GPS code|
|iata_code|varchar(256)|Airport International Air Transport Association code|
|local_code|varchar(256)|Airport local code|
|coordinates|varchar(256)|Airport Coordinates|

## fact_immigration

|Field|Type|Description|
|----|-----|-----------|
|immigration_id|bigint|Primary Key|
|country_id|bigint|Foreign key to dim_countries|
|port_id|bigint|Foreign Key to dim_ports|
|age|int|Age of immigrant|
|travel_mode|varchar(100)|Mode of travel for immigrant (air, sea, land, etc.)|
|visa_category|varchar(100)|Immigrant VISA category|
|visa_type|varchar(100)|Type of VISA|
|gender|varchar(10)|Immigrant gender|
|arrdate|int|SAS timestamp of arrival date, Foreign key to dim_time|
|depdate|int|SAS timestamp of departure date, Foreign key to dim_time|