import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')
AIRPORT_DATA = config.get('S3', 'AIRPORT_PATH')
DEMO_DATA = config.get('S3', 'DEMO_PATH')
PORT_DATA = config.get('S3', 'PORT_PATH')
COUNTRY_DATA = config.get('S3', 'COUNTRY_PATH')
TEMPERATURE_DATA = config.get('S3', 'TEMPERATURE_PATH')
IMMIGRATION_DATA = config.get('S3', 'IMMIGRATION_PATH')


# CREATE STAGING TABLES

create_staging_port_table = ("""
DROP TABLE IF EXISTS public.staging_ports;
CREATE TABLE IF NOT EXISTS public.staging_ports (
    port_code varchar(3),
    city varchar(256),
    state varchar(50)
);
""")

create_staging_country_table = ("""
DROP TABLE IF EXISTS public.staging_countries;
CREATE TABLE public.staging_countries (
    country_code varchar(3) NOT NULL,
    country varchar(256) NOT NULL
    );
""")

create_staging_airport_table = ("""
DROP TABLE IF EXISTS public.staging_airports;
    CREATE TABLE public.staging_airports (
    ident VARCHAR(256) NOT NULL, 
    type VARCHAR(256) NOT NULL, 
    name VARCHAR(256) NOT NULL, 
    elevation_ft INTEGER, 
    iso_country VARCHAR(256), 
    municipality VARCHAR(256), 
    gps_code VARCHAR(256), 
    local_code VARCHAR(256), 
    coordinates varchar(256)
    );
""")

create_staging_temperature_table = ("""
DROP TABLE IF EXISTS public.staging_temperatures;
CREATE TABLE public.staging_temperatures (
    dt TIMESTAMP WITHOUT TIME ZONE,
    average_temperature DOUBLE PRECISION,
    average_temperature_uncertainty DOUBLE PRECISION,
    city TEXT,
    country TEXT,
    latitude TEXT,
    longitude TEXT
    );
""")

create_staging_demographic_table = ("""
DROP TABLE IF EXISTS public.staging_demographics;
CREATE TABLE public.staging_demographics (
    city VARCHAR(256),
    state VARCHAR(100),
    median_age DOUBLE PRECISION,
    male_population INT,
    female_population INT,
    total_population INT,
    number_of_veterans INT,
    foreign_born INT,
    average_household_size DOUBLE PRECISION,
    state_code VARCHAR(50),
    race VARCHAR(100),
    count INT
    );
""")

create_staging_immigration_table = ("""
DROP TABLE IF EXISTS public.staging_immigration;
CREATE TABLE public.staging_immigration (
    cicid DOUBLE PRECISION,
    origin_country_code TEXT,
    age DOUBLE PRECISION,
    arrival_date DATE,
    departure_date DATE,
    depdate DOUBLE PRECISION,
    arrdate DOUBLE PRECISION,
    port_code TEXT,
    mode TEXT,
    gender TEXT, 
    visa_category TEXT, 
    visatype TEXT
    )
""")

# STAGING TABLES

staging_airport_copy = ("""
    COPY staging_airports 
    FROM {} 
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
""").format(AIRPORT_DATA, IAM_ROLE)


staging_demographic_copy = ("""
    COPY staging_demographics 
    FROM {} 
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
""").format(DEMO_DATA, IAM_ROLE)


staging_port_codes_copy = ("""
    COPY staging_ports 
    FROM {} 
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
""").format(PORT_DATA, IAM_ROLE)


staging_country_codes_copy = ("""
    COPY staging_countries 
    FROM {} 
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
""").format(COUNTRY_DATA, IAM_ROLE)


staging_temperature_copy = ("""
    COPY staging_temperatures
    FROM {} 
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
""").format(TEMPERATURE_DATA, IAM_ROLE)


staging_immigration_copy = ("""
    COPY staging_immigration 
    FROM {} 
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
""").format(IMMIGRATION_DATA, IAM_ROLE)


# Create Countries Table
create_countries_table = """
DROP TABLE IF EXISTS public.dim_countries CASCADE;
CREATE TABLE public.dim_countries (
    country_id bigint GENERATED ALWAYS AS IDENTITY,
    country_code varchar(3) NOT NULL UNIQUE,
    country varchar(256) NOT NULL UNIQUE,
    average_temperature NUMERIC(16,3) NULL,
    PRIMARY KEY(country_id)
);
"""

# Extract countries from staging immigration data
extract_countries = """
INSERT INTO public.dim_countries (country_code, country, average_temperature)
SELECT DISTINCT c.country_code, c.country, t.average_temperature
FROM public.staging_immigration i
INNER JOIN public.staging_countries c ON i.origin_country_code = c.country_code
LEFT JOIN public.staging_temperatures t ON UPPER(c.country) = UPPER(t.country)
ORDER BY c.country
"""


# Create Ports Dimension Table
create_ports_table = """
DROP TABLE IF EXISTS public.dim_ports CASCADE;
CREATE TABLE public.dim_ports (
    port_id BIGINT GENERATED ALWAYS AS IDENTITY,
    port_code VARCHAR(3) UNIQUE,
    port_city VARCHAR(256),
    port_state VARCHAR(50),
    average_temperature NUMERIC(16,3) NULL,
    PRIMARY KEY(port_id)
);
"""


# Extract ports from staging immigration data
extract_ports = """
INSERT INTO public.dim_ports (port_code, port_city, port_state, average_temperature)
SELECT DISTINCT p.port_code, p.city, p.state, t.average_temperature
FROM public.staging_immigration i
INNER JOIN public.staging_ports p ON i.port_code = p.port_code
LEFT JOIN public.staging_temperatures t ON UPPER(p.city) = UPPER(t.city)
ORDER BY p.port_code
"""


# Create Airports Dimension Table
create_airports_table = """
DROP TABLE IF EXISTS public.dim_airports CASCADE;
CREATE TABLE public.dim_airports (
    airport_id BIGINT GENERATED ALWAYS AS IDENTITY,
    port_id BIGINT UNIQUE,
    airport_type VARCHAR(256),
    airport_name VARCHAR(256),
    elevation_ft INT,
    municipality VARCHAR(256),
    gps_code VARCHAR(256),
    iata_code VARCHAR(256),
    local_code VARCHAR(256),
    coordinates VARCHAR(256),
PRIMARY KEY(airport_id),
CONSTRAINT fk_port
FOREIGN KEY(port_id) REFERENCES dim_ports(port_id)
);
"""


# Extract airports data from staging airports 
extract_airports = """
INSERT INTO public.dim_airports (port_id, airport_type, airport_name, elevation_ft, municipality, gps_code, local_code, coordinates)
SELECT p.port_id, a.type, a.name, a.elevation_ft, a.municipality,
a.gps_code, a.local_code, a.coordinates
FROM public.staging_airports a
INNER JOIN public.dim_ports p ON a.ident = p.port_code
ORDER BY p.port_code
"""


# Create demographics dimension table
create_demographics_table = """
DROP TABLE IF EXISTS public.dim_demographics CASCADE;
CREATE TABLE public.dim_demographics (
    demographics_id BIGINT GENERATED ALWAYS AS IDENTITY,
    port_id BIGINT,
    median_age numeric(18,2),
    male_population int,
    female_population int,
    total_population bigint,
    number_of_veterans int,
    foreign_born int,
    avg_household_size numeric(18,2),
    race varchar(100),
    demo_count int,
UNIQUE (port_id, race),
PRIMARY KEY(demographics_id),
CONSTRAINT fk_port
FOREIGN KEY(port_id) REFERENCES dim_ports(port_id)
);
"""

# Extract demographics from staging data
extract_demographics = """
INSERT INTO public.dim_demographics (port_id, median_age, male_population, female_population, total_population, number_of_veterans,foreign_born,avg_household_size, race, demo_count)
SELECT DISTINCT p.port_id, d.median_age, d.male_population, d.female_population, d.total_population,
d.number_of_veterans, d.foreign_born, d.average_household_size, d.race, d.count
FROM public.dim_ports p
INNER JOIN public.staging_demographics d 
ON UPPER(p.port_city) = UPPER(d.city) AND UPPER(p.port_state) = UPPER(d.state_code)
WHERE EXISTS (SELECT port_code FROM public.staging_immigration i 
WHERE p.port_code = i.port_code)
"""


 # Create time dimension table
create_time_table = """
DROP TABLE IF EXISTS public.dim_time CASCADE;
CREATE TABLE public.dim_time (
sas_timestamp INT NOT NULL UNIQUE,
year INT NOT NULL,
month INT NOT NULL,
day INT NOT NULL,
week INT NOT NULL,
day_of_week INT NOT NULL,
quarter INT NOT NULL,
PRIMARY KEY (sas_timestamp)
);
"""

# Extract time dimension data from staging immigration arrival and departure dates
extract_time_data = """
INSERT INTO public.dim_time (sas_timestamp, year, month, day, quarter, week, day_of_week)
SELECT ts, 
    date_part('year', t1.sas_date) as year,
    date_part('month', t1.sas_date) as month,
    date_part('day', t1.sas_date) as day, 
    date_part('quarter', t1.sas_date) as quarter,
    date_part('week', t1.sas_date) as week,
    date_part('dow', t1.sas_date) as day_of_week
FROM
(SELECT DISTINCT arrdate as ts, TIMESTAMP '1960-01-01 00:00:00 +00:00' + (arrdate * INTERVAL '1 day') as sas_date
FROM staging_immigration
UNION
SELECT DISTINCT depdate as ts, TIMESTAMP '1960-01-01 00:00:00 +00:00' + (depdate * INTERVAL '1 day') as sas_date
FROM staging_immigration
WHERE depdate IS NOT NULL
) t1
"""

# Create fact immigration table
create_fact_immigration_table = """
DROP TABLE IF EXISTS public.fact_immigration CASCADE;
CREATE TABLE public.fact_immigration (
    immigration_id BIGINT GENERATED ALWAYS AS IDENTITY,
    country_id BIGINT,
    port_id BIGINT,
    age int,
    travel_mode varchar(100),
    visa_category varchar(100),
    visa_type varchar(100),
    gender varchar(10),
    arrdate int NOT NULL,
    depdate int NULL,
PRIMARY KEY (immigration_id),
CONSTRAINT fk_port FOREIGN KEY(port_id) REFERENCES dim_ports(port_id),
CONSTRAINT fk_country FOREIGN KEY(country_id) REFERENCES dim_countries(country_id),
CONSTRAINT fk_arrdate FOREIGN KEY(arrdate) REFERENCES dim_time(sas_timestamp),
CONSTRAINT fk_depdate FOREIGN KEY(depdate) REFERENCES dim_time(sas_timestamp)
);
"""

# Extract immigration data from staging to fact table
extract_immigration_data = """
INSERT INTO public.fact_immigration (country_id, port_id, age, travel_mode, visa_category, visa_type, gender, arrdate,depdate)
SELECT c.country_id, p.port_id, i.age, i.mode, i.visa_category, i.visatype, i.gender, i.arrdate, i.depdate
FROM public.staging_immigration i
INNER JOIN public.dim_countries c ON i.origin_country_code = c.country_code
INNER JOIN public.dim_ports p ON i.port_code = p.port_code
"""


# Staging count data quality check
# Ensure items are in staging table
staging_count_data_quality_check = """
SELECT COUNT(*) FROM staging_immigration
"""

# Staging to fact table data quality check
# Ensures that qualifying staging items make it to fact table
staging_to_fact_data_quality_check = """
SELECT s.stagingCount - f.factCount FROM
(SELECT COUNT(i.*) as stagingCount
FROM public.staging_immigration i
INNER JOIN public.dim_countries c ON i.origin_country_code = c.country_code
INNER JOIN public.dim_ports p ON i.port_code = p.port_code) s
CROSS JOIN (SELECT COUNT(*) as factCount FROM fact_immigration i INNER JOIN dim_time t ON i.arrdate = t.sas_timestamp 
WHERE t.year = 2016) f
"""


## Testing Data
imigrants_by_country = """
SELECT c.country, COUNT(*) FROM fact_immigration i
INNER JOIN dim_countries c ON i.country_id = c.country_id
INNER JOIN dim_time t ON i.arrdate=t.sas_timestamp
WHERE t.year=2016 AND t.month=4
GROUP BY c.country
ORDER BY count DESC
LIMIT 10;
"""

arrival_days = """
SELECT t.day_of_week,COUNT(*) as count
FROM fact_immigration i
INNER JOIN dim_ports p ON i.port_id = p.port_id
INNER JOIN dim_time t ON i.arrdate=t.sas_timestamp
WHERE t.year=2016 AND t.month=4
GROUP BY t.day_of_week
ORDER BY t.day_of_week
"""
