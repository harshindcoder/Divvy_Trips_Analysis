-- Create database and table
CREATE DATABASE IF NOT EXISTS Divvy_Trips_Database;
USE Divvy_Trips_Database;

CREATE TABLE IF NOT EXISTS divvy_trips (
    rental_id BIGINT,
    local_start_time DATETIME,
    local_end_time DATETIME,
    bike_id INT,
    duration_in_seconds FLOAT,
    start_station_id INT,
    start_station_name VARCHAR(255),
    end_station_id INT,
    end_station_name VARCHAR(255),
    user_type VARCHAR(50),
    member_gender VARCHAR(50),
    member_birth_year INT
);


LOAD DATA LOCAL INFILE '/Users/harsh/Desktop/Divvy_Trips_Analysis/data/Divvy_Trips_2018_Q1.csv'
INTO TABLE divvy_trips
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(rental_id, local_start_time, local_end_time, bike_id, duration_in_seconds, 
 start_station_id, start_station_name, end_station_id, end_station_name, 
 user_type, member_gender, member_birth_year)
SET
start_station_name = NULLIF(start_station_name, ''),
end_station_name = NULLIF(end_station_name, ''),
user_type = NULLIF(user_type, ''),
member_gender = NULLIF(member_gender, ''),
member_birth_year = NULLIF(member_birth_year, '');

LOAD DATA LOCAL INFILE '/Users/harsh/Desktop/Divvy_Trips_Analysis/data/Divvy_Trips_2018_Q2.csv'
INTO TABLE divvy_trips
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(rental_id, local_start_time, local_end_time, bike_id, duration_in_seconds, 
 start_station_id, start_station_name, end_station_id, end_station_name, 
 user_type, member_gender, member_birth_year)
SET
start_station_name = NULLIF(start_station_name, ''),
end_station_name = NULLIF(end_station_name, ''),
user_type = NULLIF(user_type, ''),
member_gender = NULLIF(member_gender, ''),
member_birth_year = NULLIF(member_birth_year, '');

LOAD DATA LOCAL INFILE '/Users/harsh/Desktop/Divvy_Trips_Analysis/data/Divvy_Trips_2018_Q3.csv'
INTO TABLE divvy_trips
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(rental_id, local_start_time, local_end_time, bike_id, duration_in_seconds, 
 start_station_id, start_station_name, end_station_id, end_station_name, 
 user_type, member_gender, member_birth_year)
SET
start_station_name = NULLIF(start_station_name, ''),
end_station_name = NULLIF(end_station_name, ''),
user_type = NULLIF(user_type, ''),
member_gender = NULLIF(member_gender, ''),
member_birth_year = NULLIF(member_birth_year, '');

LOAD DATA LOCAL INFILE '/Users/harsh/Desktop/Divvy_Trips_Analysis/data/Divvy_Trips_2018_Q4.csv'
INTO TABLE divvy_trips
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(rental_id, local_start_time, local_end_time, bike_id, duration_in_seconds, 
 start_station_id, start_station_name, end_station_id, end_station_name, 
 user_type, member_gender, member_birth_year)
SET
start_station_name = NULLIF(start_station_name, ''),
end_station_name = NULLIF(end_station_name, ''),
user_type = NULLIF(user_type, ''),
member_gender = NULLIF(member_gender, ''),
member_birth_year = NULLIF(member_birth_year, '');

SELECT 'rental_id' AS column_name, COUNT(*) - COUNT(rental_id) AS null_count FROM divvy_trips
UNION ALL
SELECT 'local_start_time', COUNT(*) - COUNT(local_start_time) FROM divvy_trips
UNION ALL
SELECT 'local_end_time', COUNT(*) - COUNT(local_end_time) FROM divvy_trips
UNION ALL
SELECT 'bike_id', COUNT(*) - COUNT(bike_id) FROM divvy_trips
UNION ALL
SELECT 'duration_in_seconds', COUNT(*) - COUNT(duration_in_seconds) FROM divvy_trips
UNION ALL
SELECT 'start_station_id', COUNT(*) - COUNT(start_station_id) FROM divvy_trips
UNION ALL
SELECT 'start_station_name', COUNT(*) - COUNT(start_station_name) FROM divvy_trips
UNION ALL
SELECT 'end_station_id', COUNT(*) - COUNT(end_station_id) FROM divvy_trips
UNION ALL
SELECT 'end_station_name', COUNT(*) - COUNT(end_station_name) FROM divvy_trips
UNION ALL
SELECT 'user_type', COUNT(*) - COUNT(user_type) FROM divvy_trips
UNION ALL
SELECT 'member_gender', COUNT(*) - COUNT(member_gender) FROM divvy_trips
UNION ALL
SELECT 'member_birth_year', COUNT(*) - COUNT(member_birth_year) FROM divvy_trips;

-- Create silver_divvy_trips table
CREATE TABLE IF NOT EXISTS silver_divvy_trips AS
SELECT 
    rental_id,
    local_start_time,
    local_end_time,
    bike_id,
    duration_in_seconds,
    CASE 
        WHEN start_station_name = 'Lincoln Ave & Sunnyside Ave' THEN 665
        ELSE start_station_id
    END AS start_station_id,
    start_station_name,
    CASE 
        WHEN end_station_name = 'Lincoln Ave & Sunnyside Ave' THEN 665
        ELSE end_station_id
    END AS end_station_id,
    end_station_name,
    user_type,
    COALESCE(member_gender, 'unknown') AS member_gender,
    COALESCE(member_birth_year, 9999) AS member_birth_year
FROM bronze_divvy_trips;


-- Check for NULL values in silver_divvy_trips
SELECT 'rental_id' AS column_name, COUNT(*) - COUNT(rental_id) AS null_count FROM silver_divvy_trips
UNION ALL
SELECT 'local_start_time', COUNT(*) - COUNT(local_start_time) FROM silver_divvy_trips
UNION ALL
SELECT 'local_end_time', COUNT(*) - COUNT(local_end_time) FROM silver_divvy_trips
UNION ALL
SELECT 'bike_id', COUNT(*) - COUNT(bike_id) FROM silver_divvy_trips
UNION ALL
SELECT 'duration_in_seconds', COUNT(*) - COUNT(duration_in_seconds) FROM silver_divvy_trips
UNION ALL
SELECT 'start_station_id', COUNT(*) - COUNT(start_station_id) FROM silver_divvy_trips
UNION ALL
SELECT 'start_station_name', COUNT(*) - COUNT(start_station_name) FROM silver_divvy_trips
UNION ALL
SELECT 'end_station_id', COUNT(*) - COUNT(end_station_id) FROM silver_divvy_trips
UNION ALL
SELECT 'end_station_name', COUNT(*) - COUNT(end_station_name) FROM silver_divvy_trips
UNION ALL
SELECT 'user_type', COUNT(*) - COUNT(user_type) FROM silver_divvy_trips
UNION ALL
SELECT 'member_gender', COUNT(*) - COUNT(member_gender) FROM silver_divvy_trips
UNION ALL
SELECT 'member_birth_year', COUNT(*) - COUNT(member_birth_year) FROM silver_divvy_trips;


CREATE TABLE station_location (
    intersection VARCHAR(255) NOT NULL,
    latitude DECIMAL(9,6) NULL,
    longitude DECIMAL(9,6) NULL
);

LOAD DATA LOCAL INFILE '/Users/harsh/Desktop/Divvy_Trips_Analysis/data/station_coordinates.csv'
INTO TABLE station_location
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(intersection, latitude, longitude)
SET 
latitude = NULLIF(latitude, ''),
longitude = NULLIF(longitude, '');



-- Add founded coordinates
INSERT INTO station_location (intersection, latitude, longitude) VALUES 
('Loomis St & Taylor St (*)', 41.869417, -87.661206),
('Washtenaw Ave & Ogden Ave (*)', 41.856268, -87.693488),
('MLK Jr Dr & 56th St (*)', 41.793242, -87.615405),
('Orleans St & Elm St (*)', 41.903222, -87.637313);

INSERT INTO station_location (intersection, latitude, longitude)
VALUES ('BBB ~ Divvy Parts Testing', NULL, NULL);

UPDATE station_location
SET latitude = 42.0325958, longitude = -87.6792508
WHERE intersection = 'Chicago Ave & Washington St';


-- Some fake company offics and repair center locations

UPDATE station_location 
SET latitude = 41.888888, longitude = -87.654321 
WHERE intersection = 'DIVVY Map Frame B/C Station';

UPDATE station_location 
SET latitude = 41.876543, longitude = -87.667890 
WHERE intersection = 'BBB ~ Divvy Parts Testing';

UPDATE station_location 
SET latitude = 41.895432, longitude = -87.678901 
WHERE intersection = 'TS ~ DIVVY PARTS TESTING';

UPDATE station_location 
SET latitude = 41.882200, longitude = -87.620000 
WHERE intersection = 'DIVVY CASSETTE REPAIR MOBILE STATION';





-- VIew created
CREATE TABLE IF NOT EXISTS gold_divvy_trips AS
SELECT 
    DISTINCT
    t.rental_id AS ride_id,
    t.bike_id AS rideable_type,
    t.local_start_time AS started_at,
    t.local_end_time AS ended_at,
    t.start_station_id,
    t.start_station_name,
    s1.latitude AS start_lat,
    s1.longitude AS start_lng,
    t.end_station_id,
    t.end_station_name,
    s2.latitude AS end_lat,
    s2.longitude AS end_lng,

    -- Round trip
    IF(t.end_station_name = t.start_station_name, 1, 0) AS round_trip,

    -- Date/time breakdown
    DATE(t.local_start_time) AS date,
    DATE_FORMAT(t.local_start_time, '%b') AS month,
    YEAR(t.local_start_time) AS year,
    DATE_FORMAT(t.local_start_time, '%a') AS day_of_week,
    DATE_FORMAT(t.local_start_time, '%H') AS hour_of_day,

    -- Times
    TIME(t.local_start_time) AS start_time,
    TIME(t.local_end_time) AS end_time,

    -- Ride duration in minutes
    t.duration_in_seconds / 60 AS ride_length_mins,

    -- Route string
    CONCAT(t.start_station_name, ' to ', t.end_station_name) AS route,

    -- User type
    t.user_type AS member_casual

FROM silver_divvy_trips t

-- Join for start station location
LEFT JOIN station_location s1 
    ON t.start_station_name = s1.intersection

-- Join for end station location
LEFT JOIN station_location s2 
    ON t.end_station_name = s2.intersection

-- Filters
WHERE
    t.start_station_name IS NOT NULL
    AND t.end_station_name IS NOT NULL
    AND t.start_station_id IS NOT NULL
    AND t.end_station_id IS NOT NULL
    AND t.duration_in_seconds > 60
    AND t.duration_in_seconds < 86400

ORDER BY date, start_time;

