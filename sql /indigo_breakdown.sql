CREATE TABLE india_carrier_monthly (
    type VARCHAR(50),
    airline VARCHAR(50),
    year INT,
    month INT,
    aircraft_number INT,
    aircraft_hours FLOAT,
    aircraft_km FLOAT,
    passenger_number NUMERIC,
    passenger_km FLOAT,
    seat_km FLOAT,
    passenger_load_factor FLOAT,
    freight FLOAT,
    mail FLOAT,
    total_cargo FLOAT,
    passenger_tonne_km FLOAT,
    mail_tonne_km FLOAT,
    freight_tonne_km FLOAT,
    total_tonne_km FLOAT,
    available_tonne_km FLOAT,
    weight_load_factor FLOAT
);

COPY india_carrier_monthly
FROM 'C:\Program Files\PostgreSQL\18\carrier.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

CREATE TABLE indiGo_monthly AS
SELECT *
FROM india_carrier_monthly
WHERE airline ILIKE '%Indigo%' OR airline ILIKE '%6E%';

CREATE TABLE india_city_traffic (
    year INT,
    month INT,
    city1 VARCHAR(50),
    city2 VARCHAR(50),
    pax_to_city2 NUMERIC,
    pax_from_city2 NUMERIC,
    freight_to_city2 NUMERIC,
    freight_from_city2 NUMERIC,
    mail_to_city2 NUMERIC,
    mail_from_city2 NUMERIC
);

COPY india_city_traffic
FROM 'C:\Program Files\PostgreSQL\18\city.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

CREATE TABLE airlines_monthly_wide (
    year INT,
    airline VARCHAR(50),
    january NUMERIC,
    february NUMERIC,
    march NUMERIC,
    april NUMERIC,
    may NUMERIC,
    june NUMERIC,
    july NUMERIC,
    august NUMERIC,
    september NUMERIC,
    october NUMERIC,
    november NUMERIC,
    december NUMERIC
);

COPY airlines_monthly_wide
FROM 'C:\Program Files\PostgreSQL\18\RS_Session_260_AU_1927_1.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL 'NA');

CREATE TABLE airlines_monthly_long AS
SELECT year, airline, 'January' AS month, january AS value FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'February', february FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'March', march FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'April', april FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'May', may FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'June', june FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'July', july FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'August', august FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'September', september FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'October', october FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'November', november FROM airlines_monthly_wide
UNION ALL SELECT year, airline, 'December', december FROM airlines_monthly_wide;

-- Filter Indigo only
CREATE TABLE indiGo_third AS
SELECT *
FROM airlines_monthly_long
WHERE airline ILIKE '%Indigo%' OR airline ILIKE '%6E%';

-- Convert month names → integers for joins
CREATE OR REPLACE VIEW indiGo_third_long AS
SELECT 
    year,
    airline,
    CASE month
        WHEN 'January' THEN 1
        WHEN 'February' THEN 2
        WHEN 'March' THEN 3
        WHEN 'April' THEN 4
        WHEN 'May' THEN 5
        WHEN 'June' THEN 6
        WHEN 'July' THEN 7
        WHEN 'August' THEN 8
        WHEN 'September' THEN 9
        WHEN 'October' THEN 10
        WHEN 'November' THEN 11
        WHEN 'December' THEN 12
    END AS month_int,
    value AS third_csv_value
FROM indiGo_third;

CREATE OR REPLACE VIEW indiGo_monthly_changes AS
SELECT 
    year,
    month,
    passenger_number AS total_passengers,
    total_cargo,
    passenger_load_factor,
    CASE 
        WHEN LAG(passenger_number) OVER (ORDER BY year, month) IS NULL 
             OR LAG(passenger_number) OVER (ORDER BY year, month) = 0
        THEN NULL
        ELSE ROUND(
            (100.0 * (passenger_number - LAG(passenger_number) OVER (ORDER BY year, month)) /
            NULLIF(LAG(passenger_number) OVER (ORDER BY year, month),0))::NUMERIC, 2
        )
    END AS pct_change_passengers,
    CASE 
        WHEN LAG(total_cargo) OVER (ORDER BY year, month) IS NULL 
             OR LAG(total_cargo) OVER (ORDER BY year, month) = 0
        THEN NULL
        ELSE ROUND(
            (100.0 * (total_cargo - LAG(total_cargo) OVER (ORDER BY year, month)) /
            NULLIF(LAG(total_cargo) OVER (ORDER BY year, month),0))::NUMERIC, 2
        )
    END AS pct_change_cargo,
    CASE 
        WHEN LAG(passenger_load_factor) OVER (ORDER BY year, month) IS NULL 
             OR LAG(passenger_load_factor) OVER (ORDER BY year, month) = 0
        THEN NULL
        ELSE ROUND(
            (100.0 * (passenger_load_factor - LAG(passenger_load_factor) OVER (ORDER BY year, month)) /
            NULLIF(LAG(passenger_load_factor) OVER (ORDER BY year, month),0))::NUMERIC, 2
        )
    END AS pct_change_load
FROM indiGo_monthly
ORDER BY year, month;

CREATE OR REPLACE VIEW city_proportions AS
SELECT 
    year,
    month,
    city1,
    city2,
    CASE 
        WHEN SUM(pax_to_city2 + pax_from_city2) OVER (PARTITION BY year, month) = 0 
        THEN NULL
        ELSE (pax_to_city2 + pax_from_city2) / 
             SUM(pax_to_city2 + pax_from_city2) OVER (PARTITION BY year, month)
    END AS proportion_pax
FROM india_city_traffic;

CREATE TABLE indiGo_crisis_analysis AS
SELECT m.year,
       m.month,
       m.total_passengers,
       m.pct_change_passengers,
       m.total_cargo,
       m.pct_change_cargo,
       m.passenger_load_factor,
       m.pct_change_load,
       t.third_csv_value,
       c.city1,
       c.city2,
       c.proportion_pax
FROM indiGo_monthly_changes m
LEFT JOIN indiGo_third_long t
  ON m.year = t.year AND m.month = t.month_int
LEFT JOIN city_proportions c
  ON m.year = c.year AND m.month = c.month
ORDER BY m.year, m.month, c.city1, c.city2;

-- Identify months with biggest passenger drops:
SELECT year, month, total_passengers, pct_change_passengers
FROM indiGo_crisis_analysis
WHERE pct_change_passengers < 0
ORDER BY pct_change_passengers ASC;

-- Look at cargo or load factor drops:
SELECT year, month, total_cargo, pct_change_cargo, passenger_load_factor, pct_change_load
FROM indiGo_crisis_analysis
WHERE pct_change_cargo < 0 OR pct_change_load < 0
ORDER BY year, month;

-- Examine city traffic proportions to see if cancellations were concentrated on certain routes:
SELECT year, month, city1, city2, proportion_pax
FROM indiGo_crisis_analysis
WHERE proportion_pax > 0.05 -- only major routes
ORDER BY proportion_pax DESC;

-- Identify Crisis Months
-- biggest drops in passengers, cargo, or load factor
-- Top drops in passengers
SELECT year, month, total_passengers, pct_change_passengers
FROM indiGo_crisis_analysis
WHERE pct_change_passengers < 0
ORDER BY pct_change_passengers ASC
LIMIT 5;

-- Top drops in cargo
SELECT year, month, total_cargo, pct_change_cargo
FROM indiGo_crisis_analysis
WHERE pct_change_cargo < 0
ORDER BY pct_change_cargo ASC
LIMIT 5;

-- Load factor drops
SELECT year, month, passenger_load_factor, pct_change_load
FROM indiGo_crisis_analysis
WHERE pct_change_load < 0
ORDER BY pct_change_load ASC
LIMIT 5;

-- Route-Level Impact
-- routes with largest passenger proportions during crisis months:
SELECT year, month, city1, city2, proportion_pax
FROM indiGo_crisis_analysis
WHERE (year, month) IN (
    SELECT year, month
    FROM indiGo_crisis_analysis
    WHERE pct_change_passengers < 0
)
ORDER BY proportion_pax DESC
LIMIT 20;

-- Compare Monthly Revenue
SELECT year, month, total_passengers, pct_change_passengers, third_csv_value
FROM indiGo_crisis_analysis
WHERE pct_change_passengers < 0
ORDER BY pct_change_passengers ASC;

-- Combined Crisis Summary
CREATE OR REPLACE VIEW indiGo_crisis_summary AS
SELECT 
    year, 
    month,
    total_passengers,
    pct_change_passengers,
    total_cargo,
    pct_change_cargo,
    passenger_load_factor,
    pct_change_load,
    third_csv_value,
    ARRAY_AGG(route ORDER BY max_proportion DESC) AS top_routes
FROM (
    SELECT DISTINCT 
        year, month, total_passengers, pct_change_passengers, 
        total_cargo, pct_change_cargo, passenger_load_factor, pct_change_load, third_csv_value,
        city1 || '-' || city2 AS route,
        proportion_pax AS max_proportion
    FROM indiGo_crisis_analysis
    WHERE pct_change_passengers < 0
) t
GROUP BY year, month, total_passengers, pct_change_passengers, total_cargo, pct_change_cargo, passenger_load_factor, pct_change_load, third_csv_value
ORDER BY pct_change_passengers ASC;

-- Final Crisis Report (Top 5 Routes)
CREATE OR REPLACE VIEW indiGo_crisis_report AS
SELECT 
    year,
    month,
    total_passengers,
    pct_change_passengers,
    total_cargo,
    pct_change_cargo,
    passenger_load_factor,
    pct_change_load,
    third_csv_value,
    top_routes[1:5] AS top_5_routes  -- first 5 routes
FROM indiGo_crisis_summary
ORDER BY pct_change_passengers ASC;

SELECT *
FROM indiGo_crisis_report;

-- Worst month only
SELECT *
FROM indiGo_crisis_report
WHERE pct_change_passengers = (SELECT MIN(pct_change_passengers) FROM indiGo_crisis_report);

SELECT 
    year,
    month,
    total_passengers,
    pct_change_passengers,
    total_cargo,
    pct_change_cargo,
    passenger_load_factor,
    pct_change_load,
    third_csv_value AS revenue_metric,
    ARRAY_TO_STRING(top_5_routes, ', ') AS top_5_routes
FROM indiGo_crisis_report
ORDER BY pct_change_passengers ASC;

-- Complete
CREATE OR REPLACE VIEW indiGo_single_source AS
SELECT 
    m.year,
    m.month,
    m.total_passengers,
    m.pct_change_passengers,
    m.total_cargo,
    m.pct_change_cargo,
    m.passenger_load_factor,
    m.pct_change_load,
    t.third_csv_value AS revenue_metric,
    ARRAY_AGG(c.city1 || '-' || c.city2 ORDER BY c.proportion_pax DESC) AS top_routes
FROM indiGo_monthly_changes m
LEFT JOIN indiGo_third_long t
    ON m.year = t.year AND m.month = t.month_int
LEFT JOIN city_proportions c
    ON m.year = c.year AND m.month = c.month
GROUP BY 
    m.year, m.month, m.total_passengers, m.pct_change_passengers, 
    m.total_cargo, m.pct_change_cargo, m.passenger_load_factor, m.pct_change_load, t.third_csv_value
ORDER BY pct_change_passengers ASC;

-- csv formatting
CREATE OR REPLACE VIEW indiGo_dashboard_ready AS
SELECT 
    year,
    month,
    total_passengers,
    pct_change_passengers,
    total_cargo,
    pct_change_cargo,
    passenger_load_factor,
    pct_change_load,
    revenue_metric,
    ARRAY_TO_STRING(top_routes[1:5], ', ') AS top_5_routes
FROM indiGo_single_source
ORDER BY pct_change_passengers ASC;

