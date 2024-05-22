use new_scheme;

CREATE TABLE dim_energy (
	sl_no INT PRIMARY KEY,
    iso_code VARCHAR(3),
    country VARCHAR(255),
    year INT
);

CREATE TABLE fact_energy(
    sl_no INT PRIMARY KEY,
    iso_code VARCHAR(3),
    coal_prod_change_pct FLOAT,
    coal_prod_change_twh FLOAT,
    gas_prod_change_pct FLOAT,
    gas_prod_change_twh FLOAT,
    oil_prod_change_pct FLOAT,
    oil_prod_change_twh FLOAT,
    energy_cons_change_pct FLOAT,
    energy_cons_change_twh FLOAT,
    biofuel_share_elec FLOAT,
    biofuel_elec_per_capita FLOAT,
    biofuel_cons_change_pct FLOAT,
    biofuel_share_energy FLOAT,
    biofuel_cons_change_twh FLOAT,
    biofuel_consumption FLOAT,
    biofuel_cons_per_capita FLOAT,
    carbon_intensity_elec FLOAT,
    coal_share_elec FLOAT,
    coal_cons_change_pct FLOAT,
    coal_share_energy FLOAT,
    coal_cons_change_twh FLOAT,
    coal_consumption FLOAT,
    coal_elec_per_capita FLOAT,
    coal_cons_per_capita FLOAT,
    coal_production FLOAT,
    coal_prod_per_capita FLOAT,
    electricity_demand FLOAT,
    biofuel_electricity FLOAT,
    coal_electricity FLOAT,
    fossil_electricity FLOAT,
    gas_electricity FLOAT,
    hydro_electricity FLOAT,
    nuclear_electricity FLOAT,
    oil_electricity FLOAT,
    other_renewable_exc_biofuel_electricity FLOAT,
    other_renewable_electricity FLOAT,
    renewables_electricity FLOAT,
    solar_electricity FLOAT,
    wind_electricity FLOAT,
    electricity_generation FLOAT,
    greenhouse_gas_emissions FLOAT,
    energy_per_gdp FLOAT,
    energy_per_capita FLOAT,
    fossil_cons_change_pct FLOAT,
    fossil_share_energy FLOAT,
    fossil_cons_change_twh FLOAT,
    fossil_fuel_consumption FLOAT,
    fossil_energy_per_capita FLOAT,
    fossil_cons_per_capita FLOAT,
    fossil_share_elec FLOAT,
    gdp FLOAT,
    gas_share_elec FLOAT,
    gas_cons_change_pct FLOAT,
    gas_share_energy FLOAT,
    gas_cons_change_twh FLOAT,
    gas_consumption FLOAT,
    gas_elec_per_capita FLOAT,
    gas_energy_per_capita FLOAT,
    gas_production FLOAT,
    gas_prod_per_capita FLOAT,
    hydro_share_elec FLOAT,
    hydro_cons_change_pct FLOAT,
    hydro_share_energy FLOAT,
    hydro_cons_change_twh FLOAT,
    hydro_consumption FLOAT,
    hydro_elec_per_capita FLOAT,
    hydro_energy_per_capita FLOAT,
    low_carbon_share_elec FLOAT,
    low_carbon_electricity FLOAT,
    low_carbon_elec_per_capita FLOAT,
    low_carbon_cons_change_pct FLOAT,
    low_carbon_share_energy FLOAT,
    low_carbon_cons_change_twh FLOAT,
    low_carbon_consumption FLOAT,
    low_carbon_energy_per_capita FLOAT,
    net_elec_imports_share_demand FLOAT,
    net_elec_imports FLOAT,
    nuclear_share_elec FLOAT,
    nuclear_cons_change_pct FLOAT,
    nuclear_share_energy FLOAT,
    nuclear_cons_change_twh FLOAT,
    nuclear_consumption FLOAT,
    nuclear_elec_per_capita FLOAT,
    nuclear_energy_per_capita FLOAT,
    oil_share_elec FLOAT,
    oil_cons_change_pct FLOAT,
    oil_share_energy FLOAT,
    oil_cons_change_twh FLOAT,
    oil_consumption FLOAT,
    oil_elec_per_capita FLOAT,
    oil_energy_per_capita FLOAT,
    oil_production FLOAT,
    oil_prod_per_capita FLOAT,
    other_renewables_elec_per_capita_exc_biofuel FLOAT,
    other_renewables_elec_per_capita FLOAT,
    other_renewables_cons_change_pct FLOAT,
    other_renewables_share_energy FLOAT,
    other_renewables_cons_change_twh FLOAT,
    other_renewable_consumption FLOAT,
    other_renewables_share_elec_exc_biofuel FLOAT,
    other_renewables_share_elec FLOAT,
    other_renewables_energy_per_capita FLOAT,
    per_capita_electricity FLOAT,
    population FLOAT,
    primary_energy_consumption FLOAT,
    renewables_elec_per_capita FLOAT,
    renewables_share_elec FLOAT,
    renewables_cons_change_pct FLOAT,
    renewables_share_energy FLOAT,
    renewables_cons_change_twh FLOAT,
    renewables_consumption FLOAT,
    renewables_energy_per_capita FLOAT,
    solar_share_elec FLOAT,
    solar_cons_change_pct FLOAT,
    solar_share_energy FLOAT,
    solar_cons_change_twh FLOAT,
    solar_consumption FLOAT,
    solar_elec_per_capita FLOAT,
    solar_energy_per_capita FLOAT,
    wind_share_elec FLOAT,
    wind_cons_change_pct FLOAT,
    wind_share_energy FLOAT,
    wind_cons_change_twh FLOAT,
    wind_consumption FLOAT,
    wind_elec_per_capita FLOAT,
    wind_energy_per_capita FLOAT,
    FOREIGN KEY (sl_no) REFERENCES dim_energy (sl_no)
);
  select count(*)
  from final_energy;
  
  select * from final_energy;
  select* from dim_energy;
  select * from fact_energy;
  
   SELECT *
FROM dim_energy AS d
INNER JOIN fact_energy AS f ON d.iso_code = f.iso_code;

SELECT *
FROM dim_energy AS d
LEFT JOIN fact_energy AS f ON d.iso_code = f.iso_code;

SELECT *
FROM dim_energy AS d
LEFT JOIN fact_energy AS f ON d.sl_no = f.sl_no;
  
   SELECT *
FROM dim_energy AS d
INNER JOIN fact_energy AS f ON d.sl_no = f.sl_no;

#time series analysis
select * from final_energy;

#total energy consumption over the years for each country
SELECT
    country,
    year,
    SUM(energy_cons_change_twh) OVER (PARTITION BY country ORDER BY year) AS total_energy_consumption
FROM
    final_energy
ORDER BY
    country, year;
    
  #moving averages to detect seasonal patterns  
    SELECT 
    year,
    energy_cons_change_twh,
    AVG(energy_cons_change_twh) OVER (ORDER BY year ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS moving_avg_energy_cons
FROM 
    final_energy
ORDER BY 
    year;

#total energy consumption per year and month
SELECT 
    year,
    SUM(energy_cons_change_twh) OVER (PARTITION BY year) AS total_energy_consumption_year,
    MONTH(year) AS month,
    SUM(energy_cons_change_twh) OVER (PARTITION BY YEAR(year), MONTH(year)) AS total_energy_consumption_month
FROM 
    final_energy
ORDER BY 
    year, month;

#average production changes over time
SELECT 
    year,
    AVG(coal_prod_change_pct) OVER (PARTITION BY year) AS avg_coal_prod_change,
    AVG(gas_prod_change_pct) OVER (PARTITION BY year) AS avg_gas_prod_change,
    AVG(oil_prod_change_pct) OVER (PARTITION BY year) AS avg_oil_prod_change
FROM 
    final_energy
ORDER BY 
    year;
    
    #trends using LAG() and LEAD() function
    SELECT 
    year,
    energy_cons_change_twh,
    LAG(energy_cons_change_twh) OVER (ORDER BY year) AS prev_year_energy_cons,
    LEAD(energy_cons_change_twh) OVER (ORDER BY year) AS next_year_energy_cons
FROM 
    final_energy
ORDER BY 
    year;
    
 select * from final_energy;
 
 #Primary energy consuption with gdp with year wise / trend analysis
 SELECT 
    de.iso_code,
    de.country,
    de.year,
    fe.primary_energy_consumption,
    fe.gdp
FROM 
    final_energy fe
JOIN 
    dim_energy de ON fe.iso_code = de.iso_code AND fe.year = de.year
LIMIT 0, 1000;

#correlation 
SELECT 
    AVG(fe.primary_energy_consumption) AS avg_energy_consumption,
    AVG(fe.gdp) AS avg_gdp
FROM 
    final_energy fe
JOIN 
    dim_energy de ON fe.iso_code = de.iso_code AND fe.year = de.year;
    
    #regional
    SELECT 
    de.country,
    SUM(fe.primary_energy_consumption) AS total_energy_consumption,
    AVG(fe.gdp) AS avg_gdp
FROM 
    final_energy fe
JOIN 
    dim_energy de ON fe.iso_code = de.iso_code AND fe.year = de.year
GROUP BY 
    de.country
ORDER BY 
    total_energy_consumption DESC;
    
    #energy intensity 
    SELECT 
    de.country,
    de.year,
    fe.primary_energy_consumption / fe.gdp AS energy_intensity
FROM 
    final_energy fe
JOIN 
    dim_energy de ON fe.iso_code = de.iso_code AND fe.year = de.year;
    
    SELECT 
    de.country,
    de.year,
    fe.primary_energy_consumption,
    fe.population
FROM 
    final_energy fe
JOIN 
    dim_energy de ON fe.iso_code = de.iso_code AND fe.year = de.year;
    
    #phase 4 
    #Average Energy Consumption per Capita by Country and Year
    SELECT 
    de.country,
    de.year,
    AVG(fe.primary_energy_consumption / fe.population) AS avg_energy_per_capita
FROM 
    fact_energy fe
JOIN 
    dim_energy de ON fe.iso_code = de.iso_code 
GROUP BY 
    de.country, de.year
ORDER BY 
    de.country, de.year;

#Countries with the Highest Energy Consumption Growth Rate
SELECT 
    de.country,
    (MAX(fe.primary_energy_consumption) - MIN(fe.primary_energy_consumption)) / MIN(fe.primary_energy_consumption) AS energy_growth_rate
FROM 
    fact_energy fe
JOIN 
    dim_energy de ON fe.iso_code = de.iso_code
GROUP BY 
    de.country
ORDER BY 
    energy_growth_rate DESC;

#Energy Intensity and GDP per Capita by Country and Year
SELECT 
    de.country,
    de.year,
    SUM(fe.primary_energy_consumption / fe.population) AS energy_intensity_per_capita,
    AVG(fe.gdp / fe.population) AS gdp_per_capita
FROM 
    fact_energy fe
JOIN 
    dim_energy de ON fe.iso_code = de.iso_code 
GROUP BY 
    de.country, de.year
ORDER BY 
    de.country, de.year;

#Renewable Energy Share of Total Energy Consumption by Country and Year
SELECT 
    de.country,
    de.year,
    SUM(fe.renewables_electricity) / SUM(fe.electricity_generation) AS renewable_energy_share
FROM 
    fact_energy fe
JOIN 
    dim_energy de ON fe.iso_code = de.iso_code 
GROUP BY 
    de.country, de.year
ORDER BY 
    de.country, de.year;


SELECT 
    date_column,
    SUM(primary_energy_consumption) AS total_energy_consumption
FROM 
    fact_energy
GROUP BY 
    date_column
ORDER BY 
    total_energy_consumption DESC
LIMIT 1;


select * from final_energyyyyyyyy;

#peak energy consumption
SELECT 
    date,
    country,
    avg(primary_energy_consumption) AS peak_energy_consumption
FROM 
    final_energyyyyyyyy
GROUP BY 
    date, country
ORDER BY 
    country,peak_energy_consumption asc
LIMIT 1,1000;


#Baseline
SELECT 
    primary_energy_consumption * gdp AS total_energy_consumption_baseline
FROM 
    final_energyyyyyyyy;

#Increase GDP by 10%
SELECT 
    primary_energy_consumption * (gdp * 1.1) AS total_energy_consumption_increase
FROM 
    final_energyyyyyyyy;

#Decrease GDP by 5%
SELECT 
    primary_energy_consumption * (gdp * 0.95) AS total_energy_consumption_decrease
FROM 
    final_energyyyyyyyy;

