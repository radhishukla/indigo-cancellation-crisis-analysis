# IndiGo Cancellation Crisis Analysis (2022–2023)

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-blue)
![Power BI](https://img.shields.io/badge/Power%20BI-teal)
![Analytics](https://img.shields.io/badge/Analytics-brightgreen)


This project analyzes the recent flight cancellation crisis faced by IndiGo Airlines using
PostgreSQL for data processing and Power BI for visualization.

## Tech Stack
- PostgreSQL – ETL, data cleaning, monthly trend extraction
- Power BI – KPI dashboard and crisis intelligence metrics
- Jira – Agile project tracking
- Confluence – Process documentation

## Key KPIs
- Total Passengers
- Passenger % Change
- Total Cargo & Cargo % Change
- Load Factor %
- Revenue per Passenger
- Crisis Severity Score
- Recovery Strength
- Operational Stability Score

## Workflow
1. Imported aviation datasets into PostgreSQL  
2. Built monthly passenger & cargo change views  
3. Identified crisis months using trend analysis  
4. Merged revenue & city traffic datasets  
5. Created Power BI crisis dashboard  

## Key Insights

- IndiGo passenger demand dropped heavily during mid-2022
- Load factor remained above 70% even in downturns
- Bengaluru–Delhi & Delhi–Mumbai were largest contributors to passenger drops
- Post-crisis recovery was strong with 1.53x rebound momentum

## How to Run
1. Import `*.csv` into PostgreSQL
2. Load SQL scripts from `/sql`
3. Connect Power BI Desktop to PostgreSQL
4. Open `powerbi/indigo_crisis_dashboard.pbix`

