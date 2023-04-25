from utils.snowflake_connection import export_data_from_sf

funding_by_city_query = """
SELECT HEADQUARTERS_LOCATION, LAST_FUNDING_TYPE, SUM(LAST_FUNDING_AMOUNT_USD)
FROM "COMPANIES_DATA_ENRICHMENT"."PUBLIC"."FUNDING_CURRENT" FUNDING
WHERE HEADQUARTERS_LOCATION LIKE '%United States%'
GROUP BY 1, 2
;
"""

funding_by_city = export_data_from_sf(funding_by_city_query)
