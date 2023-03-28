cities_linkedin_query = """
select location_raw, count(location_raw) cnt
from DWH.PUBLIC.CONTACTS_DB
where iso = 'US'
group by 1
order by 2 desc
limit 10000
;"""