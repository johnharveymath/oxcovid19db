import oxcovid19db as ox

# Open a connection to the database. Pass queries through execute. Connection will re-open if it times out
db_conn = ox.Connect()
df_epi = db_conn.execute(
    "SELECT source, date, adm_area_2, confirmed, gid FROM epidemiology WHERE source='GBR_PHE' AND "
    "adm_area_1='England' AND adm_area_2 IS NOT NULL AND adm_area_3 IS NULL")
df_mob = db_conn.execute(
    "SELECT source, date, adm_area_2, transit_stations, residential, workplace, parks, retail_recreation, "
    "grocery_pharmacy, gid FROM mobility WHERE source='GOOGLE_MOBILITY' AND adm_area_1='England' AND adm_area_2 IS "
    "NOT NULL")

# Use the merge function on two DataFrames with overlapping geography
df = ox.merge(df_epi, df_mob)
print(df.head())