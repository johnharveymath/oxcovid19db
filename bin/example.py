'''
   Copyright 2021 John Harvey

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
'''

# if you are working in a clone of the repository use this line:
from src import oxcovid19db as ox
# if you have installed the package from pypi use this line:
# import oxcovid19db as ox

# Open a connection to the database.
db_conn = ox.Connect()

# Pass queries through execute to obtain data. Connection will re-open if it times out
df_epi = db_conn.execute("SELECT source, date, adm_area_2, confirmed, gid FROM epidemiology WHERE source='GBR_PHE' AND "
                         "adm_area_1='England' AND adm_area_2 IS NOT NULL AND adm_area_3 IS NULL")
df_mob = db_conn.execute(
    "SELECT source, date, adm_area_2, transit_stations, residential, workplace, parks, retail_recreation, "
    "grocery_pharmacy, gid FROM mobility WHERE source='GOOGLE_MOBILITY' AND adm_area_1='England' AND adm_area_2 IS "
    "NOT NULL")

# Use the merge function on two DataFrames with overlapping geography
df = ox.merge(df_epi, df_mob)
print(df.head())
