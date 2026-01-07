# Views to transform marketplace data in pipeline

import os

from snowflake.core import Root, CreateMode
from snowflake.snowpark import Session
from snowflake.core.user_defined_function import (
    Argument,
    ReturnDataType,
    PythonFunction,
    UserDefinedFunction,
)
from snowflake.core.view import View, ViewColumn


# -------------------------------
# UDF: Map airport → city
# -------------------------------
map_city_to_airport = UserDefinedFunction(
    name="get_city_for_airport",
    arguments=[Argument(name="iata", datatype="VARCHAR")],
    return_type=ReturnDataType(datatype="VARCHAR"),
    language_config=PythonFunction(
        runtime_version="3.11",
        packages=["snowflake-snowpark-python"],
        handler="main",
    ),
    body="""
from snowflake.snowpark.files import SnowflakeFile
from _snowflake import vectorized
import pandas
import json

@vectorized(input=pandas.DataFrame)
def main(df):
    airport_list = json.loads(
        SnowflakeFile.open(
            "@bronze.raw/airport_list.json",
            'r',
            require_scoped_url=False
        ).read()
    )
    airports = {airport[3]: airport[1] for airport in airport_list}
    return df[0].apply(lambda iata: airports.get(iata.upper()))
""",
)

# -------------------------------
# SILVER pipeline views
# -------------------------------
pipeline = [
    View(
        name="flight_emissions",
        columns=[
            ViewColumn(name="departure_airport"),
            ViewColumn(name="arrival_airport"),
            ViewColumn(name="co2_emissions_kg_per_person"),
        ],
        query="""
        select
            departure_airport,
            arrival_airport,
            avg(estimated_co2_total_tonnes / seats) * 1000
                as co2_emissions_kg_per_person
        from oag_flight_emissions_data_sample.public
             .estimated_emissions_schedules_sample
        where seats != 0
          and estimated_co2_total_tonnes is not null
        group by departure_airport, arrival_airport
        """,
    ),

    View(
        name="flight_punctuality",
        columns=[
            ViewColumn(name="departure_iata_airport_code"),
            ViewColumn(name="arrival_iata_airport_code"),
            ViewColumn(name="punctual_pct"),
        ],
        query="""
        select
            departure_iata_airport_code,
            arrival_iata_airport_code,
            count(
                case when arrival_actual_ingate_timeliness
                     in ('OnTime','Early') then 1 end
            ) / count(*) * 100 as punctual_pct
        from oag_flight_status_data_sample.public
             .flight_status_latest_sample
        where arrival_actual_ingate_timeliness is not null
        group by
            departure_iata_airport_code,
            arrival_iata_airport_code
        """,
    ),

    View(
        name="flights_from_home",
        columns=[
            ViewColumn(name="departure_airport"),
            ViewColumn(name="arrival_airport"),
            ViewColumn(name="arrival_city"),
            ViewColumn(name="co2_emissions_kg_per_person"),
            ViewColumn(name="punctual_pct"),
        ],
        query="""
        select
            departure_airport,
            arrival_airport,
            get_city_for_airport(arrival_airport) as arrival_city,
            co2_emissions_kg_per_person,
            punctual_pct
        from flight_emissions
        join flight_punctuality
          on departure_airport = departure_iata_airport_code
         and arrival_airport   = arrival_iata_airport_code
        where departure_airport = (
            select $1:airport
            from @quickstart_common.public.quickstart_repo
                 /branches/main/data/home.json
                 (file_format => bronze.json_format)
        )
        """,
    ),

    View(
        name="weather_forecast",
        columns=[
            ViewColumn(name="postal_code"),
            ViewColumn(name="avg_temperature_air_f"),
            ViewColumn(name="avg_relative_humidity_pct"),
            ViewColumn(name="avg_cloud_cover_pct"),
            ViewColumn(name="precipitation_probability_pct"),
        ],
        query="""
        select
            postal_code,
            avg(avg_temperature_air_2m_f) avg_temperature_air_f,
            avg(avg_humidity_relative_2m_pct)
                avg_relative_humidity_pct,
            avg(avg_cloud_cover_tot_pct) avg_cloud_cover_pct,
            avg(probability_of_precipitation_pct)
                precipitation_probability_pct
        from global_weather__climate_data_for_bi.standard_tile
             .forecast_day
        where country = 'US'
        group by postal_code
        """,
    ),

    View(
        name="major_us_cities",
        columns=[
            ViewColumn(name="geo_id"),
            ViewColumn(name="geo_name"),
            ViewColumn(name="total_population"),
        ],
        query="""
        select
            geo.geo_id,
            geo.geo_name,
            max(ts.value) total_population
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE
             .DATACOMMONS_TIMESERIES ts
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE
             .GEOGRAPHY_INDEX geo
          on ts.geo_id = geo.geo_id
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE
             .GEOGRAPHY_RELATIONSHIPS geo_rel
          on geo_rel.related_geo_id = geo.geo_id
        where ts.variable_name = 'Total Population, census.gov'
          and date >= '2020-01-01'
          and geo.level = 'City'
          and geo_rel.geo_id = 'country/USA'
          and value > 100000
        group by geo.geo_id, geo.geo_name
        """,
    ),

    View(
        name="zip_codes_in_city",
        columns=[
            ViewColumn(name="city_geo_id"),
            ViewColumn(name="city_geo_name"),
            ViewColumn(name="zip_geo_id"),
            ViewColumn(name="zip_geo_name"),
        ],
        query="""
        select
            city.geo_id city_geo_id,
            city.geo_name city_geo_name,
            city.related_geo_id zip_geo_id,
            city.related_geo_name zip_geo_name
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE
             .GEOGRAPHY_RELATIONSHIPS country
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE
             .GEOGRAPHY_RELATIONSHIPS city
          on country.related_geo_id = city.geo_id
        where country.geo_id = 'country/USA'
          and city.level = 'City'
          and city.related_level =
              'CensusZipCodeTabulationArea'
        """,
    ),

    View(
        name="weather_joined_with_major_cities",
        columns=[
            ViewColumn(name="geo_id"),
            ViewColumn(name="geo_name"),
            ViewColumn(name="total_population"),
            ViewColumn(name="avg_temperature_air_f"),
            ViewColumn(name="avg_relative_humidity_pct"),
            ViewColumn(name="avg_cloud_cover_pct"),
            ViewColumn(name="precipitation_probability_pct"),
        ],
        query="""
        select
            city.geo_id,
            city.geo_name,
            city.total_population,
            avg(avg_temperature_air_f) avg_temperature_air_f,
            avg(avg_relative_humidity_pct)
                avg_relative_humidity_pct,
            avg(avg_cloud_cover_pct) avg_cloud_cover_pct,
            avg(precipitation_probability_pct)
                precipitation_probability_pct
        from major_us_cities city
        join zip_codes_in_city zip
          on city.geo_id = zip.city_geo_id
        join weather_forecast weather
          on zip.zip_geo_name = weather.postal_code
        group by
            city.geo_id,
            city.geo_name,
            city.total_population
        """,
    ),

    # ✅ NEW VIEW — ATTRACTIONS
    View(
        name="attractions",
        columns=[
            ViewColumn(name="geo_id"),
            ViewColumn(name="geo_name"),
            ViewColumn(name="aquarium_cnt"),
            ViewColumn(name="zoo_cnt"),
            ViewColumn(name="korean_restaurant_cnt"),
        ],
        query="""
        select
            city.geo_id,
            city.geo_name,
            count(case when category_main = 'Aquarium' then 1 end)
                aquarium_cnt,
            count(case when category_main = 'Zoo' then 1 end)
                zoo_cnt,
            count(case when category_main = 'Korean Restaurant' then 1 end)
                korean_restaurant_cnt
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE
             .point_of_interest_index poi
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE
             .point_of_interest_addresses_relationships poi_add
          on poi_add.poi_id = poi.poi_id
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE
             .us_addresses address
          on address.address_id = poi_add.address_id
        join major_us_cities city
          on city.geo_id = address.id_city
        where category_main in
              ('Aquarium','Zoo','Korean Restaurant')
          and id_country = 'country/USA'
        group by city.geo_id, city.geo_name
        """,
    ),
]

# -------------------------------
# Snowflake connection
# -------------------------------
connection_parameters = {
    "account": "AEENOGP-BA23886",
    "user": "BHAVANAVENULA03",
    "password": "Bhavana9963@LPDG",
    "role": "ACCOUNTADMIN",
    "warehouse": "QUICKSTART_WH",
    "database": "QUICKSTART_PROD",
    "schema": "PUBLIC",
}

session = Session.builder.configs(connection_parameters).create()
root = Root(session)

# -------------------------------
# Deploy objects
# -------------------------------
silver_schema = root.databases["quickstart_prod"].schemas["silver"]

silver_schema.user_defined_functions.create(
    map_city_to_airport,
    mode=CreateMode.or_replace
)

for view in pipeline:
    silver_schema.views.create(view, mode=CreateMode.or_replace)
