from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import folium as folium
import plotly.express as px
import matplotlib.pyplot as plt
import geopandas

spark=SparkContext.getOrCreate()

sqlContext=SQLContext(spark)

airports = sqlContext.read.csv("airports.csv", sep=",", inferSchema="true", header="true")


columns=[]
airlines=airports.toDF("FlightDate", "IATA_CODE_Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline", "Origin", "OriginCityName", "OriginState", "OriginStateFips", "Dest", "DestCityName", "DestState", "DestStateFips", "DestStateName", "CRSDepTime", "DepTime", "DepDelay", "CRSArrTime", "ArrTime","ArrDelay", "Cancelled", "CancellationCode", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Flights", "Distance", "DistanceGroup", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")

airLineArrDelay=airlines.groupby("OriginCityName").agg({"ArrDelay":"avg"})



airLineArrDelay.na.fill(0)

airLineArrDelays = airLineArrDelay.withColumn('City', split(airLineArrDelay['OriginCityName'], ',').getItem(0)) \
                                   .withColumn('State', split(airLineArrDelay['OriginCityName'], ',').getItem(1))
airLineArrDelays.na.fill(0)



airLineArrDelayState=airLineArrDelays.groupby("State").agg({"avg(ArrDelay)":"avg"})

airlineArrDF=airLineArrDelayState.na.fill(0).toPandas()

airlineArrDF["State"] = airlineArrDF["State"].astype(str)
airlineArrDF["avg(avg(ArrDelay))"]=airlineArrDF["avg(avg(ArrDelay))"].astype(int)




airlineArrDF=airlineArrDF.drop([13,30,47])

airlineArrDF.columns=["State","Arrival Delay"]
# print(airlineArrDF)

fig = px.bar(airlineArrDF, x='State', y='Arrival Delay',title="Arrival Delays in Airports(in hrs.)")
fig.show()

my_USA_map = 'us-states.json'

# Us_map = folium.Map(location=[48, -102], zoom_start=3)

# folium.GeoJson(my_USA_map).add_to(Us_map)
# Us_map.choropleth(geo_data=my_USA_map, data=airlineArrDF,
#              columns=["State","Delay"],
#              key_on='feature.id',
#              fill_color='BuPu',
#              fill_opacity=0.7,
#              line_opacity=0.2,
#              legend_name='Arrival delays Average'
#               )
# Us_map


#--------------------Carrier Delay

airlineCarrierDelay=airlines.groupby("OriginCityName").agg({"CarrierDelay":"avg"})
# airlineCarrierDelay.show()
airlineCarrierDelay.na.fill(0)
airlineCarrierDelays = airlineCarrierDelay.withColumn('City', split(airlineCarrierDelay['OriginCityName'], ',').getItem(0)) \
                                   .withColumn('State', split(airlineCarrierDelay['OriginCityName'], ',').getItem(1))
airLineCarrierDelayState=airlineCarrierDelays.groupby("State").agg({"avg(CarrierDelay)":"avg"})

airLineCarrierDelayState.sort("State").show(100)
airlineCarrierDF=airLineCarrierDelayState.na.fill(0).toPandas()

airlineCarrierDF["State"] = airlineCarrierDF["State"].astype(str)
airlineCarrierDF["avg(avg(CarrierDelay))"]=airlineCarrierDF["avg(avg(CarrierDelay))"].astype(int)
airlineCarrierDF=airlineCarrierDF.drop([13,30,47])
airlineCarrierDF.columns=["State","Carrier Delay"]
print(airlineCarrierDF)

fig1 = px.bar(airlineCarrierDF, x='State', y='Carrier Delay',title="Carrier Delays in Airports(in hrs.)")
fig1.show()

#------------------------WeatherDelay
airlineWeatherDelay=airlines.groupby("OriginCityName").agg({"WeatherDelay":"avg"})
# airlineCarrierDelay.show()
airlineWeatherDelay.na.fill(0)
airlineWeatherDelays = airlineWeatherDelay.withColumn('City', split(airlineCarrierDelay['OriginCityName'], ',').getItem(0)) \
                                   .withColumn('State', split(airlineCarrierDelay['OriginCityName'], ',').getItem(1))
airlineWeatherDelayState=airlineWeatherDelays.groupby("State").agg({"avg(WeatherDelay)":"avg"})

airlineWeatherDelayState.sort("State").show(100)
airlineWeatherDF=airlineWeatherDelayState.na.fill(0).toPandas()

airlineWeatherDF["State"] = airlineWeatherDF["State"].astype(str)
airlineWeatherDF["avg(avg(WeatherDelay))"]=airlineWeatherDF["avg(avg(WeatherDelay))"].astype(int)
airlineWeatherDF=airlineWeatherDF.drop([13,30,47])
airlineWeatherDF.columns=["State","Weather Delay"]
print(airlineWeatherDF)

fig2 = px.bar(airlineWeatherDF, x='State', y='Weather Delay',title="Weather Delays in Airports(in hrs.)")
fig2.show()

#-------------------------NASDelay
airlineNASDelay=airlines.groupby("OriginCityName").agg({"NASDelay":"avg"})
# airlineCarrierDelay.show()
airlineNASDelay.na.fill(0)
airlineNASDelays = airlineNASDelay.withColumn('City', split(airlineCarrierDelay['OriginCityName'], ',').getItem(0)) \
                                   .withColumn('State', split(airlineCarrierDelay['OriginCityName'], ',').getItem(1))
airlineNASDelayState=airlineNASDelays.groupby("State").agg({"avg(NASDelay)":"avg"})

airlineNASDelayState.sort("State").show(100)
airlineNASDF=airlineNASDelayState.na.fill(0).toPandas()

airlineNASDF["State"] = airlineNASDF["State"].astype(str)
airlineNASDF["avg(avg(NASDelay))"]=airlineNASDF["avg(avg(NASDelay))"].astype(int)
airlineNASDF=airlineNASDF.drop([13,30,47])
airlineNASDF.columns=["State","NAS Delay"]
print(airlineNASDF)

fig3 = px.bar(airlineNASDF, x='State', y='NAS Delay',title="National Airspace Delays in Airports(in hrs.)")
fig3.show()

#------------------------SecurityDelay

airlineSecurityDelay=airlines.groupby("OriginCityName").agg({"SecurityDelay":"avg"})
# airlineCarrierDelay.show()
airlineSecurityDelay.na.fill(0)
airlineSecurityDelays = airlineSecurityDelay.withColumn('City', split(airlineCarrierDelay['OriginCityName'], ',').getItem(0)) \
                                   .withColumn('State', split(airlineCarrierDelay['OriginCityName'], ',').getItem(1))
airlineSecurityDelayState=airlineSecurityDelays.groupby("State").agg({"avg(SecurityDelay)":"avg"})

airlineSecurityDelayState.sort("State").show(100)
airlineSecurityDF=airlineSecurityDelayState.na.fill(0).toPandas()

airlineSecurityDF["State"] = airlineSecurityDF["State"].astype(str)
airlineSecurityDF["avg(avg(SecurityDelay))"]=airlineSecurityDF["avg(avg(SecurityDelay))"].astype(float)
airlineSecurityDF=airlineSecurityDF.drop([13,30,47])
airlineSecurityDF.columns=["State","Security Delay"]
print(airlineSecurityDF)

fig4 = px.bar(airlineSecurityDF, x='State', y='Security Delay',title="Security Delays in Airports(in hrs.)")
fig4.show()
#------------------------LateAircraftDelay
airlineLateAircraftDelay=airlines.groupby("OriginCityName").agg({"LateAircraftDelay":"avg"})
# airlineCarrierDelay.show()
airlineLateAircraftDelay.na.fill(0)
airlineLateAircraftDelays = airlineLateAircraftDelay.withColumn('City', split(airlineCarrierDelay['OriginCityName'], ',').getItem(0)) \
                                   .withColumn('State', split(airlineCarrierDelay['OriginCityName'], ',').getItem(1))
airlineLateAircraftDelayState=airlineLateAircraftDelays.groupby("State").agg({"avg(LateAircraftDelay)":"avg"})

airlineLateAircraftDelayState.sort("State").show(100)
airlineLateAircraftDF=airlineLateAircraftDelayState.na.fill(0).toPandas()

airlineLateAircraftDF["State"] = airlineLateAircraftDF["State"].astype(str)
airlineLateAircraftDF["avg(avg(LateAircraftDelay))"]=airlineLateAircraftDF["avg(avg(LateAircraftDelay))"].astype(int)
airlineLateAircraftDF=airlineLateAircraftDF.drop([13,30,47])
airlineLateAircraftDF.columns=["State","Late Aircraft Delay"]
print(airlineLateAircraftDF)

fig5 = px.bar(airlineLateAircraftDF, x='State', y='Late Aircraft Delay',title="Late Aircraft Delays in Airports(in hrs.)")
fig5.show()
#_________EOF
