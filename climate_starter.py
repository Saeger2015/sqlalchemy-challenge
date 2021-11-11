#!/usr/bin/env python
# coding: utf-8

# In[7]:


get_ipython().run_line_magic('matplotlib', 'inline')
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt


# In[8]:


import numpy as np
import pandas as pd
import datetime as dt


# # Reflect Tables into SQLAlchemy ORM

# In[9]:


# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func


# In[21]:


# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# In[24]:


# reflect an existing database into a new model
new_database = automap_base()
# reflect the tables
new_database.prepare(engine, reflect=True)


# In[25]:


# View all of the classes that automap found
new_database.classes.keys()


# In[26]:


# Save references to each table
Measurement = new_database.classes.measurement
Station = new_database.classes.station


# In[27]:


# Create our session (link) from Python to the DB
session = Session(engine)


# # Exploratory Precipitation Analysis

# In[29]:


# Find the most recent date in the data set.
first_row = session.query(Measurement).first()
first_row.__dict__


# In[31]:


first_row = session.query(Station).first()
first_row.__dict__


# In[ ]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results. 
# Starting from the most recent data point in the database. 


# In[32]:


#Most recent date
recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
print(f'The most recent date is: {recent_date[0]}')


# In[33]:


# Calculate the date one year from the last date in data set.
dates = dt.date(2017, 8, 23) - dt.timedelta(days=365)


# In[35]:


# Perform a query to retrieve the data and precipitation scores
results = session.query(Measurement.date, func.avg(Measurement.prcp)).            filter(Measurement.date.between (dates, dt.date(2017, 8, 23))).group_by('date')

for value in results:
    print(value)


# In[38]:


# Save the query results as a Pandas DataFrame and set the index to the date column
new_df = pd.DataFrame(results, columns=['Date', 'Precipitation'])
# Sort the dataframe by date
new_df.set_index('Date', inplace=True)

new_df.sort_values(by='Date')
new_df


# In[40]:


# Use Pandas Plotting with Matplotlib to plot the data
new_df.plot(figsize= (10,6), rot = 90);
plt.ylabel(f'Precipitation (in)', size=14)
plt.title(f'Precipitation in 12 Months from 2016-08-23 to 2017-08-23', size=20)
plt.savefig("Images/my_precipitation.png");
plt.show();


# In[42]:


# Use Pandas to calcualte the summary statistics for the precipitation data
round(new_df.describe(),3)


# # Exploratory Station Analysis

# In[43]:


# Design a query to calculate the total number stations in the dataset
total_number_stations  = session.query(Station.station.distinct()).filter(Station.station.isnot(None)).count()
total_number_stations
print(f'The total number of stations in the data set is: {total_number_stations}')


# In[45]:


# Design a query to find the most active stations (i.e. what stations have the most rows?)
# List the stations and the counts in descending order.
active_stations = session.query(Measurement.station.label("Station"), Station.name.label("Name"), func.count(Measurement.station).label("Count"))                .join(Station, Station.station == Measurement.station).order_by(func.count(Measurement.station).desc())                .filter(Measurement.station.isnot(None))                .group_by(Measurement.station, Station.name)

for station in active_stations:
    print(station)


# In[46]:


print(f'The most active stations is: {active_stations[0][0]} {active_stations[0][1]}')
print(f'with: {active_stations[0][2]} rows')


# In[47]:


# Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
AVG_most_active_station = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs))                          .filter(Measurement.station == active_stations[0][0]).all()
AVG_most_active_station

print(f'Stations:{active_stations[0][0]}')
print(f'Lowest temperature: {AVG_most_active_station[0][0]}') 
print(f'Highest temperature: {AVG_most_active_station[0][1]}')
print(f'Average temperature: {round(AVG_most_active_station[0][2],2)}')


# In[48]:


# Using the most active station id
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram
temp_obser_12 = session.query(Measurement.date, Measurement.tobs)                .filter(Measurement.station == active_stations[0][0], Measurement.date.between(dates, dt.date(2017, 8, 23)))                .group_by(Measurement.date)                .order_by(Measurement.date)

for station in temp_obser_12:
    print(station)    


# In[49]:


#histograms: 
temp_obser_df = pd.DataFrame(temp_obser_12, columns=['Date', 'Temp'])
temp_obser_df.set_index('Date', inplace=True)
plt.xlabel('Temperature', fontsize='12')
plt.ylabel('Frec. temperature', fontsize='12')
plt.title(f'Temperature observation for: {active_stations[0][0]}', fontsize='12')
plt.hist(temp_obser_df, bins=12, alpha=.9, color='#00177f', label='temperature', edgecolor='w')
plt.grid(axis='x', alpha=.4)
plt.legend();
plt.savefig("Images/station-histogram_USC00519281.png");


# In[51]:


domain = np.linspace(temp_obser_df['Temp'].min(), temp_obser_df['Temp'].max())
mean_temp = temp_obser_df['Temp'].mean()
std_temp = temp_obser_df['Temp'].std()
plt.plot(domain, norm.pdf(domain, mean_temp, std_temp ), label= '$\mathcal{N}$ ' + f'$( \mu \\approx {round(mean_temp)} , \sigma \\ approx {round(std_temp)} )$', color='black')
plt.hist(temp_obser_df, edgecolor='w', alpha=.9, density=True, color='#00177f')
plt.xlabel('Temperature', fontsize='12')
plt.ylabel('Frec. temperature', fontsize='12')
plt.title(f'Normal fit Temperature observation for: {active_stations[0][0]}', fontsize='12')
plt.grid(axis='x', alpha=.4)
plt.legend()
plt.savefig("Images/norm_station-histogram_USC00519281.png");
plt.show();
print(f'The average temperature of station {active_stations[0][0]} is: {round(mean_temp,2)}')


# # Close session

# In[ ]:


# Close Session
session.close()


# In[ ]:




