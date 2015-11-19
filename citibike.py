import pandas as pd
import matplotlib.pyplot as plt
from pandas.tools.plotting import scatter_matrix
from scipy import stats
import numpy as np
import statsmodels.api as sm
import math
import requests
from pandas.io.json import json_normalize
import sqlite3 as lite
import time  # a package with datetime objects
from dateutil.parser import parse  # a package for parsing a string into a Python datetime object
import collections
import datetime


r = requests.get('http://www.citibikenyc.com/stations/json')
print r.json().keys()	#Display keys
print len(r.json()['stationBeanList'])    #view number of docks in stationBeanList

key_list = [] #unique list of keys for each station listing
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)

df = json_normalize(r.json()['stationBeanList'])

df['availableBikes'].hist()
plt.show()

df['totalDocks'].hist()
plt.show()

df['testStation'].hist()
plt.show()

print 'Mean of all available bikes'
print df['availableBikes'].mean()
print 'Median of all available bikes'
print df['availableBikes'].median()

condition = (df['statusValue'] == 'In Service')
print 'Mean of available bikes of only docks in service'
print df[condition]['totalDocks'].mean()

print 'Median of total docks'
print df['totalDocks'].median()
print 'Median of total docks in service'
print df[df['statusValue'] == 'In Service']['totalDocks'].median

print 'number of docks not in service'
print df[df['statusValue'] != 'In Service']['totalDocks'].count()
print 'number of docks in service'
print df[df['statusValue'] == 'In Service']['totalDocks'].count()

con = lite.connect('citi_bike.db')
cur = con.cursor()

#Be sure to create citibike_reference table one time only
with con:
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')

#a prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#for loop to populate values in the database
#Run this section only when creating citibike_reference table for the first time
with con:
    for station in r.json()['stationBeanList']:
        #id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))

#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

#add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

#create the table
#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")


#Loop over the retrieval of the avialble bikes once every 60 seconds and only 60 times
i = 1
for i in range(1,61):
	#Re-fetch fresh data from the data source for every loop
    r = requests.get('http://www.citibikenyc.com/stations/json')
    key_list = [] #unique list of keys for each station listing
    for station in r.json()['stationBeanList']:
        for k in station.keys():
            if k not in key_list:
                key_list.append(k)

    df = json_normalize(r.json()['stationBeanList'])
    #take the string and parse it into a Python datetime objects
    exec_time = parse(r.json()['executionTime'])
    with con:
		cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))
    id_bikes = collections.defaultdict(int) #defaultdict to store available bikes by station
	#loop through the stations in the station list
    for station in r.json()['stationBeanList']:
		id_bikes[station['id']] = station['availableBikes']
	#iterate through the defaultdict to update the values in the database
    with con:
		for k, v in id_bikes.iteritems():
			cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
    time.sleep(60)
    print i  	#print iteration counter after every loop to serve as a status update
	#i =+ 1


#Read in data collected for 1 hour of data
hourOfData = pd.read_sql_query("SELECT * FROM available_bikes ORDER BY execution_time",con,index_col='execution_time')

hour_change = collections.defaultdict(int)

for col in hourOfData.columns:
    station_vals = hourOfData[col].tolist()
    station_id = col[1:] 	#trim the "_"
    station_change = 0
    for k,v in enumerate(station_vals):
        if k < len(station_vals) - 1:
            station_change += abs(station_vals[k] - station_vals[k+1])	#why the iteration of station_change?
    hour_change[int(station_id)] = station_change 	#convert the station id back to integer

def keywithmaxval(d):
    # create a list of the dict's keys and values; 
    v = list(d.values())
    k = list(d.keys())

    # return the key with the max value
    return k[v.index(max(v))]

# assign the max key to max_station
max_station = keywithmaxval(hour_change)

#query sqlite for reference information
cur.execute("SELECT id, stationname, latitude, longitude FROM citibike_reference WHERE id = ?", (max_station,))
data = cur.fetchone()
print "The most active station is station id %s at %s latitude: %s longitude: %s " % data
print "With " + str(hour_change[379]) + " bicycles coming and going in the hour between " + datetime.datetime.fromtimestamp(int(hourOfData.index[0])).strftime('%Y-%m-%dT%H:%M:%S') + " and " + datetime.datetime.fromtimestamp(int(hourOfData.index[-1])).strftime('%Y-%m-%dT%H:%M:%S')

plt.bar(hour_change.keys(), hour_change.values())
plt.show()