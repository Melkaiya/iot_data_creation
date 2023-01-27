#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 20 04:53:52 2023
@author: dcordoba
Notes:
    Code will require arguments to be pass from the command line to execute the upfate function
"""
############################
# Import Statements
import sqlite3
import time
import datetime
import requests
import pandas as pd
import random
import logging
import concurrent.futures
############################
#DEFINE VARIABLES
start_time = time.time()
response = None

Colombia_MainMeter_WaterReading_Min = 122.190
Colombia_MainMeter_WaterReading_Max = 165.234
Colombia_MainMeter_Temp_Min = 16.0
Colombia_MainMeter_Temp_Max = 17.7
Colombia_MainMeterBattery = 94.78

Colombia_Edge_WaterReading_Min = 0.001
Colombia_Edge_WaterReading_Max = 1.176
Colombia_Edge_Temp_Min = 19.0
Colombia_Edge_Temp_Max = 21.0
Colombia_Edge_Battery = 98.71

USA_MainMeter_WaterReading_Min = 122.190
USA_MainMeter_WaterReading_Max = 165.234
USA_MainMeter_Temp_Min = 16.0
USA_MainMeter_Temp_Max = 17.7
USA_MainMeterBattery = 94.78

USA_Edge_WaterReading_Min = 0.001
USA_Edge_WaterReading_Max = 1.176
USA_Edge_Temp_Min = 19.0
USA_Edge_Temp_Max = 21.0
USA_Edge_Battery = 94.19

logging.basicConfig(filename='sensors_log.txt', level=logging.INFO)

############################
#DEFINE FUCNTIONS
#############################

#Data Base Creation or Connection, Data collection
def fnCreateDB_WaterSensors(db_file, table_name):
    #connect to database
    with sqlite3.connect(db_file) as conn:
        logging.info("Connecting to: "+db_file)
        #check if table exists
        if not conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone():
            conn.execute(f'''CREATE TABLE {table_name} (
                     name TEXT PRIMARY KEY NOT NULL UNIQUE,
                     DeviceProfile TEXT NOT NULL,
                     new_reading REAL NULL,
                     last_reading REAL NULL,
                     country TEXT NULL,
                     latitude REAL NULL,
                     longitude REAL NULL,
                     battery REAL NULL,
                     leakage INT NULL,
                     RSSI INT NULL,
                     pulseCounter INT NULL,
                     temperature REAL NULL,
                     token TEXT NULL UNIQUE,
                     sensorType TEXT NULL,
                     sensorSize TEXT NULL,
                     valvePosition TEXT NULL);''')
            logging.info(f"Table {table_name} created successfully")
        else:
            logging.info(f"Table {table_name} connection successful.")
        
        #query to select all data from the table
        cursor = conn.execute(f"SELECT * FROM {table_name}")
    
        #fetchall() method returns an empty list if the table is empty
        if not cursor.fetchall():
            logging.warning(f"The table {table_name} is empty.")
        else:
            print(f"The table {table_name} is not empty.")
            
        print("Closing Connection to: "+db_file)

#Import data from CSV to a data frame an dthe push it to SQL DB
def fnPushCSVtoSQL(csv_file, db_file, table_name):
    try:
        #read csv file
        df_waterSensors = pd.read_csv(csv_file)
    
        #connect to database
        with sqlite3.connect(db_file) as conn:
            #insert dataframe into table
            df_waterSensors.to_sql(table_name, conn, if_exists='replace', index=False)
            print("Data imported successfully")
            print("##########################################################")
            print("####     DATA FRAME INFORMATION    ####")
            print("##########################################################")
            print(df_waterSensors.info())
            print("##########################################################")
    except Exception as e:
        print("An error occurred:", e)   

#Print all the Row on a Table
def fnPrint_Database(db_file, table_name):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        # Count the number of rows in the table
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print("Number of Sensors Registered: ", row_count)

        # Print the data for each row
        cursor = conn.execute(f"SELECT name, DeviceProfile, country, sensorType, RSSI FROM {table_name}")
        #cursor = conn.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        if not rows:
            print(f"The table {table_name} is empty.")
        else:
            for row in rows:
                print(row)

        
def fnSensorDataUpdate(db_file, table_name, country,batch_num):
    
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        # Count the number of rows in the table
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE country='{country}'")
        row_count = cursor.fetchone()[0]
        print(f"{row_count} Sensors registered in {country}, running batch number: {batch_num}")
        
        # Retrieve all rows that match the country and battery condition
        cursor.execute(f"SELECT name, token, last_reading, sensorType FROM {table_name} WHERE country='{country}'")
        rows = cursor.fetchall()
        if not rows:
            print(f"No sensor found in {country}")
        else:
            for i, row in enumerate(rows):
                
                if country == 'Colombia':
                    
                    if i < (batch_num * 50) and i >= ((batch_num-1) * 50):
                        sensorName = row[0]
                        token = row[1]
                        last_reading = row[2]
                        sensorType = row[3]
                        
                        if sensorType == "Distribution":
                            WaterReading_Min = Colombia_MainMeter_WaterReading_Min   
                            WaterReading_Max = Colombia_MainMeter_WaterReading_Min
                            Temp_Min = Colombia_MainMeter_Temp_Min
                            Temp_Max = Colombia_MainMeter_Temp_Max
                            battery = Colombia_MainMeterBattery
                        elif sensorType == "End User":
                            WaterReading_Min = Colombia_Edge_WaterReading_Min  
                            WaterReading_Max = Colombia_Edge_WaterReading_Max
                            Temp_Min = Colombia_Edge_Temp_Min
                            Temp_Max = Colombia_Edge_Temp_Max
                            battery = Colombia_Edge_Battery
                        
                        new_temp = str(round(random.uniform(Temp_Min, Temp_Max), 1))
                        new_reading = round(last_reading + random.uniform(WaterReading_Min, WaterReading_Max),3)
                        last_reading = new_reading
                        
                        if sensorType == "Distribution":
                            random_RSSI = str(round(random.uniform(-90, -79)))
                        elif sensorType == "End User":
                            random_RSSI = str(round(random.uniform(-110, -89)))
                        
                        
                        ## Update emulated telemetry to IoT Server
                        headers = {'Content-Type': 'application/json', }
                        data = '{"leakage": false, "pulseCounter": ' + str(new_reading) + ', "valvePosition": open, "battery": ' + str(battery) + ', "temperature": ' +  new_temp + ', "RSSI": ' + random_RSSI + '}'
                        response = requests.post('https://iot.vistatech.tech/api/v1/'+token+'/telemetry', headers=headers, data=data)
                
                        #update data for sensors in the given country
                        cursor.execute(f"UPDATE {table_name} SET new_reading={new_reading}, last_reading={last_reading}, pulseCounter={new_reading}, temperature={new_temp}, RSSI={random_RSSI}, battery={battery} WHERE name='{sensorName}'")
                        #print(f"Data for sensor {sensorName} updated successfully")
                        logging.info(f"Data for sensor {sensorName} updated successfully")
                        conn.commit()
                        
                        ## New Data Local Output
                        logging.info("Sensor Name: " + sensorName + " Token: " + token + '\n' +
                              "Last Reading: " + str(last_reading)+ "m3 New Reading: " + str(new_reading) + "m3 Last RSSI = " + random_RSSI + ", Temperature = " + new_temp + "ºC")
                        
                elif country == 'USA':
                    
                    if i < (batch_num * 50) and i >= ((batch_num-1) * 50):
                        sensorName = row[0]
                        token = row[1]
                        last_reading = row[2]
                        sensorType = row[3]
                        
                        if sensorType == "Distribution":
                            WaterReading_Min = USA_MainMeter_WaterReading_Min   
                            WaterReading_Max = USA_MainMeter_WaterReading_Min
                            Temp_Min = USA_MainMeter_Temp_Min
                            Temp_Max = USA_MainMeter_Temp_Max
                            battery = USA_MainMeterBattery
                        elif sensorType == "End User":
                            WaterReading_Min = USA_Edge_WaterReading_Min  
                            WaterReading_Max = USA_Edge_WaterReading_Max
                            Temp_Min = USA_Edge_Temp_Min
                            Temp_Max = USA_Edge_Temp_Max
                            battery = USA_Edge_Battery
                        
                        new_temp = str(round(random.uniform(Temp_Min, Temp_Max), 1))
                        new_reading = round(last_reading + random.uniform(WaterReading_Min, WaterReading_Max),3)
                        last_reading = new_reading
                        
                        if sensorType == "Distribution":
                            random_RSSI = str(round(random.uniform(-90, -79)))
                        elif sensorType == "End User":
                            random_RSSI = str(round(random.uniform(-110, -89)))
                        
                        
                        ## Update emulated telemetry to IoT Server
                        headers = {'Content-Type': 'application/json', }
                        data = '{"leakage": false, "pulseCounter": ' + str(new_reading) + ', "valvePosition": open, "battery": ' + str(battery) + ', "temperature": ' +  new_temp + ', "RSSI": ' + random_RSSI + '}'
                        response = requests.post('https://iot.vistatech.tech/api/v1/'+token+'/telemetry', headers=headers, data=data)
                
                        #update data for sensors in the given country
                        cursor.execute(f"UPDATE {table_name} SET new_reading={new_reading}, last_reading={last_reading}, pulseCounter={new_reading}, temperature={new_temp}, RSSI={random_RSSI}, battery={battery} WHERE name='{sensorName}'")
                        #print(f"Data for sensor {sensorName} updated successfully")
                        logging.info(f"Data for sensor {sensorName} updated successfully")
                        conn.commit()
                        
                        ## New Data Local Output
                        logging.info("Sensor Name: " + sensorName + " Token: " + token + '\n' +
                              "Last Reading: " + str(last_reading)+ "m3 New Reading: " + str(new_reading) + "m3 Last RSSI = " + random_RSSI + ", Temperature = " + new_temp + "ºC")
                        
            
#############################
# END OF FUNCTIONS DEFINITION
#############################
# START CODE EXECUTION
#############################
fnCreateDB_WaterSensors('virtual_sensors.db', 'virtual_water_sensors')
#fnPushCSVtoSQL('waterSensors.csv','virtual_sensors.db', 'virtual_water_sensors')
#fnPrint_Database('virtual_sensors.db', 'virtual_water_sensors')

with concurrent.futures.ThreadPoolExecutor() as executor:
    # Submit all the functions to the executor
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 1)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 2)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 3)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 4)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 5)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 6)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 7)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 8)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 9)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 10)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 11)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 12)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 13)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 14)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'Colombia', 15)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'USA', 1)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'USA', 2)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'USA', 3)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'USA', 4)
    executor.submit(fnSensorDataUpdate, 'virtual_sensors.db', 'virtual_water_sensors', 'USA', 5)

end_time = time.time()

print("############################################################")
print("####"+' END OF TRANSMITION TOTAL RUNTIME = ' + str(round(end_time - start_time, 4)) + 
      ' seconds'+" #####")
print("############################################################")
logging.info("############################################################")
logging.info("####"+' END OF TRANSMITION TOTAL RUNTIME = ' + str(round(end_time - start_time, 4)) + 
      ' seconds'+" #####")
logging.info("############################################################")

now = datetime.datetime.now()
f = open("crontab_log.txt","a")
f.write("Virtual Sensor Python Code Execute @ {}\n".format(now.strftime("%Y-%m-%d %H:%M:%S")))
f.close()
print(f.read)

