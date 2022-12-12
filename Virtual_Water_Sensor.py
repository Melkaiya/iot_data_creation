# -*- coding: utf-8 -*-
############################
# Import Statements
import datetime
import requests
import random
import time
import pandas as pd
#############################
now = datetime.datetime.now()
start = time.time() # Grab Currrent Time Before Running the Code

# Create dataFrame from CSV
df_WaterSensors =  pd.read_csv('coagua_iot_ptychi.csv')
print(df_WaterSensors)

#Define Variables
intSensorQty = 6
intMainMeter_WaterReading_Min = 15.000
intMainMeter_WaterReading_Max = 125.234
intMainMeter_Temp_Min = 19.0
intMainMeter_Temp_Max = 26.
intMainMeterBattery = 94.78

intEdgeMeter_WaterReading_Min = 0.001
intEdgeMeter_WaterReading_Max = 2.876
intEdgeMeter_Temp_Min = 23.0
intEdgeMeter_Temp_Max = 29.0
intEdgeMeterBattery = 96.20

print("##########################################################")
print("#########  NEW DATA ENTRY @ "+now.strftime("%Y-%m-%d %H:%M:%S")+"  #########")
print("##########################################################")

for index in range(intSensorQty):
    
    
    if index == 0:
 
        random_RSSI = str(round(random.uniform(-90, -79)))
        random_temperature = str(round(random.uniform(intMainMeter_Temp_Min, intMainMeter_Temp_Max), 1))
        
        sensorName = df_WaterSensors.loc[index, "Meter Name"]
        token = df_WaterSensors.loc[index, "Token"]
        lastReading = df_WaterSensors.loc[index, "Last Reading"]
        newReading = round(lastReading + random.uniform(intMainMeter_WaterReading_Min, intMainMeter_WaterReading_Max),3)
        df_WaterSensors.loc[index, "Last Reading"] = newReading
        
        ## Update emulated telemetry to IoT Server
        headers = {'Content-Type': 'application/json', }
        data = '{"leakage": false, "pulseCounter": ' + str(newReading) + ', "valvePosition": Open, "battery": ' + str(intMainMeterBattery) + ', "temperature": ' +  random_temperature + ', "RSSI": ' + random_RSSI + '}'
        response = requests.post('https://thingsboard.cloud/api/v1/'+token+'/telemetry', headers=headers, data=data)
        
        ## New Data Local Output
        print("Sensor Name: " + sensorName + " Token: " + token)
        print("Last Reading: " + str(lastReading)+ "m3 New Reading: " + str(newReading) + "m3 Last RSSI = " + random_RSSI + ", Temperature = " + random_temperature + "ºC")
        
    elif index > 0:

        random_RSSI = str(round(random.uniform(-110, -79)))
        random_temperature = str(round(random.uniform(intEdgeMeter_Temp_Min, intEdgeMeter_Temp_Max), 1))
        
        sensorName = df_WaterSensors.loc[index, "Meter Name"]
        token = df_WaterSensors.loc[index, "Token"]
        lastReading = df_WaterSensors.loc[index, "Last Reading"]
        newReading = round(lastReading + random.uniform(intEdgeMeter_WaterReading_Min, intEdgeMeter_WaterReading_Max),3)
        df_WaterSensors.loc[index, "Last Reading"] = newReading
        
        ## Update emulated telemetry to IoT Server
        headers = {'Content-Type': 'application/json', }
        data = '{"leakage": false, "pulseCounter": ' + str(newReading) + ', "valvePosition": Open, "battery": ' + str(intMainMeterBattery) + ', "temperature": ' +  random_temperature + ', "RSSI": ' + random_RSSI + '}'
        response = requests.post('https://thingsboard.cloud/api/v1/'+token+'/telemetry', headers=headers, data=data)
        
        ## New Data Local Output
        print("Sensor Name: " + sensorName + " Token: " + token)
        print("Last Reading: " + str(lastReading)+ "m3 New Reading: " + str(newReading) + "m3 Last RSSI = " + random_RSSI + ", Temperature = " + random_temperature + "ºC")
    
    
    
#Update csv file
df_WaterSensors.to_csv('coagua_iot_ptychi.csv', index=False)  
#Print Final runtime and close connection
end = time.time() # Grab Currrent Time After Running the Code
total_time = end - start #Subtract Start Time from The End Time
print("##########################################################")
print("####"+' END OF TRANSMITION TOTAL RUNTIME = ' + str(round(total_time, 2)) + ' seconds'+" ##3##")
print("##########################################################")


