"""
parse.py

Parses data drift sensor logs.

Author: James Pope
"""

import sys
import stdio
from instream import InStream
import timeutil
import datetime
from datetime import timedelta
from node import Node
import matplotlib.pyplot as plt

import os
from zipfile import ZipFile

# For detection, from https://scikit-multiflow.readthedocs.io/en/stable/api/generated/skmultiflow.drift_detection.DDM.html#skmultiflow.drift_detection.DDM
import numpy as np
from skmultiflow.drift_detection import DDM
from skmultiflow.drift_detection.adwin import ADWIN
from skmultiflow.drift_detection.hddm_a import HDDM_A
from skmultiflow.drift_detection.hddm_w import HDDM_W


def readFiles( path, ext="csv" ):
    """
    Reads all files in the with the given ext.
    """
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if ext in file:
                files.append(os.path.join(r, file))
    return files


def readSensorData( filename, nodes, sensor_id_to_find ):
    """
    Reads in the records into a list from the audit file.

    Reads in each line and creates a Record that parses the line.
    The records are appended to a list in the order encountered.
    returns list of devices.
    """
    print(f"PROCESSING: File {filename}")
    #------------------------------------------------------
    # Parse audit log and read into records
    #------------------------------------------------------
    datafile = InStream(filename)
    #devices = dict()

    lineNumber = 0
    while( datafile.hasNextLine() ):
        log = datafile.readLine()
        lineNumber = lineNumber + 1

        # DEBUG
        #if(lineNumber > 100): break

        # Skip header line
        if( lineNumber > 1 ):
            #record = Record(log, lineNumber)
            #records.append(record)
            #stdio.println(record)
            tokens = log.split(",")
            numTokens = len(tokens)
            if( numTokens != 4 ):
                #raise IOError(f"File {filename} expected four tokens at line {lineNumber} but instead got only {numTokens} tokens")
                print(f"ISSUE: File {filename} expected four tokens at line {lineNumber} but instead got only {numTokens} tokens")
                break
            # Time,DeviceId,Sensor,Value
            epoch     = int(tokens[0])
            node_id   = tokens[1]
            sensor_id = tokens[2]
            value     = float(tokens[3])

            if( sensor_id == sensor_id_to_find ):

                # Convert time_str into datetime
                datetime = timeutil.epoch_to_datetime(epoch)
                #print( f"{datetime}  device {node_id} sensor {sensor_id} value {value:.2f} " )

                if( node_id not in nodes ):
                    nodes[node_id] = Node(node_id)
                node = nodes[node_id]
                node.add( sensor_id, datetime, value )
            

    del(datafile)

def find_node( nodes, node_id_to_find ):
    for node_id in nodes.keys():
        if( node_id.endswith(node_id_to_find) ):
            return nodes[node_id]
    return None

    
def dict2lists( dictionary ):
    timestamps = list()
    values     = list()
    for timestamp in dictionary:
        value = dictionary[timestamp]
        timestamps.append(timestamp)
        values.append(value)
    return timestamps, values


def main():
    """
    Main entry point.

    The command-line argument should be the name of the
    function that should be called.
    """
    argslen = len(sys.argv)
    if( argslen != 5 ):
        print("  Usage: <directory of csv files> <sensor_id> <node a> <node b>")
        print("Example: ../mini_files_csv Temperature 85 b5")
        return

    rootdir_csv_files = sys.argv[1]
    sensor_id         = sys.argv[2]
    node_a_name       = sys.argv[3]
    node_b_name       = sys.argv[4]

    # DEVICES
    # f6ce368d7563b285
    # f6ce368f8d612db5
    # f6ce36f0118e6361
    # f6ce36d563cef9cb
    # f6ce364ff4c1c55a
    # f6ce3667a3445b20
    # f6ce36c1896a819b
    # f6ce36ef672a639d

    # SENSORS
    # MIC
    # Pressure
    # IR
    # Humidity
    # RSSI
    # Gas
    # Temperature
    # Accelerometer
    # Light
    #sensor_ids = ["Pressure", "Humidity", "Gas", "Temperature", "Light"]
    #sensor_id = "Pressure"

    #------------------------------------------------------
    # Read the csv data files keeping track of the devices
    #------------------------------------------------------
    csv_files = readFiles(rootdir_csv_files, ext="csv")
    nodes = dict()
    for f in csv_files:
        readSensorData(f, nodes, sensor_id)

    node_a = find_node( nodes, node_a_name )
    node_b = find_node( nodes, node_b_name )

    
    #------------------------------------------------------
    # Loop through printing out the records
    #------------------------------------------------------
    
    print(f'Sensors {sensor_id}')

    anomalies = list()

    # Use node_a timestamp, has some privilege in this way
    timestamps_a, values_a = node_a.getSensorTimestamps( sensor_id )
    timestamps_b, values_b = node_b.getSensorTimestamps( sensor_id )

    #start_timestamp = timestamps_a[0] # use same starting timestamp so both are aligned
    start_timestamp = node_a.minTimestamp(sensor_id) # use same starting timestamp so both are aligned
    #start_timestamp_b = node_b.minTimestamp(sensor_id)
    #print(f"First date {node_a_name} {start_timestamp_a}")
    #print(f"First date {node_b_name} {start_timestamp_b}")

    a_averages = node_a.averageSensorValues( sensor_id, timedelta(hours=1), timestamp_start=start_timestamp )
    b_averages = node_b.averageSensorValues( sensor_id, timedelta(hours=1), timestamp_start=start_timestamp )

    # Convert dictionary of values to lists suitable for plotting
    a_avg_timestamps, a_avg_values = dict2lists( a_averages )
    b_avg_timestamps, b_avg_values = dict2lists( b_averages )

    # Create one detector
    detector = HDDM_W( drift_confidence=0.001, warning_confidence=0.005, lambda_option=0.01, two_side_option=False )
    diff_dict = dict()
    for i in range(len(a_avg_timestamps)):
        timestamp = a_avg_timestamps[i]
        a_value = a_avg_values[i]
        b_value = b_avg_values[i]
        
        #print(f"DIFF between {a_value} and {b_value}")
        if( a_value is None or b_value is None ):
            continue
        value = a_value - b_value
        diff_dict[timestamp] = value

        #------------------------------------------------------
        # Add the values to the detector one at a time
        #------------------------------------------------------
        detector.add_element(value)
        # Only applicable to the DDM, HDDM_A detectors, not all detectors have warning zone
        if detector.detected_warning_zone():
            print('Warning zone has been detected in data: ' + str(value) + ' - of index: ' + str(i) + " timestamp: " + str(timestamp) )
        if detector.detected_change():
            print('Change has been detected in data: ' + str(value) + ' - of index: ' + str(i) + " timestamp: " + str(timestamp) )
            #anomaly = (i, timestamp, value)
            anomaly = timestamp
            anomalies.append(anomaly)
    print( f'Sensor {sensor_id} Devices {node_a_name} {node_b_name} encountered {len(anomalies)} anomalies' )

    print(f"TIMESTAMPS first {a_avg_timestamps[0]} and {b_avg_timestamps[0]}")
    print(f"TIMESTAMPS length a {len(a_avg_timestamps)} and b {len(b_avg_timestamps)}")
    print(f"TIMESTAMPS last {a_avg_timestamps[-1]} and {b_avg_timestamps[-1]}")
    

    #print(f"--------------------------- A ---------------------------") 
    #print(f"{a_averages}")
    #print(f"--------------------------- B ---------------------------") 
    #print(f"{b_averages}")
    

    diff_timestamps, diff_values = dict2lists(diff_dict)    

    plt.scatter(    timestamps_a,     values_a, label="Device Raw Values " + node_a_name)
    #plt.scatter(a_avg_timestamps, a_avg_values, label="Device Avg Values " + node_a_name)
    plt.scatter(    timestamps_b,     values_b, label="Device Raw Values " + node_b_name)
    #plt.scatter(b_avg_timestamps, b_avg_values, label="Device Avg Values " + node_b_name)
    
    diff_label = f"Diff Values between {node_a_name} and {node_b_name} for sensor {sensor_id}"
    #plt.scatter(diff_timestamps, diff_values, label=diff_label )
    #plt.vlines(x=anomalies, ymin=min(values_a), ymax=max(values_a), colors='red')
            
    
    plt.xlabel('timestamp')
    plt.xticks(rotation=90)
    plt.ylabel(f'{sensor_id}')
    plt.title(f'{sensor_id} Sensor')
    plt.legend()
    plt.show()

    stdio.println('Number of devices ' + str(len(nodes)) )
    for node_id in nodes.keys():
        print(f'   {node_id}')

   
    stdio.println('Sensors ["Pressure", "Humidity", "Gas", "Temperature", "Light"]' )
    

# Bootstrap, run this if script to python exe
if __name__ == '__main__':
    main()
