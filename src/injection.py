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


def main():
    """
    Main entry point.

    The command-line argument should be the name of the
    function that should be called.
    """
    argslen = len(sys.argv)
    if( argslen != 6 ):
        print("  Usage: <directory of csv files> <sensor_id> <node_id> <injection from> <injection to>")
        print("Example: ../03_mar_csv/ Temperature 61 2022-03-10 2022-03-13")
        return

    rootdir_csv_files = sys.argv[1]
    sensor_id         = sys.argv[2]
    node_id_to_find   = sys.argv[3]

    d1 = datetime.datetime.fromisoformat( sys.argv[4] )
    d2 = datetime.datetime.fromisoformat( sys.argv[5] )

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

    #------------------------------------------------------
    # We have a different detector for each device
    #------------------------------------------------------
    device_detectors = dict()
    for node_id in nodes.keys():
        #detector = ADWIN( delta=0.002 )
        #detector = ADWIN( delta=0.2 )
        
        #detector = DDM( min_num_instances=30, warning_level=2.0, out_control_level=3.0 )
        #detector = DDM( min_num_instances=30, warning_level=10.0, out_control_level=3.0 )
        
        #detector = HDDM_A( drift_confidence=0.001, warning_confidence=0.005, two_side_option=True )
        #detector = HDDM_A( drift_confidence=0.0000000001, warning_confidence=0.0000000005, two_side_option=True )

        # lambda_option: The weight given to recent data. Smaller values mean less weight given to recent data.
        #detector = HDDM_W( drift_confidence=0.001, warning_confidence=0.005, lambda_option=0.05, two_side_option=True )
        #detector = HDDM_W( drift_confidence=0.00001, warning_confidence=0.00005, lambda_option=0.000001, two_side_option=False )

        lambda_option = 0.000001   # for Pressure
        lambda_option = 0.0001     # for Temperature
        lambda_option = 0.0000001  # for Light
        lambda_option = 0.0001     # for Humidity

        #detector = HDDM_W( drift_confidence=0.001, warning_confidence=0.005, lambda_option=0.000001, two_side_option=False )
        detector = HDDM_W( drift_confidence=0.001, warning_confidence=0.005, lambda_option=0.0001, two_side_option=False )

        device_detectors[node_id] = detector
        

    #------------------------------------------------------
    # Loop through printing out the records
    #------------------------------------------------------
    
    print(f'Sensors {sensor_id}')
    #if( sensor_id == 'Temperature' ):

    alpha = 0.5
    beta  = 1.0 - alpha

    for node_id in nodes.keys():
        node = nodes[node_id]

        detector = device_detectors[node_id]

        if( node.hasSensor(sensor_id) and node_id.endswith(node_id_to_find) ):
            timestamps, values = node.getSensorTimestamps( sensor_id )

            print( f'Length of values {len(values)}' )

            anomalies = list()

            
            #------------------------------------------------------
            # Add "anomaly"
            #------------------------------------------------------
            # date and time in yyyy/mm/dd hh:mm:ss format 
            # Find start and stop index
            #d1 = datetime.datetime(2022, 3, 10, 00, 00, 00)
            #d2 = datetime.datetime(2022, 3, 13, 00, 00, 00)
            
            start_index = None
            stop_index  = None
            for i in range( len(values) ):
                timestamp = timestamps[i]
                if( timestamp >= d1 and timestamp <= d2 ):
                    if(start_index is None):
                        start_index = i
                elif(start_index is not None and stop_index is None):
                    stop_index = i
                
                #if( start_index is not None and (timestamp < d1 or timestamp > d2) ): 
                #    stop_index = i
            
            for i in range(start_index, stop_index+1):
                #values[i] = 100000.0
                #values[i] = np.random.randint(low=50000, high=150000)
                dc_signal = values[start_index] - 0.00001*i
                
                #values[i] = (alpha*values[i]) + (beta*dc_signal)
                #values[i] = 20.0 # insert constant signal
            

            
            #------------------------------------------------------
            # Add the values to the detector one at a time
            #------------------------------------------------------
            for i in range( len(values) ):
                timestamp = timestamps[i]
                value     = values[i]
                
                detector.add_element(value)
                # Only applicable to the DDM, HDDM_A detectors, not all detectors have warning zone
                if detector.detected_warning_zone():
                    print('Warning zone has been detected in data: ' + str(value) + ' - of index: ' + str(i) + " timestamp: " + str(timestamp) )
                if detector.detected_change():
                    print('Change has been detected in data: ' + str(value) + ' - of index: ' + str(i) + " timestamp: " + str(timestamp) )
                    #anomaly = (i, timestamp, value)
                    anomaly = timestamp
                    anomalies.append(anomaly)
            print( f'Sensor {sensor_id} Device {node_id} encountered {len(anomalies)} anomalies' )

            plt.scatter(timestamps, values, label="Device " + node_id_to_find)
            plt.vlines(x=anomalies, ymin=min(values), ymax=max(values), colors='red')
            
    
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
    print(f"Index range [{start_index},{stop_index}]")
    print( f'Length of values {len(values)}' )


# Bootstrap, run this if script to python exe
if __name__ == '__main__':
    main()
