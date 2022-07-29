"""
parse.py

Parses data drift sensor logs.

Author: James Pope
"""

import sys
import stdio
from instream import InStream
import timeutil
from device import Device
import matplotlib.pyplot as plt

import os
from zipfile import ZipFile



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


def readSensorData( filename, devices, aggregation_window, alpha ):
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
            device_id = tokens[1]
            sensor_id = tokens[2]
            value     = float(tokens[3])

            # Convert time_str into datetime
            datetime = timeutil.epoch_to_datetime(epoch)
            #print( f"{datetime}  device {device_id} sensor {sensor_id} value {value:.2f} " )

            if( device_id not in devices ):
                devices[device_id] = Device(device_id, aggregation_window, alpha)
            device = devices[device_id]
            device.add( sensor_id, datetime, value )
            

    del(datafile)


def main():
    """
    Main entry point.

    The command-line argument should be the name of the
    function that should be called.
    """
    argslen = len(sys.argv)
    if( argslen != 3 ):
        print("  Usage: <int: aggregation window> <float: alpha for EWMA>")
        print("Example: 100 0.7")
        return

    
    rootdir_zip_files = "../data"
    rootdir_csv_files = "../files_csv"

    aggregation_window=int( sys.argv[1] ) # samples to average over
    alpha=float( sys.argv[2] ) # average parameter, only applicable when aggregation_window > 1

    
    #------------------------------------------------------
    # Preprocessing - unzip any files not already unzipped
    #------------------------------------------------------
    # r=root, d=directories, f = files

    # Get all the current csv file names
    # Ignore full path as unziped paths are not consistent
    csv_files = set()
    for r, d, f in os.walk(rootdir_csv_files):
        for afile in f:
            if( ".csv" in afile ):
                csv_files.add(afile) # afile is just filename not the full path
    
    #print(csv_files)
    # Now find all zip file names, if there is not corresponding csv, then unzip
    for r, d, f in os.walk(rootdir_zip_files):
        for afile in f:
            if( ".zip" in afile ):
                csvfilename = afile.replace(".zip", ".csv")
                if( csvfilename not in csv_files ):
                    csvfile = os.path.join(r, csvfilename)
                    zipfile = os.path.join(r, afile)
                    print( "CSV: " + str(csvfile) )
                    print( "ZIP: " + str(zipfile) )
                    print( " " )
                    with ZipFile(zipfile, 'r') as zip_ref:
                        zip_ref.extractall(rootdir_csv_files)

    #------------------------------------------------------
    # Read the csv data files keeping track of the devices
    #------------------------------------------------------
    csv_files = readFiles(rootdir_csv_files, ext="csv")
    devices = dict()
    for f in csv_files:
        readSensorData(f, devices, aggregation_window, alpha)    


    #------------------------------------------------------
    # Figure out all the sensor_ids
    #------------------------------------------------------
    sensor_ids = set()
    for device_id in devices.keys():
        device = devices[device_id]
        for sensor_id in device.getSensorIds():
            sensor_ids.add(sensor_id)
            
    #------------------------------------------------------
    # Loop through printing out the records
    #------------------------------------------------------
    for sensor_id in sensor_ids:
        #print(f'Sensors {sensor_id}')
        for device_id in devices.keys():
            device = devices[device_id]
            if( device.hasSensor(sensor_id) ):
                timestamps, values = device.getSensorTimestamps( sensor_id )
                #plt.plot(timestamps, values, label=device_id)
                plt.scatter(timestamps, values, label=device_id)
            
        plt.xlabel('timestamp')
        plt.xticks(rotation=90)
        plt.ylabel(f'{sensor_id}')
        plt.title(f'{sensor_id} Sensor')
        plt.legend()
        plt.show()

    stdio.println('Number of devices ' + str(len(devices)) )
    #stdio.println('Number of sensors  ' + str(len(device)) )


# Bootstrap, run this if script to python exe
if __name__ == '__main__':
    main()
