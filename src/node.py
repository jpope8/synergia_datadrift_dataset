"""
node.py

Device in data drift system.  The device.py aggregates the samples (otherwise out of memory).
This implementation is essentially the same as Device but does not aggregate.

Author: James Pope
"""
import datetime

#------------------------------------------------------

class Segment:
    def __init__(self, timestamp_start, timestamp_stop):
        self._timestamp_start = timestamp_start
        self._timestamp_stop = timestamp_stop
        self._total = 0.0
        self._count = 0
    def add( self, value ):
        self._total += value
        self._count += 1
    def contains(self, timestamp):
        return timestamp >= self._timestamp_start and timestamp < self._timestamp_stop
    def count( self ):
        return self._count
    def avg( self ):
        return self._total / self._count

class Node:

    """
    Node represents tx/rx hardware with multiple sensors.
    """

    #------------------------------------------------------

    def __init__(self, node_id):
        """
        Construct record.

        Parses the log (str line from audit file ).
        This class is immutable.
        The lineNumber is only used for debugging.
        """
        self._sensors = {} # dict mapping str -> dict, e.g. "Temperature" -> {1643879872:20.32, 1643879873:20.32, ...}
        self._nodeid  = node_id
    

    #------------------------------------------------------
    def __len__(self):
        """
        Return number of sensors for this device.
        """
        return len(self._sensors)


    #------------------------------------------------------
    def add(self, sensor_id, datetime, value):
        """
        Adds sensor_id and value to this device at the datetime.
        """
        if( sensor_id not in self._sensors ):
            self._sensors[sensor_id] = dict()
        
        sensors_values = self._sensors[sensor_id]
        sensors_values[datetime] = value


    #------------------------------------------------------
    def getSensorIds(self):
        """
        Gets list with sensor names of this devices.
        """
        return self._sensors.keys()

    #------------------------------------------------------
    def getId(self):
        """
        Gets the node_id as a str.
        """
        return self._deviceid

    #------------------------------------------------------
    def getSensorValues(self, sensor_id):
        """
        Gets dict of timestamps -> value for the given sensor.
        """
        return self._sensors[sensor_id]

    #------------------------------------------------------
    def hasSensor(self, sensor_id):
        """
        Returns True if this device has sensor_id, False otherwise.
        """
        return sensor_id in self._sensors


    #------------------------------------------------------
    def getSensorTimestamps(self, sensor_id):
        """
        Gets list of timestamps and list of values for the given sensor.
        return: timestamp_list, values_list
        """
        timestamp_list = list()
        values_list    = list()
        sensor_dict = self._sensors[sensor_id]
        for timestamp in sorted( sensor_dict.keys() ):
            value = sensor_dict[timestamp]
            timestamp_list.append(timestamp)
            values_list.append(value)
            
        return timestamp_list, values_list



    #------------------------------------------------------
    def getSensorValue(self, sensor_id, timestamp):
        """
        Gets value associated with timestamp for given sensor_id (or None if timestamp not found).
        return: value or None
        """
        value = None
        sensor_dict = self._sensors[sensor_id]
        if timestamp in sensor_dict:
            value = sensor_dict[timestamp]
        return value


    #------------------------------------------------------
    def averageSensorValues(self, sensor_id, timedelta, timestamp_start=None):
        """
        Gets values from sensor_id and average over the given timedelta.  This is done in a linear, efficient way.
        return: values
        """
        timestamps, values = self.getSensorTimestamps(sensor_id)
        timestamp_first =timestamps[0] 
        timestamp_last = timestamps[-1]

        if( timestamp_start is None ):
            timestamp_start = timestamp_first

        # First create all the segments
        segments = dict()
        while timestamp_start < timestamp_last:
            timestamp_stop = timestamp_start + timedelta # timedelta(hours=1)

            # Store the total and counts alond with end of segment
            segment = Segment(timestamp_start, timestamp_stop)
            segments[timestamp_start] = segment

            # Move start for next range
            timestamp_start = timestamp_stop
        
        # Assign the timestamp-values to their respective segment
        for i in range( len(timestamps) ):
            timestamp = timestamps[i]
            value     = values[i]
            # Find the segment this timestamp falls in
            for timestamp_start in segments:
                segment = segments[timestamp_start]
                if( segment.contains( timestamp ) ):
                    segment.add(value)
        
        # Compute average for each segment and add to dict for return
        sensor_values = dict()
        for timestamp_start in segments:
            segment = segments[timestamp_start]
            if( segment.count() > 0 ): sensor_values[timestamp_start] = segment.avg()
            else:                      sensor_values[timestamp_start] = None
            
        return sensor_values

        """
        while timestamp_start < timestamp_last and i < len(values):

            timestamp_stop = timestamp_start + timedelta # timedelta(hours=1)

            if(i < 10):
                value_not_in_range = timestamps[i] < timestamp_start or timestamps[i] >= timestamp_stop
                print(f"NODE {self._nodeid} {timestamp_start} < {timestamps[i]} < {timestamp_stop} is {value_not_in_range}")

            
            # Only proceed if we have a sample, otherwise add None to indicate empty range
            if( timestamps[i] < timestamp_start or timestamps[i] >= timestamp_stop ):
                sensor_values[timestamp_start] = None
            elif( i < len(values) ):
                total = 0.0
                count = 0
                while( i < len(values) and timestamps[i] >= timestamp_start and timestamps[i] < timestamp_stop ):
                    timestamp = timestamps[i]
                    value     = values[i]
                    i = i + 1

                    total = total + value
                    count = count + 1
                avg_value = total / count
                # Might be better to use midpoint timestamp
                sensor_values[timestamp_start] = avg_value

            # Move start for next range
            timestamp_start = timestamp_stop

        return sensor_values
        """

    #------------------------------------------------------
    def minTimestamp(self, sensor_id):
        """
        Gets minimum timestamp.
        return: min
        """
        sensor_dict = self._sensors[sensor_id]
        return min( sensor_dict.keys() )

    #------------------------------------------------------
    def __str__(self):
        #return 'Record type=' + self.getType() + ' ts=' + self.getTimestamp() + ' id=' + self.getId()
        msg = "Device " + self.getSensorIds()
        #for sensor_id in self.getSensorIds():
        #    
        #    val = self._fields.get(key)
        #    msg = msg + '\n    ' + key + ' -> ' + str(val)
        return msg

