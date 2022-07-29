"""
device.py

Device in data drift system.

Author: James Pope
"""
import datetime

#------------------------------------------------------

class Device:

    """
    Device represents tx/rx hardware with multiple sensors.
    """

    #------------------------------------------------------

    def __init__(self, device_id, aggregation_window=100, alpha=0.7):
        """
        Construct record.

        Parses the log (str line from audit file ).
        This class is immutable.
        The lineNumber is only used for debugging.
        """
        #self._datetimes = {} # dict mapping datetime -> dict, e.g. "1643879872" -> {"Temperature":20.32, "Humidity":41.74, ...}
        self._sensors = {} # dict mapping str -> dict, e.g. "Temperature" -> {1643879872:20.32, 1643879873:20.32, ...}
        self._deviceid  = device_id

        self._alpha = alpha
        self._aggregation_window = aggregation_window
        self._counts = {} # dict of count for aggregation sensor_id -> int
        self._ewmas = {} # dict of history ewma for given sensor  sensor_id -> float
    

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
        if( sensor_id not in self._ewmas ):
            self._ewmas[sensor_id] = value
            self._counts[sensor_id] = 0
            self._sensors[sensor_id] = dict()
        
        # Update count and ewma      
        count = self._counts[sensor_id]
        count = count + 1
        self._counts[sensor_id] = count

        old_value = self._ewmas[sensor_id]
        new_value = (self._alpha*value) + ( (1.0-self._alpha)*old_value )
        self._ewmas[sensor_id] = new_value

        # If count met, emit averaged sample
        if( count > self._aggregation_window ):
            self._counts[sensor_id] = 0
            sensors_values = self._sensors[sensor_id]
            sensors_values[datetime] = new_value

        #if( sensor_id not in self._sensors ):
        #    self._sensors[sensor_id] = dict()
        #sensors_values = self._sensors[sensor_id]
        #sensors_values[datetime] = value


    #------------------------------------------------------
    def getSensorIds(self):
        """
        Gets list with sensor names of this devices.
        """
        return self._sensors.keys()

    #------------------------------------------------------
    def getId(self):
        """
        Gets the device_id as a str.
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
    def __str__(self):
        #return 'Record type=' + self.getType() + ' ts=' + self.getTimestamp() + ' id=' + self.getId()
        msg = "Device " + self.getSensorIds()
        #for sensor_id in self.getSensorIds():
        #    
        #    val = self._fields.get(key)
        #    msg = msg + '\n    ' + key + ' -> ' + str(val)
        return msg

