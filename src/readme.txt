The Python code is intended to parse log files produced by the data drift system.
The systems consists of 8-10 devices each with ~8 sensors that sample once per second.

The main method is in parse.py and typical usage is as follows:

  python parse.py ./three_devices/

The example format of the log files is as follows:

Time,DeviceId,Sensor,Value
1643879872,f6ce368d7563b285,Temperature,20.326000213623047
1643879872,f6ce368d7563b285,Humidity,41.74300003051758
1643879872,f6ce368d7563b285,Pressure,100958.0
1643879872,f6ce368d7563b285,Gas,500.0
1643879872,f6ce368d7563b285,Accelerometer,0
1643879872,f6ce368d7563b285,Light,560
1643879872,f6ce368d7563b285,MIC,1
1643879872,f6ce368d7563b285,RSSI,55
1643879882,f6ce368d7563b285,Temperature,20.326000213623047
1643879882,f6ce368d7563b285,Humidity,41.7599983215332
1643879882,f6ce368d7563b285,Pressure,100956.0
1643879882,f6ce368d7563b285,Gas,501.5889892578125
1643879882,f6ce368d7563b285,Accelerometer,0
1643879882,f6ce368d7563b285,Light,571
1643879882,f6ce368d7563b285,MIC,1
1643879882,f6ce368d7563b285,RSSI,55

