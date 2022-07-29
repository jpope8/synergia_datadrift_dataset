"""
TimeZone conversion in Python is terrible.  Though real issue is ausearch.

Given tau as a number of seconds, ausearch converts into local dt.  Irritating.

ausearch converts tau -> (utc time + timezone offset)

We want to convert (utc time + timezone offset) -> tau

If we simply (utc time + timezone offset).toseconds() we are off by an hour (because BST).
  One way is to convert epoch utc time to local time so we get
  (utc time + timezone offset) - (0 + timezone offset) = utc time


See
https://stackoverflow.com/questions/4563272/how-to-convert-a-utc-datetime-to-a-local-datetime-using-only-standard-library
https://www.saltycrane.com/blog/2009/05/converting-time-zones-datetime-objects-python/
https://www.kite.com/python/answers/how-to-convert-timezones-in-python

"""

from datetime import datetime, timedelta
from time import gmtime, strftime
import calendar
#import pytz

def epoch_to_datetime( epoch_time ):
    """
    Converts epoch time in seconds to datetime in Python.
    Not sure but suspect assumes UTC timezone (???).
    """
    dt = datetime.fromtimestamp( epoch_time )
    return dt


def utc_to_local(utc_dt):
    """
    Converts utc to timetuple so can be converted into local dt.
    The timetuple does not have microseconds so we have to copy over.
    """
    # get integer timestamp to avoid precision lost
    timestamp = calendar.timegm(utc_dt.timetuple())
    # The datetime.fromtimestamp converts to local dt
    # Return the local date corresponding to the POSIX timestamp, such as is returned by time.time().
    local_dt = datetime.fromtimestamp(timestamp)
    assert utc_dt.resolution >= timedelta(microseconds=1)
    return local_dt.replace(microsecond=utc_dt.microsecond)

EPOCH = datetime.utcfromtimestamp(0)
LOCAL_EPOCH = utc_to_local(EPOCH)

def local_to_epoch(local_dt):
    """
    Converts local dt to seconds since epoch.
    """
    #epoch = datetime.utcfromtimestamp(0)
    #localepoch = utc_to_local(epoch)
    # The (s - LOCAL_EPOCH) must return a timedelta object
    epochtime = (local_dt - LOCAL_EPOCH ).total_seconds()
    return epochtime

def str2epoch( strdatetime ):
    """
    Convenience method.  Equivalent to
    datetime.strptime(strdatetime, '%d/%m/%y %H:%M:%S.%f') - EPOCH
    This does not consider time zone.
    
    """
    s = datetime.strptime(strdatetime, '%d/%m/%y %H:%M:%S.%f')
    # The (s - EPOCH) must return a timedelta object
    return (s - EPOCH).total_seconds()

def main():
    """
    Test code
    """
    epoch = datetime.utcfromtimestamp(0)
    
    # 11/06/21 13:56:22.623    (This is BST though not included)
    # Expect: 1623416182.623

    # Actual: 1623419782.62    (off by 3600 seconds, 1 hour)

    # Sadly this is TimeZone not GMT, i.e. it is +1 hour (3600 seconds)
    # This is an issue if we want to recover the epoch time (which we do)
    # because we need to -3600 seconds to get back.
    #s = datetime.strptime('11/06/21 13:56:22.623 BST', '%d/%m/%y %H:%M:%S.%f %Z')
    #s = datetime.strptime('11/06/21 13:56:22.623', '%d/%m/%y %H:%M:%S.%f')
    print( str2epoch('11/06/21 13:56:22.623') )

    #epochtime = (s - utc_to_local(epoch) ).total_seconds()
    #localepoch = utc_to_local(epoch)
    #epochtime = (s - localepoch ).total_seconds()

    # Because 

    #print( local_to_epoch(s) )
    
if __name__ == '__main__':
    main()


#print(utc_to_local(datetime.utcnow()))
