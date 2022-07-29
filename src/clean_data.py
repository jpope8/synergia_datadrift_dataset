"""
clean_data.py

Removes all unzipped csv files from the data directory.

Author: James Pope
"""

import sys
import stdio
import os

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

def main():
    """
    Main entry point.

    The command-line argument should be the name of the
    function that should be called.
    """
    argslen = len(sys.argv)
    if( argslen != 2 ):
        print("  Usage: <str: parent folder of csv and zip files>")
        print("Example: ../data/")
        return

    path = sys.argv[1]

    #------------------------------------------------------
    # We are careful to only delete csv files with mathing zip files
    # Read any zip files and unzip if not already
    # NB: Week 20220207_Week and 20220613_Week does not match zip and csv file :(
    #------------------------------------------------------
    # Get all the current csv file names
    # Ignore full path as unziped paths are not consistent
    csv_files = set()
    for r, d, f in os.walk(path):
        for afile in f:
            if( ".csv" in afile ):
                csv_files.add(afile) # afile is just filename not the full path
    #print(csv_files)
    # Now find all zip file names, if there is a corresponding csv, then delete it
    for r, d, f in os.walk(path):
        for afile in f:
            if( ".zip" in afile ):
                csvfilename = afile.replace(".zip", ".csv")
                if( csvfilename in csv_files ):
                    csvfile = os.path.join(r, csvfilename)
                    zipfile = os.path.join(r, afile)
                    if( os.path.exists(csvfile) ):
                        print( "DELETING: " + str(csvfile) )
                        os.remove( csvfile )
                        #print( "ZIP: " + str(zipfile) )
                        #print( " " )
    print( "CLEAN COMPLETE" )

# Bootstrap, run this if script to python exe
if __name__ == '__main__':
    main()
