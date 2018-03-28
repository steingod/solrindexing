#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    This is a wrapper around the Java SolR indexing tool. It id designed
    to simplify the process of indexing single or multiple datasets. In
    the current version it only supports one level of metadata as do the
    Java utility.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2017-11-09 

UPDATES:


NOTES:
    - Should support ingestion of directories as well...
    - Should support ingestion of two level dataset as well...
"""

import sys
import os.path
import getopt
import subprocess

def usage():
    print ''
    print 'Usage: '+sys.argv[0]+' -i <dataset_name> -c <core_name> [-h]' 
    print '\t-h: dump this text'
    print '\t-i: index an individual dataset'
    print '\t-d: index a directory with multiple datasets'
    print '\t-c: core name (e.g. normap, sios, nbs)'
    print '\t-t: index a single thumbnail (no argument, require -i or -d)'
    print '\t-f: index a single feature type (no argument, require -i or -d)'
    print ''
    sys.exit(2)

def main(argv):
    cflg = iflg = dflg = tflg = fflg = False
    try:
        opts, args = getopt.getopt(argv,"hi:d:c:tf",["ifile=", "ddir=", "core="])
    except getopt.GetoptError:
        print sys.argv[0]+' -i <inputfile>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-i", "--ifile"):
            infile = arg
            iflg = True
        elif opt in ("-d", "--ddir"):
            ddir = arg
            dflg = True
        elif opt in ("-c", "--core"):
            myCore = arg
            cflg = True
        elif opt in ("-t"):
            tflg = True
        elif opt in ("-f"):
            fflg = True

    if not cflg or (not iflg and not dflg):
        usage()

    SolrServer = 'http://157.249.176.182:8080/solr/'
    # Must be fixed when supporting multiple levels
    mySolRl1 = SolrServer + myCore + "-l1" 
    mySolRtn = SolrServer + myCore + "-thumbnail" 
    #print mySolRl1 + "\n" + mySolRtn
    #sys.exit(2)

    # Find files to process
    if (iflg):
        myfiles = [infile]
    else:
        try:
            myfiles = os.listdir(ddir)
        except os.error:
            print os.error
            sys.exit(1)

    for myfile in myfiles:
        myfile = os.path.join(ddir,myfile)
        # Index files
        print "Indexing a single file in "+mySolRl1
        if not os.path.isfile(myfile):
            print myfile+" does not exist"
            sys.exit(1)
        myproc = subprocess.call(['/usr/bin/java',
            '-jar','metsis-metadata-jar-with-dependencies.jar',
            'index-single-metadata',
            '--level', 'l1', '--metadataFile', myfile, '--server', mySolRl1])
        print "Return value: " + str(myproc)
        if tflg:
            print "Indexing a single thumbnail in "+mySolRtn
            myproc = subprocess.call(['/usr/bin/java',
                '-jar','metsis-metadata-jar-with-dependencies.jar',
                'index-single-thumbnail',
                '--metadataFile', myfile, '--server', mySolRtn, 
                '--wmsVersion', '1.3.0'])
            #print "Thumbnail indexing: " + mySolRtn
            print "Return value: " + str(myproc)
        if fflg:
            print "Indexing a single feature type in "+mySolRtn
            myproc = subprocess.call(['/usr/bin/java',
                '-jar','metsis-metadata-jar-with-dependencies.jar',
                'index-single-feature',
                '--metadataFile', myfile, '--server', mySolRtn])
            print "Return value: " + str(myproc)


if __name__ == "__main__":
    main(sys.argv[1:])
