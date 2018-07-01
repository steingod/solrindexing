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
    Øystein Godøy, METNO/FOU, 2018-04-19
        Added support for level 2

NOTES:
    - Should support ingestion of directories as well...
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
    print '\t-l: index individual datasetis from list file'
    print '\t-d: index a directory with multiple datasets'
    print '\t-c: core name (e.g. normap, sios, nbs)'
    print '\t-2: index level 2 dataset'
    print '\t-t: index a single thumbnail (no argument, require -i or -d)'
    print '\t-f: index a single feature type (no argument, require -i or -d)'
    print ''
    sys.exit(2)

def main(argv):

    mylog = "mylogfile.txt"
    try:
        f = open(mylog,"w")
    except OSError as e:
        print e

    cflg = iflg = dflg = tflg = fflg = lflg = l2flg = False
    try:
        opts, args = getopt.getopt(argv,"hi:d:c:l:tf2",["ifile=", "ddir=",
            "core=", "list="])
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
        elif opt in ("-l"):
            infile = arg
            lflg = True
        elif opt in ("-2"):
            l2flg = True
        elif opt in ("-t"):
            tflg = True
        elif opt in ("-f"):
            fflg = True

    if not cflg or (not iflg and not dflg and not lflg):
        usage()

    if l2flg:
        myLevel = "l2"
    else:
        myLevel = "l1"

    SolrServer = 'http://157.249.176.182:8080/solr/'
    # Must be fixed when supporting multiple levels
    if l2flg:
        mySolRc = SolrServer + myCore + "-l2" 
    else:
        mySolRc = SolrServer + myCore + "-l1" 
    mySolRtn = SolrServer + myCore + "-thumbnail" 
    #print mySolRc + "\n" + mySolRtn
    #sys.exit(2)

    # Find files to process
    if (iflg):
        myfiles = [infile]
    elif (lflg):
        f2 = open(infile,"r")
        myfiles = f2.readlines()
        f2.close()
    else:
        try:
            myfiles = os.listdir(ddir)
        except os.error:
            print os.error
            sys.exit(1)

    if dflg and l2flg:
        # Until the indexing utility actually works as expected...
        print "Indexing a Level 2 directory in "+mySolRc
        f.write("\n======\nIndexing "+ ddir)
        myproc = subprocess.check_output(['/usr/bin/java',
            '-jar','metsis-metadata-jar-with-dependencies.jar',
            'index-metadata',
            '--sourceDirectory', ddir, 
            '--server', mySolRc,
            '--level', myLevel,
            '--includeRelatedDataset', 'true'])
        f.write(myproc)
        if tflg:
            print "Indexing a single thumbnail in "+mySolRtn
            myproc = subprocess.check_output(['/usr/bin/java',
                '-jar','metsis-metadata-jar-with-dependencies.jar',
                'index-thumbnail',
                '--sourceDirectory', ddir, '--server', mySolRtn, 
                '--wmsVersion', '1.3.0'])
            print "Return value: " + str(myproc)
            f.write(myproc)
        if fflg:
            print "Indexing a single feature type in "+mySolRtn
            myproc = subprocess.check_output(['/usr/bin/java',
                '-jar','metsis-metadata-jar-with-dependencies.jar',
                'index-feature',
                '--sourceDirectory', ddir, '--server', mySolRtn])
            print "Return value: " + str(myproc)
            f.write(myproc)
    else:
        for myfile in myfiles:
            if lflg:
                myfile = myfile.rstrip()
            if dflg:
                myfile = os.path.join(ddir,myfile)
            # Index files
            print "Indexing a single file in "+mySolRc
            f.write("\n======\nIndexing "+ myfile)
            if not os.path.isfile(myfile):
                print myfile+" does not exist"
                sys.exit(1)
            myproc = subprocess.check_output(['/usr/bin/java',
                '-jar','metsis-metadata-jar-with-dependencies.jar',
                'index-single-metadata',
                '--level', myLevel, '--metadataFile', myfile, '--server', mySolRc])
            f.write(myproc)
            #print "Return value: " + str(myproc)
            if tflg:
                print "Indexing a single thumbnail in "+mySolRtn
                myproc = subprocess.check_output(['/usr/bin/java',
                    '-jar','metsis-metadata-jar-with-dependencies.jar',
                    'index-single-thumbnail',
                    '--metadataFile', myfile, '--server', mySolRtn, 
                    '--wmsVersion', '1.3.0'])
                #print "Thumbnail indexing: " + mySolRtn
                #print "Return value: " + str(myproc)
                f.write(myproc)
            if fflg:
                print "Indexing a single feature type in "+mySolRtn
                myproc = subprocess.check_output(['/usr/bin/java',
                    '-jar','metsis-metadata-jar-with-dependencies.jar',
                    'index-single-feature',
                    '--metadataFile', myfile, '--server', mySolRtn])
                #print "Return value: " + str(myproc)
                f.write(myproc)
            f.write(myproc)

    # Report status
    f.write("Number of files processed were:" + str(len(myfiles)))
    f.close()


if __name__ == "__main__":
    main(sys.argv[1:])
