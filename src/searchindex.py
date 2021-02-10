#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    This searches SolR for specific records and optionally deletes them. It can also optionally create a list of identifiers to delete. Search is done in ID for now.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2021-02-10 

UPDATES:

NOTES:
    - under rewrite...

"""

import sys
import os.path
import argparse
import subprocess
import pysolr
import xmltodict
import dateutil.parser
import warnings
import json
import yaml
from collections import OrderedDict
from owslib.wms import WebMapService
import base64

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-c","--cfg",dest="cfgfile",
            help="Configuration file", required=True)
    parser.add_argument("-s","--searchstringst",dest="string",
            help="String to search for", required=True)
    parser.add_argument('-2','--level2', action='store_true', help="Flag to search in level 2 core")
    parser.add_argument('-t','--thumbnail', action='store_true', help="Flag to search in thumbnail core")

    args = parser.parse_args()

    if args.cfgfile is None or args.string is None:
        parser.print_help()
        parser.exit()

    return args

def parse_cfg(cfgfile):
    # Read config file
    print("Reading", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)

    return cfgstr

class IndexMMD:
    """ requires a list of dictionaries representing MMD as input """

    def __init__(self, mysolrserver):
        """
        Connect to SolR cores

        The thumbnail core should be removed in the future and elements
        added to the ordinary L1 and L2 cores. It could be that only one
        core could be used eventually as parent/child relations are
        supported by SolR.
        """
        # Connect to L1
        try:
            self.solr1 = pysolr.Solr(mysolrserver, always_commit=True)
        except Exception as e:
            print("Something failed in SolR init", str(e))
        print("Connection established to: " + str(mysolrserver))

        # Connect to L2
        mysolrserver2 = mysolrserver.replace('-l1', '-l2')
        try:
            self.solr2 = pysolr.Solr(mysolrserver2, always_commit=True)
        except Exception as e:
            print("Something failed in SolR init", str(e))
        print("Connection established to: " + str(mysolrserver2))

        # Connect to thumbnail
        mysolrservert = mysolrserver.replace('-l1', '-thumbnail')
        try:
            self.solrt = pysolr.Solr(mysolrservert, always_commit=True)
        except Exception as e:
            print("Something failed in SolR init", str(e))
        print("Connection established to: " + str(mysolrservert))

    def delete_level1(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        print("Deleting ", datasetid, " from Level 1")
        try:
            self.solr1.delete(id=datasetid)
        except Exception as e:
            print("Something failed in SolR delete", str(e))

        print("Records successfully deleted from Level 1 core")

    def delete_level2(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        print("Deleting ", datasetid, " from Level 2")
        try:
            self.solr2.delete(id=datasetid)
        except Exception as e:
            print("Something failed in SolR delete", str(e))

        print("Records successfully deleted from Level 2 core")

    def delete_thumbnail(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        print("Deleting ", datasetid, " from thumbnail")
        try:
            self.solrt.delete(id=datasetid)
        except Exception as e:
            print("Something failed in SolR delete", str(e))

        print("Records successfully deleted from thumbnail core")

    def search(self, myargs):
        """ Require Id as input """
        try:
            if myargs.level2:
                results = self.solr2.search(myargs.string,**{'wt':'python','rows':100000})
            elif myargs.thumbnail:
                results = self.solrt.search(myargs.string,**{'wt':'python','rows':100000})
            else:
                results = self.solr1.search(myargs.string,**{'wt':'python','rows':100000})
        except Exception as e:
            print("Something failed: ", str(e))

        return results


def main(argv):

    # Parse command line arguments
    try:
        args = parse_arguments()
    except:
        raise SystemExit('Command line arguments didn\'t parse correctly.')

    # Parse configuration file
    cfg = parse_cfg(args.cfgfile)

    SolrServer = cfg['solrserver']
    myCore = cfg['solrcore']

    mySolRc = SolrServer+myCore+'-l1'

    # Search for records
    mysolr = IndexMMD(mySolRc)
    myresults = mysolr.search(args)
    print(dir(myresults))
    print(myresults.hits)
    i=0
    for doc in myresults:
        print(i, doc['id'])
        i+=1
    #print(myresults.docs)

    sys.exit()
    # Find files to process
    if iflg:
        myfiles = [infile]
    elif lflg:
        f2 = open(infile, "r")
        myfiles = f2.readlines()
        f2.close()
    elif rflg:
        mysolr = IndexMMD(mySolRc)
        mysolr.delete_level1(deleteid)
        sys.exit()
    elif rflg and l2flg:
        mysolr = IndexMMD(mySolRc)
        mysolr.delete_level2(deleteid)
        sys.exit()
    elif rflg:
        mysolr = IndexMMD(mySolRc)
        mysolr.delete_level(deleteid)
        sys.exit()
    elif dflg:
        try:
            myfiles = os.listdir(ddir)
        except Exception as e:
            print("Something went wrong in decoding cmd arguments: " + str(e))
            sys.exit(1)

    return

if __name__ == "__main__":
    main(sys.argv[1:])
