#!/usr/bin/env python3
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
from requests.auth import HTTPBasicAuth

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-c","--cfg",dest="cfgfile",
            help="Configuration file", required=True)
    parser.add_argument("-s","--searchstringst",dest="string",
            help="String to search for", required=True)
    parser.add_argument('-d','--delete', action='store_true', help="Flag to delete records")
    parser.add_argument('-a','--always_commit', action='store_true', help="Flag to commit directly")

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

    def __init__(self, mysolrserver, commit=False, authentication=None):
        # Set up logging
        self.logger = logging.getLogger('searchindex.IndexMMD')
        self.logger.info('Creating an instance of IndexMMD')
        """
        Connect to SolR core
        """
        try:
            self.solrc = pysolr.Solr(mysolrserver, always_commit=commit, timeout=1020, auth=authentication)
        except Exception as e:
            print("Something failed in SolR init", str(e))
        print("Connection established to: " + str(mysolrserver))

        # Test connection to server
        self.logger.info('Testing SolR client')
        try:
            mypingresult = self.solrc.ping()
        except Exception as e:
            self.logger.error('Could not reach the SolR server: %s', e)
            raise SystemExit()

    def delete_item(self, datasetid, commit):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        print("Deleting ", datasetid, " from Level 1")
        try:
            self.solrc.delete(id=datasetid)
        except Exception as e:
            print("Something failed in SolR delete", str(e))

        print("Record successfully deleted from core")

    def search(self, myargs):
        """ Require Id as input """
        try:
            results = self.solrc.search(myargs.string,**{'wt':'python','rows':100000})
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

    mySolRc = SolrServer+myCore
    # check for authentication
    if 'auth-basic-username' in cfg and 'auth-basic-password' in cfg:
        username = cfg['auth-basic-username']
        password = cfg['auth-basic-password']
        if username == '' or password == '':
            raise Exception('Authentication username and/or password are configured,'
                            'but have blank strings')
        else:
            authentication = HTTPBasicAuth(username, password)
    else:
        authentication = None


    # Search for records
    mysolr = IndexMMD(mySolRc, args.always_commit, authentication)
    myresults = mysolr.search(args)
    #print(dir(myresults))
    print('Found %d matches' % myresults.hits)
    print('Looping through matches:')
    i=0
    for doc in myresults:
        print('\t', i, doc['id'])
        deleteid = doc['id']
        if args.delete:
            mysolr.delete_item(deleteid, commit=None)
        i+=1
    print('Found %d matches' % myresults.hits)

    return

if __name__ == "__main__":
    main(sys.argv[1:])
