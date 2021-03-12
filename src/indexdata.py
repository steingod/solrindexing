#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    This is designed to simplify the process of indexing single or multiple datasets. 

AUTHOR:
    Øystein Godøy, METNO/FOU, 2017-11-09

UPDATES:
    Øystein Godøy, METNO/FOU, 2019-05-31
        Integrated modifications from Trygve Halsne and Massimo Di Stefano
    Øystein Godøy, METNO/FOU, 2018-04-19
        Added support for level 2
    Øystein Godøy, METNO/FOU, 2021-02-19
        Added argparse, fixing robustness issues.

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
import cartopy.crs as ccrs
import cartopy
import matplotlib.pyplot as plt
from owslib.wms import WebMapService
import base64
import netCDF4
import logging
from logging.handlers import TimedRotatingFileHandler

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-c','--cfg',dest='cfgfile', help='Configuration file', required=True)
    parser.add_argument('-i','--input_file',help='Individual file to be ingested.')
    parser.add_argument('-l','--list_file',help='File with datasets to be ingested specified.')
    parser.add_argument('-d','--directory',help='Directory to ingest')
    parser.add_argument('-t','--thumbnail',help='Create and index thumbnail, do not update the main content.', action='store_true')
    parser.add_argument('-n','--no_thumbnail',help='Do not index thumbnails (normally done automatically if WMS available).', action='store_true')
    parser.add_argument('-f','--feature_type',help='Extract featureType during ingestion (to be done automatically).', action='store_true')
    parser.add_argument('-r','--remove',help='Remove the dataset with the specified identifier (to be replaced by searchindex).')
    parser.add_argument('-2','--level2',help='Operate on child core.')
    args = parser.parse_args()

    if args.cfgfile is None:
        parser.print_help()
        parser.exit()
    if not args.input_file and not args.directory and not args.list_file and not args.remove:
        parser.print_help()
        parser.exit()

    return args

def parse_cfg(cfgfile):
    # Read config file
    print("Reading", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)

    return cfgstr

def initialise_logger(outputfile, name):
    # Check that logfile exists
    logdir = os.path.dirname(outputfile)
    if not os.path.exists(logdir):
        try:
            os.makedirs(logdir)
        except:
            raise IOError
    # Set up logging
    mylog = logging.getLogger(name)
    mylog.setLevel(logging.INFO)
    #logging.basicConfig(level=logging.INFO, 
    #        format='%(asctime)s - %(levelname)s - %(message)s')
    myformat = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(myformat)
    mylog.addHandler(console_handler)
    file_handler = logging.handlers.TimedRotatingFileHandler(
            outputfile,
            when='w0',
            interval=1,
            backupCount=7)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(myformat)
    mylog.addHandler(file_handler)

    return(mylog)

class MMD4SolR:
    """ Read and check MMD files, convert to dictionary """

    def __init__(self, filename):
        # Set up logging
        self.logger = logging.getLogger('indexdata.MMD4SolR')
        self.logger.info('Creating an instance of IndexMMD')
        """ set variables in class """
        self.filename = filename
        try:
            with open(self.filename, encoding='utf-8') as fd:
                self.mydoc = xmltodict.parse(fd.read())
        except Exception as e:
            self.logger.error('Could not open file: %s',self.filename)
            raise

    def check_mmd(self):
        """ Check and correct MMD if needed """
        """ Remember to check that multiple fields of abstract and title
        have set xml:lang= attributes... """

        """
        Check for presence of required elements
        Temporal and spatial extent are not required as of no as it will
        break functionality for some datasets and communities especially
        in the Arctic context.
        """
        # TODO add proper docstring
        mmd_requirements = {
            'mmd:metadata_version': False,
            'mmd:metadata_identifier': False,
            'mmd:title': False,
            'mmd:abstract': False,
            'mmd:metadata_status': False,
            'mmd:dataset_production_status': False,
            'mmd:collection': False,
            'mmd:last_metadata_update': False,
            'mmd:iso_topic_category': False,
            'mmd:keywords': False,
        }
        """
        Check for presence and non empty elements
        This must be further developed...
        """
        for requirement in mmd_requirements.keys():
            if requirement in self.mydoc['mmd:mmd']:
                self.logger.info('\n\tChecking for: %s',requirement)
                if requirement in self.mydoc['mmd:mmd']:
                    if self.mydoc['mmd:mmd'][requirement] != None:
                        self.logger.info('\n\t%s is present and non empty',requirement)
                        mmd_requirements[requirement] = True
                    else:
                        self.logger.warning('\n\tRequired element %s is missing, setting it to unknown',requirement)
                        self.mydoc['mmd:mmd'][requirement] = 'Unknown'
                else:
                    self.logger.warning('\n\tRequired element %s is missing, setting it to unknown.',requirement)
                    self.mydoc['mmd:mmd'][requirement] = 'Unknown'

        """
        Check for correct vocabularies where necessary
        Change to external files (SKOS), using embedded files for now
        Should be collected from
            https://github.com/steingod/scivocab/tree/master/metno
        """
        mmd_controlled_elements = {
            'mmd:iso_topic_category': ['farming',
                                       'biota',
                                       'boundaries',
                                       'climatologyMeteorologyAtmosphere',
                                       'economy',
                                       'elevation',
                                       'environment',
                                       'geoscientificInformation',
                                       'health',
                                       'imageryBaseMapsEarthCover',
                                       'inlandWaters',
                                       'location',
                                       'oceans',
                                       'planningCadastre',
                                       'society',
                                       'structure',
                                       'transportation',
                                       'utilitiesCommunication'],
            'mmd:collection': ['ACCESS',
                               'ADC',
                               'APPL',
                               'CC',
                               'DAM',
                               'DOKI',
                               'GCW',
                               'NBS',
                               'NMAP',
                               'NMDC',
                               'NSDN',
                               'SIOS',
                               'SESS_2018',
                               'SESS_2019',
                               'SIOS_access_programme', 
                               'YOPP'],
            'mmd:dataset_production_status': ['Planned',
                                              'In Work',
                                              'Complete',
                                              'Obsolete'],
        }
        for element in mmd_controlled_elements.keys():
            self.logger.info('\n\tChecking %s\n\tfor compliance with controlled vocabulary', element)
            if element in self.mydoc['mmd:mmd']:
                #print('Found',element,'in document')
                if isinstance(self.mydoc['mmd:mmd'][element], list):
                    for elem in self.mydoc['mmd:mmd'][element]:
                        #print('>>>',elem)
                        if isinstance(elem,dict):
                            myvalue = elem['#text']
                        else:
                            myvalue = elem
                        if myvalue not in mmd_controlled_elements[element]:
                            if myvalue is not None:
                                self.logger.warning('\n\t%s contains non valid content: \n\t\t%s', element, myvalue)
                            else:
                                self.logger.warning('Discovered an empty element.')
                else:
                    if isinstance(self.mydoc['mmd:mmd'][element],dict):
                        myvalue = self.mydoc['mmd:mmd'][element]['#text']
                    else:
                        myvalue = self.mydoc['mmd:mmd'][element]
                    if myvalue not in mmd_controlled_elements[element]:
                        self.logger.warning('\n\t%s contains non valid content: \n\t\t%s', element, myvalue)

        """
        Check that keywords also contain GCMD keywords
        Need to check contents more specifically...
        """
        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], list):
            i = 0
            gcmd = False
            ##print(type(self.mydoc['mmd:mmd']['mmd:keywords']))
            ##print(len(self.mydoc['mmd:mmd']['mmd:keywords']))
            # TODO: remove unused for loop
            # Switch to using e instead of self.mydoc...
            for e in self.mydoc['mmd:mmd']['mmd:keywords']:
                if str(self.mydoc['mmd:mmd']['mmd:keywords'][i]).upper() == 'GCMD':
                    gcmd = True
                    break
                i += 1
            if not gcmd:
                self.logger.warning('\n\tKeywords in GCMD are not available')
        else:
            if not str(self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary']).upper() == 'GCMD':
                # warnings.warning('Keywords in GCMD are not available')
                self.logger.warning('\n\tKeywords in GCMD are not available')

        """ 
        Modify dates if necessary 
        Adapted for the new MMD specification, but not all information is
        extracted as SolR is not adapted.
        """
        if 'mmd:last_metadata_update' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:last_metadata_update'],
                    dict):
                for mydict in self.mydoc['mmd:mmd']['mmd:last_metadata_update'].items():
                    if 'mmd:update' in mydict:
                        for myupdate in mydict:
                            if 'mmd:update' not in myupdate:
                                mydateels = myupdate
                                # The comparison below is a hack, need to
                                # revisit later, but works for now.
                                myvalue = '0000-00-00:T00:00:00Z'
                                if isinstance(mydateels,list):
                                    for mydaterec in mydateels:
                                        if mydaterec['mmd:datetime'] > myvalue:
                                            myvalue = mydaterec['mmd:datetime']
                                else:
                                    myvalue = mydateels['mmd:datetime']

            else:
                # To be removed when all records are transformed into the
                # new format
                myvalue = self.mydoc['mmd:mmd']['mmd:last_metadata_update']
            mydate = dateutil.parser.parse(myvalue)
            self.mydoc['mmd:mmd']['mmd:last_metadata_update'] = mydate.strftime('%Y-%m-%dT%H:%M:%SZ')
        if 'mmd:temporal_extent' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:temporal_extent'], list):
                #print(self.mydoc['mmd:mmd']['mmd:temporal_extent'])
                i=0
                for item in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                    #print(i, item)
                    for mykey in  item:
                        #print('\t', mykey,item[mykey])
                        if (item[mykey]==None) or (item[mykey]=='--'):
                            mydate = ''
                            self.mydoc['mmd:mmd']['mmd:temporal_extent'][i][mykey] = mydate
                        else:
                            mydate = dateutil.parser.parse(str(item[mykey]))
                            self.mydoc['mmd:mmd']['mmd:temporal_extent'][i][mykey] = mydate.strftime('%Y-%m-%dT%H:%M:%SZ')
                    i += 1
            else:
                for mykey in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                    if mykey == '@xmlns:gml':
                        continue
                    if (self.mydoc['mmd:mmd']['mmd:temporal_extent'][mykey] == None) or (self.mydoc['mmd:mmd']['mmd:temporal_extent'][mykey] == '--'): 
                        mydate = ''
                        self.mydoc['mmd:mmd']['mmd:temporal_extent'][mykey] = mydate
                    else:
                        try:
                            mydate = dateutil.parser.parse(str(self.mydoc['mmd:mmd']['mmd:temporal_extent'][mykey]))
                            self.mydoc['mmd:mmd']['mmd:temporal_extent'][mykey] = mydate.strftime('%Y-%m-%dT%H:%M:%SZ')
                        except Exception as e:
                            self.logger.error('Date format could not be parsed: %s', e)

    def tosolr(self):
        """ 
        Collect required elements 
        """
        mydict = OrderedDict()

        # SolR Can't use the mmd:metadata_identifier as identifier if it contains :, replace : by _ in the id field, let mmd_metadata_identifier be the correct one.

        """ Identifier """
        if isinstance(self.mydoc['mmd:mmd']['mmd:metadata_identifier'],dict):
            mydict['id'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier']['#text'].replace(':','_')
            mydict['mmd_metadata_identifier'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier']['#text']
        else:
            mydict['id'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier'].replace(':','_')
            mydict['mmd_metadata_identifier'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier']
        
        """ Metadata status """
        if isinstance(self.mydoc['mmd:mmd']['mmd:metadata_status'],dict):
            mydict['mmd_metadata_status'] = self.mydoc['mmd:mmd']['mmd:metadata_status']['#text']
        else:
            mydict['mmd_metadata_status'] = self.mydoc['mmd:mmd']['mmd:metadata_status']
        # TODO: the string below [title, abstract, etc ...]
        #  should be comments or some sort of logging statments
        """ title """
        if isinstance(self.mydoc['mmd:mmd']['mmd:title'], list):
            i = 0
            # TODO: remove unused for loop
            # Switch to using e instead of self.mydoc...
            for e in self.mydoc['mmd:mmd']['mmd:title']:
                if self.mydoc['mmd:mmd']['mmd:title'][i]['@xml:lang'] == 'en':
                    mydict['mmd_title'] = self.mydoc['mmd:mmd']['mmd:title'][i]['#text'].encode('utf-8')
                i += 1
        else:
            if isinstance(self.mydoc['mmd:mmd']['mmd:title'],dict):
                if self.mydoc['mmd:mmd']['mmd:title']['@xml:lang'] == 'en':
                    mydict['mmd_title'] = self.mydoc['mmd:mmd']['mmd:title']['#text']

            else:
                mydict['mmd_title'] = str(self.mydoc['mmd:mmd']['mmd:title'])

        """ abstract """
        if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'], list):
            i = 0
            for e in self.mydoc['mmd:mmd']['mmd:abstract']:
                if self.mydoc['mmd:mmd']['mmd:abstract'][i]['@xml:lang'] == 'en':
                    mydict['mmd_abstract'] = self.mydoc['mmd:mmd']['mmd:abstract'][i]['#text'].encode('utf-8')
                i += 1
        else:
            if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'],dict):
                if self.mydoc['mmd:mmd']['mmd:abstract']['@xml:lang'] == 'en':
                    mydict['mmd_abstract'] = self.mydoc['mmd:mmd']['mmd:abstract']['#text']

            else:
                mydict['mmd_abstract'] = str(self.mydoc['mmd:mmd']['mmd:abstract'])

        """ Last metadata update """
        if 'mmd:last_metadata_update' in self.mydoc['mmd:mmd']:
            mydict['mmd_last_metadata_update'] = str(self.mydoc['mmd:mmd']['mmd:last_metadata_update'])

        """ Dataset production status """
        if 'mmd:dataset_production_status' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:dataset_production_status'],
                    dict):
                mydict['mmd_dataset_production_status'] = self.mydoc['mmd:mmd']['mmd:dataset_production_status']['#text']
            else:
                mydict['mmd_dataset_production_status'] = str(self.mydoc['mmd:mmd']['mmd:dataset_production_status'])

        """ Collection """
        if 'mmd:collection' in self.mydoc['mmd:mmd']:
            mydict['mmd_collection'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:collection'], list):  # Does not work on single collection
                i = 0
                for e in self.mydoc['mmd:mmd']['mmd:collection']:
                    if isinstance(e,dict):
                        mydict['mmd_collection'].append(e['#text'])
                    else:
                        mydict['mmd_collection'].append(e)
                    i += 1
            else:
                #mydict['mmd_collection'] = self.mydoc['mmd:mmd']['mmd:collection'].encode('utf-8')
                mydict['mmd_collection'] = self.mydoc['mmd:mmd']['mmd:collection']

        """ 
        ISO TopicCategory 

        Need to fix the possibility for multiple values, but not
        prioritised now
        """
        if 'mmd:iso_topic_category' in self.mydoc['mmd:mmd']:
            mydict['mmd_iso_topic_category'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:iso_topic_category'],dict):
                mydict['mmd_iso_topic_category'].append(self.mydoc['mmd:mmd']['mmd:iso_topic_category']['#text'])
            else:
                mydict['mmd_iso_topic_category'].append(self.mydoc['mmd:mmd']['mmd:iso_topic_category'])

        """ Keywords """
        """ Should structure this on GCMD only at some point """
        """ Need to support multiple sets of keywords... """
        if 'mmd:keywords' in self.mydoc['mmd:mmd']:
            mydict['mmd_keywords_keyword'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], dict):
                if isinstance(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'],str):
                    mydict['mmd_keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])
                else:
                    for i in range(len(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])):
                        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'][i],str):
                            mydict['mmd_keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'][i])
            elif isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], list):
                for i in range(len(self.mydoc['mmd:mmd']['mmd:keywords'])):
                    if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'][i],dict):
                        if len(self.mydoc['mmd:mmd']['mmd:keywords'][i]) < 2:
                            continue
                        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'],list):
                            for j in range(len(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'])):
                                mydict['mmd_keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'][j])

                        else:
                            mydict['mmd_keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'])

            else:
                mydict['mmd_keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])

        """ Temporal extent """
        if 'mmd:temporal_extent' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:temporal_extent'], list):
                maxtime = dateutil.parser.parse('1000-01-01T00:00:00Z')
                mintime = dateutil.parser.parse('2099-01-01T00:00:00Z')
                for item in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                    #print(item)
                    for mykey in item:
                        #print(item[mykey])
                        if item[mykey] != '':
                            mytime = dateutil.parser.parse(item[mykey])
                        if mytime < mintime:
                            mintime = mytime
                        if mytime > maxtime:
                            maxtime = mytime
                #print('max',maxtime.strftime('%Y-%m-%dT%H:%M:%SZ'))
                #print('min',mintime.strftime('%Y-%m-%dT%H:%M:%SZ'))
                mydict['mmd_temporal_extent_start_date'] = mintime.strftime('%Y-%m-%dT%H:%M:%SZ')
                mydict['mmd_temporal_extent_end_date'] = maxtime.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                mydict["mmd_temporal_extent_start_date"] = str(
                    self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:start_date']),
                if 'mmd:end_date' in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                    if self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:end_date']!=None:
                        mydict["mmd_temporal_extent_end_date"] = str(
                            self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:end_date']),

        """ Geographical extent """
        """ Assumes longitudes positive eastwards and in the are -180:180
        """
        if 'mmd:geographic_extent' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:geographic_extent'],
                    list):
                self.logger.warning('This is a challenge as multiple bounding boxes are not supported in MMD yet, flattening information')
                latvals = []
                lonvals = []
                for e in self.mydoc['mmd:mmd']['mmd:geographic_extent']:
                    if e['mmd:rectangle']['mmd:north'] != None:
                        latvals.append(float(e['mmd:rectangle']['mmd:north']))
                    if e['mmd:rectangle']['mmd:south'] != None:
                        latvals.append(float(e['mmd:rectangle']['mmd:south']))
                    if e['mmd:rectangle']['mmd:east'] != None:
                        lonvals.append(float(e['mmd:rectangle']['mmd:east']))
                    if e['mmd:rectangle']['mmd:west'] != None:
                        lonvals.append(float(e['mmd:rectangle']['mmd:west']))
                #print(len(latvals))
                #print(latvals)
                if len(latvals) > 0 and len(lonvals) > 0:
                    mydict['mmd_geographic_extent_rectangle_north'] = max(latvals)
                    mydict['mmd_geographic_extent_rectangle_south'] = min(latvals)
                    mydict['mmd_geographic_extent_rectangle_west'] = min(lonvals)
                    mydict['mmd_geographic_extent_rectangle_east'] = max(lonvals)
                    mydict['bbox'] = "ENVELOPE("+str(min(lonvals))+","+str(max(lonvals))+","+ str(max(latvals))+","+str(min(latvals))+")"
                else:
                    mydict['mmd_geographic_extent_rectangle_north'] = 90.
                    mydict['mmd_geographic_extent_rectangle_south'] = -90.
                    mydict['mmd_geographic_extent_rectangle_west'] = -180.
                    mydict['mmd_geographic_extent_rectangle_east'] = 180.
            else:
                for item in self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']:
                    #print(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle'][item])
                    if self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle'][item] == None:
                        Warning('Missing geographical element')
                        mydict['mmd_metadata_status'] = 'Inactive'
                        return mydict

                mydict['mmd_geographic_extent_rectangle_north'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']),
                mydict['mmd_geographic_extent_rectangle_south'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']),
                mydict['mmd_geographic_extent_rectangle_east'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']),
                mydict['mmd_geographic_extent_rectangle_west'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']),
                mydict['bbox'] = "ENVELOPE("+self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']+","+self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']+","+ self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']+","+ self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']+")"

        """ Data access """
        """ Double check this ØG """
        """ Especially description """
        """ Revisit when SolR core is modified, duplicated information is
        used now """
        if 'mmd:data_access' in self.mydoc['mmd:mmd']:
            mydict['mmd_data_access_resource'] = []
            mydict['mmd_data_access_type'] = []
            if self.mydoc['mmd:mmd']['mmd:data_access']==None:
                self.logger.warning("data_access element is empty")
            elif isinstance(self.mydoc['mmd:mmd']['mmd:data_access'], list):
                i = 0
                for e in self.mydoc['mmd:mmd']['mmd:data_access']:
                    if e['mmd:type'] == None:
                        continue
                    #print('>>>>>> '+str(e))
                    mydict['mmd_data_access_resource'].append(
                        '\"' +
                        e['mmd:type'] +
                        '\":\"' +
                        e['mmd:resource'] +
                        '\",\"description\":\"\"'
                    )
                    mydict['mmd_data_access_type'].append(
                        e['mmd:type']
                    )
                    i += 1
            else:
                if self.mydoc['mmd:mmd']['mmd:data_access']['mmd:type'] != None and self.mydoc['mmd:mmd']['mmd:data_access']['mmd:resource'] != None:
                    mydict['mmd_data_access_resource'] = [
                        '\"' + self.mydoc['mmd:mmd']['mmd:data_access']['mmd:type'] +
                        '\":\"' + self.mydoc['mmd:mmd']['mmd:data_access']['mmd:resource'] + '\"'
                        ',\"description\":' + '\"'
                        ]
                    mydict['mmd_data_access_type'] = [
                        '\"' +
                        self.mydoc['mmd:mmd']['mmd:data_access']['mmd:type'] +
                        '\"' ]

                #print(mydict['mmd_data_access_resource'])
        """ Related information """
        """ Must be updated to hold mutiple ØG """
        mydict['mmd_related_information_resource'] = []
        if 'mmd:related_information' in self.mydoc['mmd:mmd']:
            # There can be several related_information sections.
            # Need to fix handling of this elsewhere in the software
            # For now only Dataset landing page is extracted for SolR
            # Assumes all child elements are present if parent is found
            # Check if required children are present
            if isinstance(self.mydoc['mmd:mmd']['mmd:related_information'],list):
                for e in self.mydoc['mmd:mmd']['mmd:related_information']:
                    #print('>>>', e)
                    if 'mmd:type' in e:
                        #print('#### ',e['mmd:type'])
                        if 'Dataset landing page' in e['mmd:type'] and e['mmd:resource'] != None:
                            mystring = '\"' + e['mmd:type'] + '\":\"' + \
                                e['mmd:resource'] + '\",\"description\":'
                            mydict['mmd_related_information_resource'].append(mystring)
            else:
                if 'mmd:resource' in self.mydoc['mmd:mmd']['mmd:related_information'] and (self.mydoc['mmd:mmd']['mmd:related_information']['mmd:resource'] != None):
                    if 'mmd:type' in self.mydoc['mmd:mmd']['mmd:related_information']:
                        mystring = '\"' + self.mydoc['mmd:mmd']['mmd:related_information']['mmd:type'] + '\":\"' + self.mydoc['mmd:mmd']['mmd:related_information']['mmd:resource'] + '\",\"description\":'
                    mydict['mmd_related_information_resource'].append(mystring)
        #print(mydict['mmd_related_information_resource'])

        """ Related dataset """
        """ TODO """
        """ Remember to add type of relation in the future ØG """
        """ Only interpreting parent for now since SolR doesn't take more
        """
        self.parent = None
        if 'mmd:related_dataset' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:related_dataset'],
                    list):
                self.logger.warning('Too many fields in related_dataset...')
                for e in self.mydoc['mmd:mmd']['mmd:related_dataset']:
                    if '@mmd:relation_type' in e:
                        if e['@mmd:relation_type'] == 'parent':
                            if '#text' in dict(e):
                                mydict['mmd_related_dataset'] = e['#text']
            else:
                """ Not sure if this is used?? """
                if '#text' in dict(self.mydoc['mmd:mmd']['mmd:related_dataset']):
                    mydict['mmd_related_dataset'] = self.mydoc['mmd:mmd']['mmd:related_dataset']['#text']

        """ Project """
        mydict['mmd_project_short_name'] = []
        mydict['mmd_project_long_name'] = []
        if 'mmd:project' in self.mydoc['mmd:mmd']:
            if self.mydoc['mmd:mmd']['mmd:project'] == None:
                mydict['mmd_project_short_name'].append('Not provided')
                mydict['mmd_project_long_name'].append('Not provided')
            elif isinstance(self.mydoc['mmd:mmd']['mmd:project'], list):
            # Check if multiple nodes are present
                for e in self.mydoc['mmd:mmd']['mmd:project']:
                    mydict['mmd_project_short_name'].append(e['mmd:short_name'])
                    mydict['mmd_project_long_name'].append(e['mmd:long_name'])
            else:
                # Extract information as appropriate
                e = self.mydoc['mmd:mmd']['mmd:project']
                if 'mmd:short_name' in e:
                    mydict['mmd_project_short_name'].append(e['mmd:short_name'])
                else:
                    mydict['mmd_project_short_name'].append('Not provided')
                    
                if 'mmd:long_name' in e:
                    mydict['mmd_project_long_name'].append(e['mmd:long_name'])
                else:
                    mydict['mmd_project_long_name'].append('Not provided')

        """ Access constraints """
        if 'mmd:access_constraint' in self.mydoc['mmd:mmd']:
            mydict['mmd_access_constraint'] = str(self.mydoc['mmd:mmd']['mmd:access_constraint'])

        """ Use constraint """
        if 'mmd:use_constraint' in self.mydoc['mmd:mmd']:
            mydict['mmd_use_constraint'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint'])

        """ Data center """
        """ Need to revisit this when SolR is reimplemented """
        if 'mmd:data_center' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:data_center'],list):
                dsselected = False
                for item in self.mydoc['mmd:mmd']['mmd:data_center']:
                    #print(item['mmd:data_center_name'])
                    if "mmd:long_name" in item['mmd:data_center_name'] and "Norwegian" in item['mmd:data_center_name']['mmd:long_name']:
                        myds = item
                        dsselected = True
                        break
                if not dsselected:
                    myds = self.mydoc['mmd:mmd']['mmd:data_center'][0]
            elif isinstance(self.mydoc['mmd:mmd']['mmd:data_center'],dict):
                myds = self.mydoc['mmd:mmd']['mmd:data_center']
            #print(myds)
            if 'mmd:long_name' in myds['mmd:data_center_name']:
                mydict['mmd_data_center_data_center_name_long_name'] = str(myds['mmd:data_center_name']['mmd:long_name'])
            if 'mmd:short_name' in myds['mmd:data_center_name']:
                mydict['mmd_data_center_data_center_name_short_name'] = str(myds['mmd:data_center_name']['mmd:short_name'])
            if 'mmd:data_center_url' in myds:
                mydict['mmd_data_center_data_center_url'] = str(myds['mmd:data_center_url'])
            if 'mmd:contact' in myds:
                mydict['mmd_data_center_contact_name'] = str(myds['mmd:contact']['mmd:name'])
                mydict['mmd_data_center_contact_role'] = str(myds['mmd:contact']['mmd:role'])
                mydict['mmd_data_center_contact_email'] = str(myds['mmd:contact']['mmd:email'])

        """ Personnel """
        """ Need to check this again, should restructure cores ØG """
        mydict['mmd_personnel_name'] = []
        mydict['mmd_personnel_email'] = []
        mydict['mmd_personnel_organisation'] = []
        mydict['mmd_personnel_role'] = []
        if 'mmd:personnel' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:personnel'], list):
                for e in self.mydoc['mmd:mmd']['mmd:personnel']:
                    if 'mmd:name' in e and e['mmd:name'] != None:
                        mydict['mmd_personnel_name'].append(e['mmd:name'])
                    if 'mmd:role' in e and e['mmd:role'] != None:
                        mydict['mmd_personnel_role'].append(e['mmd:role'])
                    if 'mmd:organisation' in e and e['mmd:organisation'] != None:
                        mydict['mmd_personnel_organisation'].append(e['mmd:organisation'])
                    if 'mmd:email' in e and e['mmd:email'] != None:
                        mydict['mmd_personnel_email'].append(e['mmd:email'])
            else:
                e = self.mydoc['mmd:mmd']['mmd:personnel']
                if 'mmd:name' in e and e['mmd:name'] != None:
                    mydict['mmd_personnel_name'].append(e['mmd:name'])
                if 'mmd:role' in e and e['mmd:role'] != None:
                    mydict['mmd_personnel_role'].append(e['mmd:role'])
                if 'mmd:organisation' in e and e['mmd:organisation'] != None:
                    mydict['mmd_personnel_organisation'].append(e['mmd:organisation'])
                if 'mmd:email' in e and e['mmd:email'] != None:
                    mydict['mmd_personnel_email'].append(e['mmd:email'])

        """ Activity type """
        if 'mmd:activity_type' in self.mydoc['mmd:mmd']:
            #print(type(self.mydoc['mmd:mmd']['mmd:activity_type']))
            if isinstance(self.mydoc['mmd:mmd']['mmd:activity_type'],dict):
                mydict['mmd_activity_type'] = self.mydoc['mmd:mmd']['mmd:activity_type']['#text']
            else:
                mydict['mmd_activity_type'] = str(self.mydoc['mmd:mmd']['mmd:activity_type'])

        return mydict


class IndexMMD:
    """ requires a list of dictionaries representing MMD as input """

    def __init__(self, mysolrserver):
        # Set up logging
        self.logger = logging.getLogger('indexdata.IndexMMD')
        self.logger.info('Creating an instance of IndexMMD')
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
            self.logger.error("Something failed in SolR init: %s", str(e))
        self.logger.info("Connection established to: %s", str(mysolrserver))

        # Connect to L2
        mysolrserver2 = mysolrserver.replace('-l1', '-l2')
        try:
            self.solr2 = pysolr.Solr(mysolrserver2, always_commit=True)
        except Exception as e:
            self.logger.error("Something failed in SolR init: %s", str(e))
        self.logger.info("Connection established to: %s", str(mysolrserver2))

        # Connect to thumbnail
        mysolrservert = mysolrserver.replace('-l1', '-thumbnail')
        try:
            self.solrt = pysolr.Solr(mysolrservert, always_commit=True)
        except Exception as e:
            self.logger.error("Something failed in SolR init: %s", str(e))
        self.logger.info("Connection established to: %s", str(mysolrservert))

    def add_level1(self, myrecord, addThumbnail=False, addFeature=False,
            mapprojection=ccrs.Mercator(),wmstimeout=120):
        if myrecord['mmd_metadata_status'] == 'Inactive':
            Warning('Skipping record')
            return

        """ Add a level 1 dataset """
        self.logger.info("Adding records to Level 1 core...")
        mylist = list()
        # print(myrecord)
        #print(json.dumps(myrecord, indent=4))
        mylist.append(myrecord)
        #print(mylist)
        try:
            self.solr1.add(mylist)
            #self.solr1.add([myrecord])
        except Exception as e:
            self.logger.error("Something failed in SolR add Level 1: %s", str(e))
        self.logger.info("Level 1 record successfully added.")

        # print(mylist[0]['mmd_data_access_resource'])
        # Remove flag later, do automatically if WMS is available...
        if addThumbnail:
            self.logger.info("Checking thumbnails...")
            darlist = self.darextract(mylist[0]['mmd_data_access_resource'])
            try:
                if 'OGC WMS' in darlist:
                    getCapUrl = darlist['OGC WMS']
                    #wms_layer = 'ice_conc'  # For S1 IW GRD data NOTE: need to parse/read the  mmd_data_access_wms_layers_wms_layer
                    wms_layer = 'temperature' # For arome data NOTE: need to parse/read the  mmd_data_access_wms_layers_wms_layer
                    #wms_style = 'boxfill/ncview'
                    wms_style = 'boxfill/redblue'
                    self.add_thumbnail(url=darlist['OGC WMS'],
                            identifier=mylist[0]['mmd_metadata_identifier'],
                            layer=wms_layer, zoom_level=0, 
                            projection=mapprojection,wmstimeout=120,
                            style=wms_style)
                elif 'OPeNDAP' in darlist:
                    # Thumbnail of timeseries to be added
                    # Or better do this as part of set_feature_type?
                    self.logger.info('OPeNDAP is not automatically parsed yet, to be added.')
            except Exception as e:
                self.logger.error("Something failed in adding thumbnail: %s", str(e))
                raise Warning("Couldn't add thumbnail.")
        elif addFeature:
            try:
                self.set_feature_type(mylist)
            except Exception as e:
                self.logger.error("Something failed in adding feature type: %s", str(e))

    def add_level2(self, myl2record, addThumbnail=False, addFeature=False,
            mapprojection=ccrs.Mercator(),wmstimeout=120):
        """ Add a level 2 dataset, i.e. update level 1 as well """
        mylist2 = list()
        mylist2.append(myl2record)
        # Fix for NPI data...
        myl2record['mmd_related_dataset'] = myl2record['mmd_related_dataset'].replace('http://data.npolar.no/dataset/','')
        myl2record['mmd_related_dataset'] = myl2record['mmd_related_dataset'].replace('https://data.npolar.no/dataset/','')
        myl2record['mmd_related_dataset'] = myl2record['mmd_related_dataset'].replace('http://api.npolar.no/dataset/','')
        myl2record['mmd_related_dataset'] = myl2record['mmd_related_dataset'].replace('.xml','')
        #print('>>>>>>>',myl2record['mmd_related_dataset'])

        """ Retrieve level 1 record """
        try:
            myresults = self.solr1.search('id:' +
                    myl2record['mmd_related_dataset'].replace(':','_'), df='', rows=100)
        except Exception as e:
            Warning("Something failed in searching for parent dataset, " + str(e))

        #print("Saw {0} result(s).".format(len(myresults)))
        if len(myresults) != 1:
            Warning("Didn't find unique parent record, skipping record")
            return
        for result in myresults:
            result.pop('full_text')
            myresults = result
        # Check that the parent found has mmd_related_dataset set and
        # update this, but first check that it doesn't already exists
        if 'mmd_related_dataset' in dict(myresults):
            # Need to check that this doesn't already exist...
            if myl2record['mmd_metadata_identifier'].replace(':','_') not in myresults['mmd_related_dataset']:
                myresults['mmd_related_dataset'].append(myl2record['mmd_metadata_identifier'].replace(':','_'))
        else:
            self.logger.warning('mmd_related_dataset not found in parent, creating it...')
            myresults['mmd_related_dataset'] = []
            self.logger.info('Adding dataset with identifier %s to parent', myl2record['mmd_metadata_identifier'].replace(':','_'),' to ',myl2record['mmd_related_dataset'])
            myresults['mmd_related_dataset'].append(myl2record['mmd_metadata_identifier'].replace(':','_'))
        mylist1 = list()
        mylist1.append(myresults)

        """ Index level 2 dataset """
        try:
            self.solr2.add(mylist2)
        except Exception as e:
            raise Exception("Something failed in SolR add level 2", str(e))
        self.logger.info("Level 2 record successfully added.")

        """ Update level 1 record with id of this dataset """
        try:
            self.solr1.add(mylist1)
        except Exception as e:
            raise Exception("Something failed in SolR update level 1 for level 2", str(e))
        self.logger.info("Level 1 record successfully updated.")

        if addThumbnail:
            self.logger.info("Checking tumbnails...")
            darlist = self.darextract(mylist2[0]['mmd_data_access_resource'])
            try:
                if 'OGC WMS' in darlist:
                    getCapUrl = darlist['OGC WMS']
                    wms_layer = 'temperature' # For arome data NOTE: need to parse/read the  mmd_data_access_wms_layers_wms_layer
                    wms_style = 'boxfill/redblue'
                    self.add_thumbnail(url=darlist['OGC WMS'],
                            identifier=mylist2[0]['mmd_metadata_identifier'],
                            layer=wms_layer, zoom_level=0, 
                            projection=mapprojection,wmstimeout=120,
                            style=wms_style)
                elif 'OPeNDAP' in darlist:
                    # Thumbnail of timeseries to be added
                    # Or better do this as part of set_feature_type?
                    self.logger.warning('OPeNDAP is not parsed automatically yet, to be added.')
            except Exception as e:
                self.logger.error("Something failed in adding thumbnail: %s", str(e)) 
                raise Warning("Couldn't add thumbnail.")
        elif addFeature:
            try:
                self.set_feature_type(mylist2)
            except Exception as e:
                self.logger.error("Something failed in adding feature type: %s", str(e))


    def add_thumbnail(self, url, identifier, layer, zoom_level,
            projection, wmstimeout, mytype='wms', style=None):
        """ Add thumbnail to SolR

            Args:
                type: Thumbnail type. (wms, ts)
                url: url to GetCapabilities document
                layer: The layer name in GetCapabilities document
                zoom_level: lat/lon number in degress for adjusting
                              zoom level. Positive give zooms out, negative
                              zooms in.
                projection: Cartopy projection. Not configurable at the moment.
            Returns:
                boolean
        """
        if mytype == 'wms':
            try:
                thumbnail = self.create_wms_thumbnail(url, layer, zoom_level,
                    projection, wmstimeout, style=style)
            except Exception as e:
                self.logger.error("Thumbnail creation from OGC WMS failed: %s",e)
                return
        elif mytype == 'ts': #time_series
            thumbnail = 'TMP'  # create_ts_thumbnail(...)
        else:
            self.logger.error('Invalid thumbnail type: %s', type)
            sys.exit(2)

        # Prepare input to SolR
        myrecord = OrderedDict()
        myrecord['id'] = identifier.replace(':','_')
        myrecord['mmd_metadata_identifier'] = identifier
        myrecord['thumbnail_data'] = str(thumbnail)
        mylist = list()
        mylist.append(myrecord)
        #print(mylist)

        try:
            self.solrt.add(mylist)
        except Exception as e:
            raise Exception("Something failed in SolR add thumbnail", str(e))

        self.logger.info("Thumbnail record successfully added.")

    def create_wms_thumbnail(self, url, layer, zoom_level=0,
            projection=ccrs.PlateCarree(),wmstimeout=120,**kwargs):
        """ Create a base64 encoded thumbnail by means of cartopy.

            Args:
                layer: The layer name in GetCapabilities document
                zoom_level: lat/lon number in degress for adjusting
                              zoom level. Positive give zooms out, negative
                              zooms in.
                projection: Cartopy projection. Not configurable at the moment.

            Returns:
                thumbnail_b64: base64 string representation of image
        """

        wms = WebMapService(url,timeout=wmstimeout)
        available_layers = list(wms.contents.keys())
        if layer not in available_layers:
            layer = available_layers[0]
            self.logger.info('creating WMS thumbnail for layer: %s',layer)

        if 'style' in kwargs.keys():
            style = kwargs['style']
            available_styles = list(wms.contents[layer].styles.keys())

            if available_styles:
                if style not in available_styles:
                    style = [available_styles[0]]
                else:
                    style = [style]
            else:
                style = None
        else:
            style = None

        wms_extent = wms.contents[available_layers[0]].boundingBoxWGS84
        cartopy_extent = [wms_extent[0], wms_extent[2], wms_extent[1], wms_extent[3]]
        #print(cartopy_extent)
        cartopy_extent_zoomed = [wms_extent[0] - zoom_level,
                wms_extent[2] + zoom_level,
                wms_extent[1] - zoom_level,
                wms_extent[3] + zoom_level]
        max_extent = [-180.0, 180.0, -90.0, 90.0]
        #print(cartopy_extent_zoomed)

        for i, extent in enumerate(cartopy_extent_zoomed):
            if i % 2 == 0:
                if extent < max_extent[i]:
                    cartopy_extent_zoomed[i] = max_extent[i]
            else:
                if extent > max_extent[i]:
                    cartopy_extent_zoomed[i] = max_extent[i]

        subplot_kw = dict(projection=projection)
        fig, ax = plt.subplots(subplot_kw=subplot_kw)
        #ax.set_extent(cartopy_extent_zoomed, crs=projection)
        #print(">>>>>", cartopy_extent_zoomed)
        # There are issues with versions of cartopy and PROJ. The
        # environment should be updated.
        #try:
        #    ax.set_extent(cartopy_extent_zoomed)
        #except Exception as e:
        #    raise Exception("Something failed on map projection", str(e))

        #land_mask = cartopy.feature.NaturalEarthFeature(category='physical',
        #                                                scale='50m',
        #                                                facecolor='#cccccc',
        #                                                name='land')
        #ax.add_feature(land_mask, zorder=0, edgecolor='#aaaaaa',
        #        linewidth=0.5)

        # transparent background
        ax.outline_patch.set_visible(False)
        ax.background_patch.set_visible(False)
        fig.patch.set_alpha(0)
        fig.set_alpha(0)
        fig.set_figwidth(4.5)
        fig.set_figheight(4.5)
        fig.set_dpi(100)
        ax.background_patch.set_alpha(1)

        ax.add_wms(wms, layer,
                wms_kwargs={'transparent': False,
                    'styles':style})
                #print({'transparent': False,'styles':style})
        ax.coastlines(resolution="50m",linewidth=0.5)

        fig.savefig('thumbnail.png', format='png', bbox_inches='tight')
        #fig.savefig('thumbnail.png', format='png')
        plt.close('all')

        with open('thumbnail.png', 'rb') as infile:
            data = infile.read()
            encode_string = base64.b64encode(data)

        thumbnail_b64 = (b'data:image/png;base64,' +
                encode_string).decode('utf-8')

        # Keep thumbnails while testing...
        #os.remove('thumbnail.png')

        return thumbnail_b64
    # with open('image_b64.txt','wb') as outimg:
        #    outimg.write(b'data:image/png;base64,'+encode_string)

    def create_ts_thumbnail(self):
        """ Create a base64 encoded thumbnail """

    def set_feature_type(self, mymd):
        """ Set feature type from OPeNDAP """
        self.logger.info("Now in set_feature_type")
        mylinks = self.darextract(mymd[0]['mmd_data_access_resource'])
        """
        for i in range(len(mymd[0]['mmd_data_access_resource'])):
            if isinstance(mymd[0]['mmd_data_access_resource'][i], bytes):
                mystr = str(mymd[0]['mmd_data_access_resource'][i], 'utf-8')
            else:
                mystr = mymd[0]['mmd_data_access_resource'][i]
            if mystr.find('description') != -1:
                t1,t2 = mystr.split(',', 1)
            else:
                t1 = mystr
            t2 = t1.replace('"', '')
            proto, myurl = t2.split(':', 1)
            mylinks[proto] = myurl
        """

        # First try to open as OPeNDAP
        try:
            ds = netCDF4.Dataset(mylinks['OPeNDAP'])
        except Exception as e:
            self.logger.error("Something failed reading dataset: %s", str(e))

        # Try to get the global attribute featureType
        try:
            featureType = ds.getncattr('featureType')
        except Exception as e:
            self.logger.error("Something failed reading dataset: %s", str(e))
            raise Warning('Could not find featureType')
        ds.close()

        mydict = OrderedDict({
            "id": mymd[0]['mmd_metadata_identifier'].replace(':','_'),
            "mmd_metadata_identifier": mymd[0]['mmd_metadata_identifier'],
            "feature_type": featureType,
        })
        mylist = list()
        mylist.append(mydict)

        try:
            self.solrt.add(mylist)
        except Exception as e:
            self.logger.error("Something failed in SolR add: %s", str(e))
            raise Warning("Something failed in SolR add" + str(e))

        self.logger.info("Successfully added feature type for OPeNDAP.")

    def delete_level1(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        self.logger.info("Deleting %s from level 1.", datasetid)
        try:
            self.solr1.delete(id=datasetid)
        except Exception as e:
            self.logger.error("Something failed in SolR delete: %s", str(e))

        self.logger.info("Record successfully deleted from Level 1 core")

    def delete_level2(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        self.logger.info("Deleting %s from level 2.", datasetid)
        try:
            self.solr2.delete(id=datasetid)
        except Exception as e:
            self.logger.error("Something failed in SolR delete: %s", str(e))

        self.logger.info("Records successfully deleted from Level 2 core")

    def delete_thumbnail(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        self.logger.info("Deleting %s from thumbnail core.", datasetid)
        try:
            self.solrt.delete(id=datasetid)
        except Exception as e:
            self.logger.error("Something failed in SolR delete: %s", str(e))

        self.logger.info("Records successfully deleted from thumbnail core")

    def search(self):
        """ Require Id as input """
        try:
            results = solr.search('mmd_title:Sea Ice Extent', df='text_en', rows=100)
        except Exception as e:
            self.logger.error("Something failed during search: %s", str(e))

        return results

    def darextract(self, mydar):
        mylinks = {}
        for i in range(len(mydar)):
            if isinstance(mydar[i], bytes):
                mystr = str(mydar[i], 'utf-8')
            else:
                mystr = mydar[i]
            if mystr.find('description') != -1:
                t1, t2 = mystr.split(',', 1)
            else:
                t1 = mystr
            t2 = t1.replace('"', '')
            proto, myurl = t2.split(':', 1)
            mylinks[proto] = myurl

        return (mylinks)

def main(argv):

    # Parse command line arguments
    try:
        args = parse_arguments()
    except:
        raise SystemExit('Command line arguments didn\'t parse correctly.')

    # Parse configuration file
    cfgstr = parse_cfg(args.cfgfile)

    # Initialise logging
    mylog = initialise_logger(cfgstr['logfile'], 'indexdata')
    mylog.info('Configuration of logging is finished.')

    tflg = l2flg = fflg = False

    # Read config file
    with open(args.cfgfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    # Specify map projection
    if cfg['wms-thumbnail-projection'] == 'Mercator':
        mapprojection = ccrs.Mercator()
    elif cfg['wms-thumbnail-projection'] == 'PlateCarree':
        mapprojection = ccrs.PlateCarree()
    elif cfg['wms-thumbnail-projection'] == 'PolarStereographic':
        mapprojection = ccrs.Stereographic(central_longitude=0.0,central_latitude=90., true_scale_latitude=60.)
    else:
        raise Exception('Map projection is not properly specified in config')

    SolrServer = cfg['solrserver']
    myCore = cfg['solrcore']

    mySolRc = SolrServer+myCore+'-l1'

    # Find files to process
    if args.input_file:
        myfiles = [args.input_file]
    elif args.list_file:
        try:
            f2 = open(args.list_file, "r")
        except IOError as e:
            mylog.error('Could not open file: %s %e', args.list_file, e)
            sys.exit()
        myfiles = f2.readlines()
        f2.close()
    elif args.remove:
        mysolr = IndexMMD(mySolRc)
        mysolr.delete_level1(args.remove)
        sys.exit()
    elif args.remove and args.level2:
        mysolr = IndexMMD(mySolRc)
        mysolr.delete_level2(args.remove)
        sys.exit()
    elif args.remove and args.thumbnail:
        mysolr = IndexMMD(mySolRc)
        mysolr.delete_thumbnail(deleteid)
        sys.exit()
    elif args.directory:
        try:
            myfiles = os.listdir(args.directory)
        except Exception as e:
            mylog.error("Something went wrong in decoding cmd arguments: %s", e)
            sys.exit(1)

    fileno = 0
    myfiles_pending = []
    for myfile in myfiles:
        myfile = myfile.strip()
        # Decide files to operate on
        if not myfile.endswith('.xml'):
            continue
        if args.list_file:
            myfile = myfile.rstrip()
        if args.directory:
            myfile = os.path.join(args.directory, myfile)

        # Index files
        mylog.info('\n\tProcessing file: %d - %s',fileno, myfile)

        try:
            mydoc = MMD4SolR(myfile) 
        except Exception as e:
            mylog.warning('Could not handle file: %s',e)
            continue
        mydoc.check_mmd()
        fileno += 1
        mysolr = IndexMMD(mySolRc)
        """ Do not search for mmd_metadata_identifier, always used id...  """
        newdoc = mydoc.tosolr()
        if (newdoc['mmd_metadata_status'] == "Inactive"):
            continue
        if (not args.no_thumbnail) and ('mmd_data_access_resource' in newdoc):
            for e in newdoc['mmd_data_access_resource']: 
                #print(type(e))
                if "OGC WMS" in str(e): 
                    tflg = True
        if 'mmd_related_dataset' in newdoc:
            # Special fix for NPI
            newdoc['mmd_related_dataset'] = newdoc['mmd_related_dataset'].replace('https://data.npolar.no/dataset/','')
            newdoc['mmd_related_dataset'] = newdoc['mmd_related_dataset'].replace('http://data.npolar.no/dataset/','')
            newdoc['mmd_related_dataset'] = newdoc['mmd_related_dataset'].replace('http://api.npolar.no/dataset/','')
            newdoc['mmd_related_dataset'] = newdoc['mmd_related_dataset'].replace('.xml','')
            # Skip if DOI is used to refer to parent, taht isn't consistent.
            if 'doi.org' in newdoc['mmd_related_dataset']:
                continue
            myresults = mysolr.solr1.search('id:' +
                    newdoc['mmd_related_dataset'], df='', rows=100)
            if len(myresults) == 0:
                mylog.warning("No parent found. Staging for second run.")
                myfiles_pending.append(myfile)
                continue
            l2flg = True
        mylog.info("Indexing dataset: %s", myfile)
        if l2flg:
            mysolr.add_level2(mydoc.tosolr(), tflg,
                    fflg,mapprojection,cfg['wms-timeout'])
        else:
            mysolr.add_level1(mydoc.tosolr(), tflg,
                    fflg,mapprojection,cfg['wms-timeout'])
        l2flg = False
        tflg = False

    # Now process all the level 2 files that failed in the previous
    # sequence. If the Level 1 dataset is not available, this will fail at
    # level 2. Meaning, the section below only ingests at level 2.
    fileno = 0
    if len(myfiles_pending)>0:
        mylog.info('Processing files that were not possible to process in first take.')
    for myfile in myfiles_pending:
        mylog.info('\tProcessing L2 file: %d - %s',fileno, myfile)
        try:
            mydoc = MMD4SolR(myfile) 
        except Exception as e:
            mylog.warning('Could not handle file: %s', e)
            continue
        mydoc.check_mmd()
        fileno += 1
        mysolr = IndexMMD(mySolRc)
        """ Do not search for mmd_metadata_identifier, always used id...  """
        """ Check if this can be used???? """
        newdoc = mydoc.tosolr()
        if 'mmd_data_access_resource' in newdoc.keys():
            for e in newdoc['mmd_data_access_resource']: 
                #print('>>>>>e', e)
                if (not nflg) and "OGC WMS" in (''.join(e)): 
                    tflg = True
        # Skip file if not a level 2 file
        if 'mmd_related_dataset' not in newdoc:
            continue
        mylog.info("Indexing dataset: %s", myfile)
        # Ingest at level 2
        mysolr.add_level2(mydoc.tosolr(), tflg,
                fflg,mapprojection,cfg['wms-timeout'])
        tflg = False

    # Report status
    mylog.info("Number of files processed were: %d", len(myfiles))


if __name__ == "__main__":
    main(sys.argv[1:])
