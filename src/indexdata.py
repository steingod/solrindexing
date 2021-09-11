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
    #parser.add_argument('-f','--feature_type',help='Extract featureType during ingestion (to be done automatically).', action='store_true')
    parser.add_argument('-r','--remove',help='Remove the dataset with the specified identifier (to be replaced by searchindex).')
    parser.add_argument('-2','--level2',help='Operate on child core.')

    ### Thumbnail parameters
    parser.add_argument('-t_layer','--thumbnail_layer',help='Specify wms_layer for thumbnail.', required=False)
    parser.add_argument('-t_style','--thumbnail_style',help='Specify the style (colorscheme) for the thumbnail.', required=False)
    parser.add_argument('-t_zl','--thumbnail_zoom_level',help='Specify the zoom level for the thumbnail.', type=float,required=False)
    parser.add_argument('-ac','--add_coastlines',help='Add coastlines too the thumbnail (True/False). Default True', const=True,nargs='?', required=False)
    parser.add_argument('-t_type','--thumbnail_type',help='Type of data. E.g. WMS or timeseries. Supports "wms" and "ts".', required=False)
    parser.add_argument('-t_extent','--thumbnail_extent',help='Spatial extent of thumbnail in lat/lon degrees like "x0 x1 y0 y1"', required=False, nargs='+')

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

                if isinstance(self.mydoc['mmd:mmd'][element], list):
                    for elem in self.mydoc['mmd:mmd'][element]:
                        if isinstance(elem,dict):
                            myvalue = elem['#text']
                        else:
                            myvalue = elem
                        if myvalue not in mmd_controlled_elements[element]:
                            if myvalue is not None:
                                self.logger.warn('\n\t%s contains non valid content: \n\t\t%s', element, myvalue)
                            else:
                                self.logger.warn('Discovered an empty element.')
                else:
                    if isinstance(self.mydoc['mmd:mmd'][element],dict):
                        myvalue = self.mydoc['mmd:mmd'][element]['#text']
                    else:
                        myvalue = self.mydoc['mmd:mmd'][element]
                    if myvalue not in mmd_controlled_elements[element]:
                        self.logger.warn('\n\t%s contains non valid content: \n\t\t%s', element, myvalue)

        """
        Check that keywords also contain GCMD keywords
        Need to check contents more specifically...
        """
        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], list):
            i = 0
            gcmd = False
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
                # warnings.warn('Keywords in GCMD are not available')
                self.logger.warn('\n\tKeywords in GCMD are not available')

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
                self.logger.warning('Removed D7 format in last_metadata_update')
                myvalue = self.mydoc['mmd:mmd']['mmd:last_metadata_update']
            mydate = dateutil.parser.parse(myvalue)
            #self.mydoc['mmd:mmd']['mmd:last_metadata_update'] = mydate.strftime('%Y-%m-%dT%H:%M:%SZ')
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
        Method for creating document with SolR representation of MMD according
        to the XSD.
        """

        # Defining Look Up Tables
        personnel_role_LUT = {'Investigator':'investigator',
                              'Technical contact': 'technical',
                              'Metadata author': 'metadata_author',
                              'Data center contact':'datacenter'
                             }
        related_information_LUT = {'Dataset landing page':'landing_page',
                              'Users guide': 'user_guide',
                              'Project home page': 'home_page',
                              'Observation facility': 'obs_facility',
                              'Extended metadata':'ext_metadata'
                             }

        # Create OrderedDict which will contain all elements for SolR
        mydict = OrderedDict()

        # SolR Can't use the mmd:metadata_identifier as identifier if it contains :, replace : by _ in the id field, let metadata_identifier be the correct one.

        """ Identifier """
        if isinstance(self.mydoc['mmd:mmd']['mmd:metadata_identifier'],dict):
            mydict['id'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier']['#text'].replace(':','_')
            mydict['metadata_identifier'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier']['#text']
        else:
            mydict['id'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier'].replace(':','_')
            mydict['metadata_identifier'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier']

        """ Last metadata update """
        if 'mmd:last_metadata_update' in self.mydoc['mmd:mmd']:
            last_metadata_update = self.mydoc['mmd:mmd']['mmd:last_metadata_update']

            lmu_datetime = []
            lmu_type = []
            lmu_note = []
            # FIXME check if this works correctly
            if isinstance(last_metadata_update['mmd:update'], dict): #Only one last_metadata_update element
                    lmu_datetime.append(str(last_metadata_update['mmd:update']['mmd:datetime']))
                    lmu_type.append(last_metadata_update['mmd:update']['mmd:type'])
                    lmu_note.append(last_metadata_update['mmd:update']['mmd:note'])

            else: # multiple last_metadata_update elements
                for i,e in enumerate(last_metadata_update['mmd:update']):
                    lmu_datetime.append(str(e['mmd:datetime']))
                    lmu_type.append(e['mmd:type'])
                    if 'mmd:note' in e.keys():
                        lmu_note.append(e['mmd:note'])

            mydict['last_metadata_update_datetime'] = lmu_datetime
            mydict['last_metadata_update_type'] = lmu_type
            mydict['last_metadata_update_note'] = lmu_note


        """ Metadata status """
        if isinstance(self.mydoc['mmd:mmd']['mmd:metadata_status'],dict):
            mydict['metadata_status'] = self.mydoc['mmd:mmd']['mmd:metadata_status']['#text']
        else:
            mydict['metadata_status'] = self.mydoc['mmd:mmd']['mmd:metadata_status']
        # TODO: the string below [title, abstract, etc ...]
        #  should be comments or some sort of logging statments

        """ Collection """
        if 'mmd:collection' in self.mydoc['mmd:mmd']:
            mydict['collection'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:collection'], list):
                i = 0
                for e in self.mydoc['mmd:mmd']['mmd:collection']:
                    if isinstance(e,dict):
                        mydict['collection'].append(e['#text'])
                    else:
                        mydict['collection'].append(e)
                    i += 1
            else:
                mydict['collection'] = self.mydoc['mmd:mmd']['mmd:collection']

        """ title """
        if isinstance(self.mydoc['mmd:mmd']['mmd:title'], list):
            i = 0
            # TODO: remove unused for loop
            # Switch to using e instead of self.mydoc...
            for e in self.mydoc['mmd:mmd']['mmd:title']:
                if self.mydoc['mmd:mmd']['mmd:title'][i]['@xml:lang'] == 'en':
                    mydict['title'] = self.mydoc['mmd:mmd']['mmd:title'][i]['#text']
                i += 1
        else:
            if isinstance(self.mydoc['mmd:mmd']['mmd:title'],dict):
                if self.mydoc['mmd:mmd']['mmd:title']['@xml:lang'] == 'en':
                    mydict['title'] = self.mydoc['mmd:mmd']['mmd:title']['#text']

            else:
                mydict['title'] = str(self.mydoc['mmd:mmd']['mmd:title'])

        """ abstract """
        if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'], list):
            i = 0
            for e in self.mydoc['mmd:mmd']['mmd:abstract']:
                if self.mydoc['mmd:mmd']['mmd:abstract'][i]['@xml:lang'] == 'en':
                    mydict['abstract'] = self.mydoc['mmd:mmd']['mmd:abstract'][i]['#text']
                i += 1
        else:
            if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'],dict):
                if self.mydoc['mmd:mmd']['mmd:abstract']['@xml:lang'] == 'en':
                    mydict['abstract'] = self.mydoc['mmd:mmd']['mmd:abstract']['#text']

            else:
                mydict['abstract'] = str(self.mydoc['mmd:mmd']['mmd:abstract'])


        """ Temporal extent """
        if 'mmd:temporal_extent' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:temporal_extent'], list):
                maxtime = dateutil.parser.parse('1000-01-01T00:00:00Z')
                mintime = dateutil.parser.parse('2099-01-01T00:00:00Z')
                for item in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                    for mykey in item:
                        if item[mykey] != '':
                            mytime = dateutil.parser.parse(item[mykey])
                        if mytime < mintime:
                            mintime = mytime
                        if mytime > maxtime:
                            maxtime = mytime
                mydict['temporal_extent_start_date'] = mintime.strftime('%Y-%m-%dT%H:%M:%SZ')
                mydict['temporal_extent_end_date'] = maxtime.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                mydict["temporal_extent_start_date"] = str(
                    self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:start_date']),
                if 'mmd:end_date' in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                    if self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:end_date']!=None:
                        mydict["temporal_extent_end_date"] = str(
                            self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:end_date']),

        """ Geographical extent """
        """ Assumes longitudes positive eastwards and in the are -180:180
        """
        if 'mmd:geographic_extent' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:geographic_extent'],
                    list):
                self.logger.warn('This is a challenge as multiple bounding boxes are not supported in MMD yet, flattening information')
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

                if len(latvals) > 0 and len(lonvals) > 0:
                    mydict['geographic_extent_rectangle_north'] = max(latvals)
                    mydict['geographic_extent_rectangle_south'] = min(latvals)
                    mydict['geographic_extent_rectangle_west'] = min(lonvals)
                    mydict['geographic_extent_rectangle_east'] = max(lonvals)
                    mydict['bbox'] = "ENVELOPE("+str(min(lonvals))+","+str(max(lonvals))+","+ str(max(latvals))+","+str(min(latvals))+")"
                else:
                    mydict['geographic_extent_rectangle_north'] = 90.
                    mydict['geographic_extent_rectangle_south'] = -90.
                    mydict['geographic_extent_rectangle_west'] = -180.
                    mydict['geographic_extent_rectangle_east'] = 180.
            else:
                for item in self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']:
                    #print(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle'][item])
                    if self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle'][item] == None:
                        Warning('Missing geographical element')
                        mydict['metadata_status'] = 'Inactive'
                        return mydict

                mydict['geographic_extent_rectangle_north'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']),
                mydict['geographic_extent_rectangle_south'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']),
                mydict['geographic_extent_rectangle_east'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']),
                mydict['geographic_extent_rectangle_west'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']),
                if '@srsName' in self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle'].keys():
                    mydict['geographic_extent_rectangle_srsName'] = self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['@srsName'],
                mydict['bbox'] = "ENVELOPE("+self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']+","+self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']+","+ self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']+","+ self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']+")"

        self.logger.info('Add location element?')

        """ Dataset production status """
        if 'mmd:dataset_production_status' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:dataset_production_status'],
                    dict):
                mydict['dataset_production_status'] = self.mydoc['mmd:mmd']['mmd:dataset_production_status']['#text']
            else:
                mydict['dataset_production_status'] = str(self.mydoc['mmd:mmd']['mmd:dataset_production_status'])

        """ Dataset language """
        if 'mmd:dataset_language' in self.mydoc['mmd:mmd']:
            mydict['dataset_language'] = str(self.mydoc['mmd:mmd']['mmd:dataset_language'])

        """ Operational status """
        if 'mmd:operational_status' in self.mydoc['mmd:mmd']:
            mydict['operational_status'] = str(self.mydoc['mmd:mmd']['mmd:operational_status'])

        """ Access constraints """
        if 'mmd:access_constraint' in self.mydoc['mmd:mmd']:
            mydict['access_constraint'] = str(self.mydoc['mmd:mmd']['mmd:access_constraint'])

        """ Use constraint """
        if 'mmd:use_constraint' in self.mydoc['mmd:mmd'] and self.mydoc['mmd:mmd']['mmd:use_constraint'] != None:
            # Need both identifier and resource for use constraint
            if 'mmd:identifier' in self.mydoc['mmd:mmd']['mmd:use_constraint'] and 'mmd:resource' in self.mydoc['mmd:mmd']['mmd:use_constraint']:
                mydict['use_constraint_identifier'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint']['mmd:identifier'])
                mydict['use_constraint_resource'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint']['mmd:resource'])
            else:
                self.logger.warning('Both identifier and resource need to be present to index this in use_constraint')
            if 'mmd:license_text' in self.mydoc['mmd:mmd']['mmd:use_constraint']:
                mydict['use_constraint_license_text'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint']['mmd:license_text'])

        """ Personnel """

        if 'mmd:personnel' in self.mydoc['mmd:mmd']:
            personnel_elements = self.mydoc['mmd:mmd']['mmd:personnel']

            if isinstance(personnel_elements, dict): #Only one element
                personnel_elements = [personnel_elements] # make it an iterable list

            for personnel in personnel_elements:
                role = personnel['mmd:role']
                mydict['personnel_{}_role'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_name'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_email'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_phone'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_fax'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_organisation'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_address'.format(personnel_role_LUT[role])] = []
                # don't think this is needed Øystein Godøy, METNO/FOU, 2021-09-08 mydict['personnel_{}_address_address'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_address_city'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_address_province_or_state'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_address_postal_code'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_address_country'.format(personnel_role_LUT[role])] = []
                for entry in personnel:
                    entry_type = entry.split(':')[-1]
                    if entry_type == role:
                        mydict['personnel_{}_role'.format(personnel_role_LUT[role])].append(personnel[entry])
                    else:
                        # Treat address specifically. 
                        if entry_type == 'contact_address':
                            for el in personnel[entry]:
                                el_type = el.split(':')[-1]
                                if el_type == 'address':
                                    mydict['personnel_{}_{}'.format(personnel_role_LUT[role], el_type)].append(personnel[entry])
                                else:
                                    mydict['personnel_{}_address_{}'.format(personnel_role_LUT[role], el_type)].append(personnel[entry])

        """ Data center """
        if 'mmd:data_center' in self.mydoc['mmd:mmd']:

            data_center_elements = self.mydoc['mmd:mmd']['mmd:data_center']

            if isinstance(data_center_elements, dict): #Only one element
                data_center_elements = [data_center_elements] # make it an iterable list

            for data_center in data_center_elements: #elf.mydoc['mmd:mmd']['mmd:data_center']: #iterate over all data_center elements
                for key,value in data_center.items():
                    if isinstance(value,dict): # if sub element is ordered dict
                        for kkey, vvalue in value.items():
                            element_name = 'data_center_{}'.format(kkey.split(':')[-1])
                            if not element_name in mydict.keys(): # create key in mydict
                                mydict[element_name] = []
                                mydict[element_name].append(vvalue)
                            else:
                                mydict[element_name].append(vvalue)
                    else: #sub element is not ordered dicts
                        element_name = '{}'.format(key.split(':')[-1])
                        if not element_name in mydict.keys(): # create key in mydict. Repetition of above. Should be simplified.
                            mydict[element_name] = []
                            mydict[element_name].append(value)
                        else:
                            mydict[element_name].append(value)

        """ Data access """
        # NOTE: This is identical to method above. Should in future versions be implified as a method
        if 'mmd:data_access' in self.mydoc['mmd:mmd']:
            data_access_elements = self.mydoc['mmd:mmd']['mmd:data_access']

            if isinstance(data_access_elements, dict): #Only one element
                data_access_elements = [data_access_elements] # make it an iterable list

            for data_access in data_access_elements: #iterate over all data_center elements
                data_access_type = data_access['mmd:type'].replace(" ","_").lower()
                mydict['data_access_url_{}'.format(data_access_type)] = data_access['mmd:resource']

                if 'mmd:wms_layers' in data_access and data_access_type == 'ogc_wms':
                    data_access_wms_layers_string = 'data_access_wms_layers'
                    data_access_wms_layers = data_access['mmd:wms_layers']
                    mydict[data_access_wms_layers_string] = [ i for i in data_access_wms_layers.values()][0]

        """ Related dataset """
        """ TODO """
        """ Remember to add type of relation in the future ØG """
        """ Only interpreting parent for now since SolR doesn't take more
        """
        self.parent = None
        if 'mmd:related_dataset' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:related_dataset'],
                    list):
                self.logger.warn('Too many fields in related_dataset...')
                for e in self.mydoc['mmd:mmd']['mmd:related_dataset']:
                    if '@mmd:relation_type' in e:
                        if e['@mmd:relation_type'] == 'parent':
                            if '#text' in dict(e):
                                mydict['related_dataset'] = e['#text']
            else:
                """ Not sure if this is used?? """
                if '#text' in dict(self.mydoc['mmd:mmd']['mmd:related_dataset']):
                    mydict['related_dataset'] = self.mydoc['mmd:mmd']['mmd:related_dataset']['#text']

        """ Storage information """
        self.logger.info('Storage information not implemented yet.')

        """ Related information """
        if 'mmd:related_information' in self.mydoc['mmd:mmd']:

            related_information_elements = self.mydoc['mmd:mmd']['mmd:related_information']

            if isinstance(related_information_elements, dict): #Only one element
                related_information_elements = [related_information_elements] # make it an iterable list

            for related_information in related_information_elements:
                for key, value in related_information.items():
                    element_name = 'related_information_{}'.format(key.split(':')[-1])

                    if value in related_information_LUT.keys():
                        mydict['related_url_{}'.format(related_information_LUT[value])] = related_information['mmd:resource']
                        mydict['related_url_{}_desc'.format(related_information_LUT[value])] = related_information['mmd:description']

        """
        ISO TopicCategory
        """
        if 'mmd:iso_topic_category' in self.mydoc['mmd:mmd']:
            mydict['iso_topic_category'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:iso_topic_category'], list):
                for iso_topic_category in self.mydoc['mmd:mmd']['mmd:iso_topic_category']:
                    mydict['iso_topic_category'].append(iso_topic_category)
            else:
                mydict['iso_topic_category'].append(self.mydoc['mmd:mmd']['mmd:iso_topic_category'])


        """ Keywords """
        """ Should structure this on GCMD only at some point """
        """ Need to support multiple sets of keywords... """
        if 'mmd:keywords' in self.mydoc['mmd:mmd']:
            mydict['keywords_keyword'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], dict):
                if isinstance(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'],str):
                    mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])
                else:
                    for i in range(len(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])):
                        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'][i],str):
                            mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'][i])
            elif isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], list):
                for i in range(len(self.mydoc['mmd:mmd']['mmd:keywords'])):
                    if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'][i],dict):
                        if len(self.mydoc['mmd:mmd']['mmd:keywords'][i]) < 2:
                            continue
                        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'],list):
                            for j in range(len(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'])):
                                mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'][j])

                        else:
                            mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'])

            else:
                mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])


        """ Project """
        mydict['project_short_name'] = []
        mydict['project_long_name'] = []
        if 'mmd:project' in self.mydoc['mmd:mmd']:
            if self.mydoc['mmd:mmd']['mmd:project'] == None:
                mydict['project_short_name'].append('Not provided')
                mydict['project_long_name'].append('Not provided')
            elif isinstance(self.mydoc['mmd:mmd']['mmd:project'], list):
            # Check if multiple nodes are present
                for e in self.mydoc['mmd:mmd']['mmd:project']:
                    mydict['project_short_name'].append(e['mmd:short_name'])
                    mydict['project_long_name'].append(e['mmd:long_name'])
            else:
                # Extract information as appropriate
                e = self.mydoc['mmd:mmd']['mmd:project']
                if 'mmd:short_name' in e:
                    mydict['project_short_name'].append(e['mmd:short_name'])
                else:
                    mydict['project_short_name'].append('Not provided')

                if 'mmd:long_name' in e:
                    mydict['project_long_name'].append(e['mmd:long_name'])
                else:
                    mydict['project_long_name'].append('Not provided')


        """ Platform """
        if 'mmd:platform' in self.mydoc['mmd:mmd']:
            for platform_key, platform_value in self.mydoc['mmd:mmd']['mmd:platform'].items():
                if isinstance(platform_value,dict): # if sub element is ordered dict
                    for kkey, vvalue in platform_value.items():
                        element_name = 'platform_{}_{}'.format(platform_key.split(':')[-1],kkey.split(':')[-1])
                        if not element_name in mydict.keys(): # create key in mydict
                            mydict[element_name] = []
                            mydict[element_name].append(vvalue)
                        else:
                            mydict[element_name].append(vvalue)
                else: #sub element is not ordered dicts
                    element_name = 'platform_{}'.format(platform_key.split(':')[-1])
                    if not element_name in mydict.keys(): # create key in mydict. Repetition of above. Should be simplified.
                        mydict[element_name] = []
                        mydict[element_name].append(platform_value)
                    else:
                        mydict[element_name].append(platform_value)

            # Add platform_sentinel for NBS
            initial_platform = mydict['platform_long_name'][0]
            if initial_platform.startswith('Sentinel'):
                mydict['platform_sentinel'] = initial_platform[:-1]


        """ Activity type """
        if 'mmd:activity_type' in self.mydoc['mmd:mmd']:
            mydict['activity_type'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:activity_type'], list):
                for activity_type in self.mydoc['mmd:mmd']['mmd:activity_type']:
                    mydict['activity_type'].append(activity_type)
            else:
                mydict['activity_type'].append(self.mydoc['mmd:mmd']['mmd:activity_type'])

        """ Dataset citation """
        if 'mmd:dataset_citation' in self.mydoc['mmd:mmd']:
            dataset_citation_elements = self.mydoc['mmd:mmd']['mmd:dataset_citation']

            if isinstance(dataset_citation_elements, dict): #Only one element
                dataset_citation_elements = [dataset_citation_elements] # make it an iterable list

            for dataset_citation in dataset_citation_elements:
                for k, v in dataset_citation.items():
                    element_suffix = k.split(':')[-1]
                    mydict['dataset_citation_{}'.format(element_suffix)] = v
                #for entry in personnel:
                #    entry_type = entry.split(':')[-1]
                #    if entry_type == role:
                #        mydict['personnel_{}_role'.format(personnel_role_LUT[role])].append(personnel[entry])
                #    else:
                #        mydict['personnel_{}_{}'.format(personnel_role_LUT[role], entry_type)].append(personnel[entry])


        return mydict


class IndexMMD:
    """ Class for indexing SolR representation of MMD to SolR server. Requires
    a list of dictionaries representing MMD as input.
    """

    def __init__(self, mysolrserver):
        # Set up logging
        self.logger = logging.getLogger('indexdata.IndexMMD')
        self.logger.info('Creating an instance of IndexMMD')

        # level variables

        self.level = None

        # Thumbnail variables
        self.wms_layer = None
        self.wms_style = None
        self.wms_zoom_level = 0
        self.wms_timeout = None
        self.add_coastlines = None
        self.projection = None
        self.thumbnail_type = None
        self.thumbnail_extent = None

        # Connecting to core
        try:
            self.solr1 = pysolr.Solr(mysolrserver, always_commit=True)
            self.logger.info("Connection established to: %s", str(mysolrserver))
        except Exception as e:
            self.logger.error("Something failed in SolR init: %s", str(e))
            self.logger.info("Add a sys.exit?")


    def index_record(self, input_record, addThumbnail, level=None, wms_layer=None, wms_style=None, wms_zoom_level=0, add_coastlines=True, projection=ccrs.PlateCarree(), wms_timeout=120, thumbnail_type='wms', thumbnail_extent=None):
        """ Add thumbnail to SolR
            Args:
                input_record() : input MMD file to be indexed in SolR
                addThumbnail (bool): If thumbnail should be added or not
                level (int): 1 or 2 depending if MMD is Level-1 or Level-2,
                            respectively. If None, assume to be Level-1
                wms_layer (str): WMS layer name
                wms_style (str): WMS style name
                wms_zoom_level (float): Negative zoom. Fixed value added in
                                        all directions (E,W,N,S)
                add_coastlines (bool): If coastlines should be added
                projection (ccrs): Cartopy projection object or name (i.e. string)
                wms_timeout (int): timeout for WMS service
                thumbnail_type (str): Type of thumbnail. Supports "wms" (OGC WMS)
                                      and "ts" (timeseries)
                thumbnail_extent (list): Spatial extent of the thumbnail in
                                      lat/lon [x0, x1, y0, y1]
            Returns:
                bool
        """
        if level == 1 or level == None:
            input_record.update({'dataset_type':'Level-1'})
            input_record.update({'isParent':'false'})
        elif level == 2:
            input_record.update({'dataset_type':'Level-2'})
        else:
            self.logger.error('Invalid level given: {}. Hence terminating'.format(level))

        if input_record['metadata_status'] == 'Inactive':
            Warning('Skipping record')
            return False
        myfeature = None
        if 'data_access_url_opendap' in input_record:
            # Thumbnail of timeseries to be added
            # Or better do this as part of get_feature_type?
            try:
                myfeature = self.get_feature_type(input_record['data_access_url_opendap'])
            except Exception as e:
                self.logger.error("Something failed while retrieving feature type: %s", str(e))
                #raise RuntimeError('Something failed while retrieving feature type')
            if myfeature:
                self.logger.info('feature_type found: %s', myfeature)
                input_record.update({'feature_type':myfeature})
        
        self.id = input_record['id']

        self.logger.info("Adding records to core...")

        mmd_record = list()
        mmd_record.append(input_record)

        if not addThumbnail:
            try:
                self.solr1.add(mmd_record)
            except Exception as e:
                self.logger.error("Something failed in SolR adding document: %s", str(e))
                return False
            self.logger.info("Record successfully added.")
            return True
        else:
            self.wms_layer = wms_layer
            self.wms_style = wms_style
            self.wms_zoom_level = wms_zoom_level
            self.add_coastlines = add_coastlines
            self.projection = projection
            self.wms_timeout = wms_timeout
            self.thumbnail_type = thumbnail_type
            self.thumbnail_extent = thumbnail_extent

            self.logger.info("Checking thumbnails...")

            if 'data_access_url_ogc_wms' in mmd_record[0]:
                # Create thumbnail from WMS
                getCapUrl = mmd_record[0]['data_access_url_ogc_wms']

                thumbnail_data = self.add_thumbnail(url=getCapUrl)

                if not thumbnail_data:
                    self.logger.error('Could not find WMS GetCapabilities document')
                    return False
            try:
                print('>>>>>>>> So far so good')
                input_record.update({'thumbnail_data':thumbnail_data})
                mmd_record = list()
                mmd_record.append(input_record)
                self.solr1.add(mmd_record)
                self.logger.info("Level 1 record successfully added.")
                return True
            except Exception as e:
                self.logger.error("Something failed in SolR adding doc: %s", str(e))
                return False

    def add_level2(self, myl2record, addThumbnail=False, addFeature=False,
            mapprojection=ccrs.Mercator(),wmstimeout=120):
        """ Add a level 2 dataset, i.e. update level 1 as well """
        mmd_record2 = list()
        mmd_record2.append(myl2record)
        # Fix for NPI data...
        myl2record['related_dataset'] = myl2record['related_dataset'].replace('http://data.npolar.no/dataset/','')
        myl2record['related_dataset'] = myl2record['related_dataset'].replace('https://data.npolar.no/dataset/','')
        myl2record['related_dataset'] = myl2record['related_dataset'].replace('http://api.npolar.no/dataset/','')
        myl2record['related_dataset'] = myl2record['related_dataset'].replace('.xml','')
        # Add additonal helpder fields
        myl2record['isChild'] = 'true'
        #print('>>>>>>>',myl2record['related_dataset'])

        """ Retrieve level 1 record """
        try:
            myresults = self.solr1.search('id:' + myl2record['related_dataset'].replace(':','_'), df='', rows=100)
        except Exception as e:
            Warning("Something failed in searching for parent dataset, " + str(e))

        # Check that only one record is returned
        if len(myresults) != 1:
            Warning("Didn't find unique parent record, skipping record")
            return
        # Convert from pySolr results object to dict and return. 
        for result in myresults:
            #result.pop('full_text')
            result.pop('bbox__maxX')
            result.pop('bbox__maxY')
            result.pop('bbox__minX')
            result.pop('bbox__minY')
            result.pop('bbox_rpt')
            myresults = result
        myresults['isParent'] = 'true'

        # Check that the parent found has related_dataset set and
        # update this, but first check that it doesn't already exists
        if 'related_dataset' in myresults:
            # Need to check that this doesn't already exist...
            if myl2record['metadata_identifier'].replace(':','_') not in myresults['related_dataset']:
                myresults['related_dataset'].append(myl2record['metadata_identifier'].replace(':','_'))
        else:
            self.logger.info('This dataset was not found in parent, creating it...')
            myresults['related_dataset'] = []
            self.logger.info('Adding dataset with identifier %s to parent %s', myl2record['metadata_identifier'].replace(':','_'),myl2record['related_dataset'])
            myresults['related_dataset'].append(myl2record['metadata_identifier'].replace(':','_'))
        mmd_record1 = list()
        mmd_record1.append(myresults)

        #print(myresults)

        """ Index level 2 dataset """
        # FIXME use of cores...
        try:
            self.solr1.add(mmd_record2)
        except Exception as e:
            raise Exception("Something failed in SolR add level 2", str(e))
        self.logger.info("Level 2 record successfully added.")

        """ Update level 1 record with id of this dataset """
        try:
            self.solr1.add(mmd_record1)
        except Exception as e:
            raise Exception("Something failed in SolR update level 1 for level 2", str(e))
        self.logger.info("Level 1 record successfully updated.")

        if addThumbnail:
            self.logger.info("Checking tumbnails...")
            darlist = self.darextract(mmd_record2[0]['mmd_data_access_resource'])
            try:
                if 'OGC WMS' in darlist:
                    getCapUrl = darlist['OGC WMS']
                    wms_layer = 'temperature' # For arome data NOTE: need to parse/read the  mmd_data_access_i_wms_layer
                    wms_style = 'boxfill/redblue'
                    self.add_thumbnail(url=darlist['OGC WMS'],
                            identifier=mmd_record2[0]['mmd_metadata_identifier'],
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
                self.set_feature_type(mmd_record2)
            except Exception as e:
                self.logger.error("Something failed in adding feature type: %s", str(e))


    def add_thumbnail(self, url, thumbnail_type='wms'):
        """ Add thumbnail to SolR
            Args:
                type: Thumbnail type. (wms, ts)
            Returns:
                thumbnail: base64 string representation of image
        """
        if thumbnail_type == 'wms':
            try:
                thumbnail = self.create_wms_thumbnail(url)
                return thumbnail
            except Exception as e:
                self.logger.error("Thumbnail creation from OGC WMS failed: %s",e)
                return None
        elif thumbnail_type == 'ts': #time_series
            thumbnail = 'TMP'  # create_ts_thumbnail(...)
            return thumbnail
        else:
            self.logger.error('Invalid thumbnail type: {}'.format(thumbnail_type))
            return None


    def create_wms_thumbnail(self, url):
        """ Create a base64 encoded thumbnail by means of cartopy.

            Args:
                url: wms GetCapabilities document

            Returns:
                thumbnail_b64: base64 string representation of image
        """

        wms_layer = self.wms_layer
        wms_style = self.wms_style
        wms_zoom_level = self.wms_zoom_level
        wms_timeout = self.wms_timeout
        add_coastlines = self.add_coastlines
        map_projection = self.projection
        thumbnail_extent = self.thumbnail_extent

        # map projection string to ccrs projection
        if isinstance(map_projection,str):
            map_projection = getattr(ccrs,map_projection)()

        wms = WebMapService(url,timeout=wms_timeout)
        available_layers = list(wms.contents.keys())

        if wms_layer not in available_layers:
            wms_layer = available_layers[0]
            self.logger.info('creating WMS thumbnail for layer: {}'.format(wms_layer))

        ### Checking styles
        available_styles = list(wms.contents[wms_layer].styles.keys())

        if available_styles:
            if wms_style not in available_styles:
                wms_style = [available_styles[0]]
            else:
                wms_style = None
        else:
            wms_style = None

        if not thumbnail_extent:
            wms_extent = wms.contents[available_layers[0]].boundingBoxWGS84
            cartopy_extent = [wms_extent[0], wms_extent[2], wms_extent[1], wms_extent[3]]

            cartopy_extent_zoomed = [wms_extent[0] - wms_zoom_level,
                    wms_extent[2] + wms_zoom_level,
                    wms_extent[1] - wms_zoom_level,
                    wms_extent[3] + wms_zoom_level]
        else:
            cartopy_extent_zoomed = thumbnail_extent

        max_extent = [-180.0, 180.0, -90.0, 90.0]

        for i, extent in enumerate(cartopy_extent_zoomed):
            if i % 2 == 0:
                if extent < max_extent[i]:
                    cartopy_extent_zoomed[i] = max_extent[i]
            else:
                if extent > max_extent[i]:
                    cartopy_extent_zoomed[i] = max_extent[i]

        subplot_kw = dict(projection=map_projection)
        self.logger.info(subplot_kw)

        fig, ax = plt.subplots(subplot_kw=subplot_kw)

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

        ax.add_wms(wms, wms_layer,
                wms_kwargs={'transparent': False,
                    'styles':wms_style})

        if add_coastlines:
            ax.coastlines(resolution="50m",linewidth=0.5)
        if map_projection == ccrs.PlateCarree():
            ax.set_extent(cartopy_extent_zoomed)
        else:
            ax.set_extent(cartopy_extent_zoomed, ccrs.PlateCarree())

        thumbnail_fname = 'thumbnail_{}.png'.format(self.id)
        fig.savefig(thumbnail_fname, format='png', bbox_inches='tight')
        plt.close('all')

        with open(thumbnail_fname, 'rb') as infile:
            data = infile.read()
            encode_string = base64.b64encode(data)

        thumbnail_b64 = (b'data:image/png;base64,' +
                encode_string).decode('utf-8')

        # Remove thumbnail
        os.remove(thumbnail_fname)
        return thumbnail_b64

    def create_ts_thumbnail(self):
        """ Create a base64 encoded thumbnail """

    def get_feature_type(self, myopendap):
        """ Set feature type from OPeNDAP """
        self.logger.info("Now in get_feature_type")

        # Open as OPeNDAP
        try:
            ds = netCDF4.Dataset(myopendap)
        except Exception as e:
            self.logger.error("Something failed reading dataset: %s", str(e))

        # Try to get the global attribute featureType
        try:
            featureType = ds.getncattr('featureType')
        except Exception as e:
            self.logger.error("Something failed reading dataset: %s", str(e))
            raise Warning('Could not find featureType')
        ds.close()

        if featureType not in ['point', 'timeSeries', 'trajectory','profile','timeSeriesProfile','trajectoryProfile']:
            self.logger.warning("The featureType found - %s - is not valid", featureType)
            raise Warning('The featureType found is not valid')

        return(featureType)

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
    except Exception as e:
        print("Something failed in parsing arguments: %s", str(e))
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

    mySolRc = SolrServer+myCore

    # Find files to process
    # FIXME remove l2 and thumbnail cores, reconsider deletion herein
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

        # FIXME, need a better way of handling this, WMS layers should be interpreted automatically, this way we need to know up fron whether WMS makes sense or not and that won't work for harvesting
        if args.thumbnail_layer:
            wms_layer = args.thumbnail_layer
        if args.thumbnail_style:
            wms_style = args.thumbnail_style
        if args.thumbnail_zoom_level:
            wms_zoom_level = args.thumbnail_zoom_level
        if args.add_coastlines:
            wms_coastlines = args.add_coastlines
        if args.thumbnail_type:
            thumbnail_type = args.thumbnail_type
        if args.thumbnail_extent:
            thumbnail_extent = [int(i) for i in args.thumbnail_extent[0].split(' ')]
            if not wms_zoom_level:
                wms_zoom_level=0
            if not wms_coastlines:
                wms_coastlines=True

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
        """ Do not search for metadata_identifier, always used id...  """
        newdoc = mydoc.tosolr()
        if (newdoc['metadata_status'] == "Inactive"):
            continue
        if (not args.no_thumbnail) and ('data_access_resource' in newdoc):
            for e in newdoc['data_access_resource']:
                #print(type(e))
                if "OGC WMS" in str(e):
                    tflg = True
        if 'related_dataset' in newdoc:
            # Special fix for NPI
            newdoc['related_dataset'] = newdoc['related_dataset'].replace('https://data.npolar.no/dataset/','')
            newdoc['related_dataset'] = newdoc['related_dataset'].replace('http://data.npolar.no/dataset/','')
            newdoc['related_dataset'] = newdoc['related_dataset'].replace('http://api.npolar.no/dataset/','')
            newdoc['related_dataset'] = newdoc['related_dataset'].replace('.xml','')
            # Skip if DOI is used to refer to piarent, taht isn't consistent.
            if 'doi.org' in newdoc['related_dataset']:
                continue
            myresults = mysolr.solr1.search('id:' +
                    newdoc['related_dataset'], df='', rows=100)
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
            #print(tflg)
            if tflg:
                try:
                    mysolr.index_record(input_record=mydoc.tosolr(), addThumbnail=tflg, wms_layer=wms_layer,wms_style=wms_style, wms_zoom_level=wms_zoom_level, add_coastlines=wms_coastlines, projection=mapprojection, thumbnail_type=thumbnail_type, wms_timeout=cfg['wms-timeout'],thumbnail_extent=thumbnail_extent)
                except Exception as e:
                    mylog.warning('Something failed during indexing %s', e)
            else:
                try:
                    mysolr.index_record(input_record=mydoc.tosolr(), addThumbnail=tflg)
                except Exception as e:
                    mylog.warning('Something failed during indexing %s', e)
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
        """ Do not search for metadata_identifier, always used id...  """
        """ Check if this can be used???? """
        newdoc = mydoc.tosolr()
        if 'data_access_resource' in newdoc.keys():
            for e in newdoc['data_access_resource']:
                #print('>>>>>e', e)
                if (not nflg) and "OGC WMS" in (''.join(e)):
                    tflg = True
        # Skip file if not a level 2 file
        if 'related_dataset' not in newdoc:
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
