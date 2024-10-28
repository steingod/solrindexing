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
import re
import subprocess
import pysolr
import xmltodict
import dateutil.parser
import warnings
import json
import yaml
import math
from collections import OrderedDict
import cartopy.crs as ccrs
import cartopy
import matplotlib.pyplot as plt
from owslib.wms import WebMapService
import base64
import netCDF4
import logging
import lxml.etree as ET
from logging.handlers import TimedRotatingFileHandler
from time import sleep
#import pickle Not used as of Øystein Godøy, METNO/FOU, 2023-04-10
from shapely.geometry import box
from shapely.wkt import loads
from shapely.geometry import mapping
import geojson
import pyproj
import shapely.geometry as shpgeo
import shapely.wkt

#For basic authentication
from requests.auth import HTTPBasicAuth
def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-a','--always_commit',action='store_true', help='Specification of whether always commit or not to SolR')
    parser.add_argument('-c','--cfg',dest='cfgfile', help='Configuration file', required=True)
    parser.add_argument('-i','--input_file',help='Individual file to be ingested.')
    parser.add_argument('-l','--list_file',help='File with datasets to be ingested specified.')
    parser.add_argument('-d','--directory',help='Directory to ingest')
    parser.add_argument('-t','--thumbnail',help='Create and index thumbnail, do not update the main content.', action='store_true')
    parser.add_argument('-n','--no_thumbnail',help='Do not index thumbnails (normally done automatically if WMS available).', action='store_true')
    parser.add_argument('-f','--no_feature',help='Do not extract featureType from files', action='store_true')

    ### Thumbnail parameters
    parser.add_argument('-m','--map_projection',help='Specify map projection for thumbnail (e.g. Mercator, PlateCarree, PolarStereographic).', required=False)
    parser.add_argument('-t_layer','--thumbnail_layer',help='Specify wms_layer for thumbnail.', required=False)
    parser.add_argument('-t_style','--thumbnail_style',help='Specify the style (colorscheme) for the thumbnail.', required=False)
    parser.add_argument('-t_zl','--thumbnail_zoom_level',help='Specify the zoom level for the thumbnail.', type=float,required=False)
    parser.add_argument('-ac','--add_coastlines',help='Add coastlines too the thumbnail (True/False). Default True', const=True,nargs='?', required=False)
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

def getZones(lon, lat):
    "get UTM zone number from latitude and longitude"

    if lat >= 72.0 and lat < 84.0:
        if lon >= 0.0 and lon < 9.0:
            return 31
        if lon >= 9.0 and lon < 21.0:
            return 33
        if lon >= 21.0 and lon < 33.0:
            return 35
        if lon >= 33.0 and lon < 42.0:
            return 37
    if lat >= 56 and lat < 64.0 and lon >= 3 and lon <= 12:
        return 32
    return math.floor((lon + 180) / 6) + 1


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
        self.logger.info('Creating an instance of MMD4SolR')
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
                                       'utilitiesCommunication',
                                       'Not available'],
            'mmd:collection': ['ACCESS',
                               'ADC',
                               'AeN',
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
            'mmd:quality_control': ['No quality control',
                                    'Basic quality control',
                                    'Extended quality control',
                                    'Comprehensive quality control'],
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
        gcmd = False
        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], list):
            i = 0
            # TODO: remove unused for loop
            # Switch to using e instead of self.mydoc...
            for e in self.mydoc['mmd:mmd']['mmd:keywords']:
                if str(self.mydoc['mmd:mmd']['mmd:keywords'][i]['@vocabulary']).upper() == 'GCMDSK':
                    gcmd = True
                    break
                i += 1
            if not gcmd:
                self.logger.warning('\n\tKeywords in GCMD are not available (a)')
        else:
            if str(self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary']).upper() == 'GCMDSK':
                gcmd = True
            else:
                # warnings.warning('Keywords in GCMD are not available')
                self.logger.warning('\n\tKeywords in GCMD are not available (b)')

        """
        Modify dates if necessary
        Adapted for the new MMD specification, but not all information is
        extracted as SolR is not adapted.
        FIXME and check
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
                                    if mydateels['mmd:datetime'].endswith('Z'):
                                        myvalue = mydateels['mmd:datetime']
                                    else:
                                        myvalue = mydateels['mmd:datetime']+'Z'

            else:
                # To be removed when all records are transformed into the
                # new format
                self.logger.warning('Removed D7 format in last_metadata_update')
                if self.mydoc['mmd:mmd']['mmd:last_metadata_update'].endswith('Z'):
                    myvalue = self.mydoc['mmd:mmd']['mmd:last_metadata_update']
                else:
                    myvalue = self.mydoc['mmd:mmd']['mmd:last_metadata_update']+'Z'
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
                            raise Exception('Error in temporal specifications for the dataset')

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
                              'Extended metadata':'ext_metadata',
                              'Scientific publication':'scientific_publication',
                              'Data paper':'data_paper',
                              'Data management plan':'data_management_plan',
                              'Other documentation':'other_documentation',
                              'Software': 'software',
                             }

        # Create OrderedDict which will contain all elements for SolR
        mydict = OrderedDict()

        # SolR Can't use the mmd:metadata_identifier as identifier if it contains :, replace : and other characters like / etc by _ in the id field, let metadata_identifier be the correct one.

        """ Identifier """
        idrepls = [':','/','.']
        if isinstance(self.mydoc['mmd:mmd']['mmd:metadata_identifier'],dict):
            myid = self.mydoc['mmd:mmd']['mmd:metadata_identifier']['#text']
            for e in idrepls:
                myid = myid.replace(e,'-')
            mydict['id'] = myid
            mydict['metadata_identifier'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier']['#text']
        else:
            myid = self.mydoc['mmd:mmd']['mmd:metadata_identifier']
            for e in idrepls:
                myid = myid.replace(e,'-')
            mydict['id'] = myid
            mydict['metadata_identifier'] = self.mydoc['mmd:mmd']['mmd:metadata_identifier']

        """ Last metadata update """
        if 'mmd:last_metadata_update' in self.mydoc['mmd:mmd']:
            last_metadata_update = self.mydoc['mmd:mmd']['mmd:last_metadata_update']

            lmu_datetime = []
            lmu_type = []
            lmu_note = []
            # FIXME check if this works correctly
            #Only one last_metadata_update element
            if isinstance(last_metadata_update['mmd:update'], dict):
                    lmu_datetime.append(str(last_metadata_update['mmd:update']['mmd:datetime']))
                    lmu_type.append(last_metadata_update['mmd:update']['mmd:type'])
                    lmu_note.append(last_metadata_update['mmd:update']['mmd:note'])
            # multiple last_metadata_update elements
            else:
                for i,e in enumerate(last_metadata_update['mmd:update']):
                    lmu_datetime.append(str(e['mmd:datetime']))
                    lmu_type.append(e['mmd:type'])
                    if 'mmd:note' in e.keys():
                        lmu_note.append(e['mmd:note'])
                    else:
                        lmu_note.append('Not provided')

            i = 0
            for myel in lmu_datetime:
                i+=1
                if myel.endswith('Z'):
                    continue
                else:
                    lmu_datetime[i-1] = myel+'Z'
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
            # Switch to using e instead of self.mydoc...
            for e in self.mydoc['mmd:mmd']['mmd:title']:
                if '@xml:lang' in e:
                    if e['@xml:lang'] == 'en':
                        mydict['title'] = e['#text']
                elif '@lang' in e:
                    if e['@lang'] == 'en':
                        mydict['title'] = e['#text']
        else:
            if isinstance(self.mydoc['mmd:mmd']['mmd:title'],dict):
                if '@xml:lang' in self.mydoc['mmd:mmd']['mmd:title']:
                    if self.mydoc['mmd:mmd']['mmd:title']['@xml:lang'] == 'en':
                        mydict['title'] = self.mydoc['mmd:mmd']['mmd:title']['#text']
                if '@lang' in self.mydoc['mmd:mmd']['mmd:title']:
                    if self.mydoc['mmd:mmd']['mmd:title']['@lang'] == 'en':
                        mydict['title'] = self.mydoc['mmd:mmd']['mmd:title']['#text']
            else:
                mydict['title'] = str(self.mydoc['mmd:mmd']['mmd:title'])

        """ abstract """
        if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'], list):
            for e in self.mydoc['mmd:mmd']['mmd:abstract']:
                if '@xml:lang' in e:
                    if e['@xml:lang'] == 'en':
                        mydict['abstract'] = e['#text']
                elif '@lang' in e:
                    if e['@lang'] == 'en':
                        mydict['abstract'] = e['#text']
        else:
            if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'],dict):
                if '@xml:lang' in self.mydoc['mmd:mmd']['mmd:abstract']:
                    if self.mydoc['mmd:mmd']['mmd:abstract']['@xml:lang'] == 'en':
                        mydict['abstract'] = self.mydoc['mmd:mmd']['mmd:abstract']['#text']
                if '@lang' in self.mydoc['mmd:mmd']['mmd:abstract']:
                    if self.mydoc['mmd:mmd']['mmd:abstract']['@lang'] == 'en':
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
                        try:
                            dateutil.parser.parse(self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:end_date'])
                            mydict["temporal_extent_end_date"] = str(self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:end_date']),
                        except Exception as e:
                            self.logger.warning("End date is not provided properly")
            if "temporal_extent_end_date" in mydict:
                self.logger.debug('Creating daterange with end date')
                if isinstance(mydict["temporal_extent_start_date"], tuple):
                    st = str(mydict["temporal_extent_start_date"][0])
                else:
                    st = str(mydict["temporal_extent_start_date"])
                if isinstance(mydict["temporal_extent_end_date"], tuple):
                    end = str(mydict["temporal_extent_end_date"][0])
                else:
                    end = str(mydict["temporal_extent_start_date"])

                mydict['temporal_extent_period_dr'] = '[' + st + ' TO ' + end + ']'
            else:
                self.logger.debug('Creating daterange with open end date')
                if isinstance(mydict["temporal_extent_start_date"], tuple):
                    st = str(mydict["temporal_extent_start_date"][0])
                else:
                    st = str(mydict["temporal_extent_start_date"])
                mydict['temporal_extent_period_dr'] = '[' + st + ' TO *]'
            self.logger.info("Temporal extent date range: %s", mydict['temporal_extent_period_dr'])

        """ Geographical extent """
        """ Assumes longitudes positive eastwards and in the are -180:180
        """
        if 'mmd:geographic_extent' in self.mydoc['mmd:mmd'] and self.mydoc['mmd:mmd']['mmd:geographic_extent'] != None:
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

                if len(latvals) > 0 and len(lonvals) > 0:
                    mydict['geographic_extent_rectangle_north'] = max(latvals)
                    mydict['geographic_extent_rectangle_south'] = min(latvals)
                    mydict['geographic_extent_rectangle_west'] = min(lonvals)
                    mydict['geographic_extent_rectangle_east'] = max(lonvals)
                    mydict['bbox'] = "ENVELOPE("+str(min(lonvals))+","+str(max(lonvals))+","+ str(max(latvals))+","+str(min(latvals))+")"
                    #Check if we have a point or a boundingbox
                    if float(e['mmd:rectangle']['mmd:north']) == float(e['mmd:rectangle']['mmd:south']):
                        if float(e['mmd:rectangle']['mmd:east']) == float(e['mmd:rectangle']['mmd:west']):
                            point = shpgeo.Point(float(e['mmd:rectangle']['mmd:east']),float(e['mmd:rectangle']['mmd:north']))
                            #print(point.y)
                            mydict['polygon_rpt'] = point.wkt

                            print(mapping(point))
                            #mydict['geom'] = geojson.dumps(mapping(point))
                    else:
                        bbox = box(min(lonvals), min(latvals), max(lonvals), max(latvals))

                        print("First conditition")
                        print(bbox)
                        polygon = bbox.wkt
                        #p = shapely.geometry.polygon.orient(polygon, sign=1.0)
                        #print(p.exterior.is_ccw)
                        mydict['polygon_rpt'] = polygon

                        #print(mapping(polygon))
                        #pGeo = shpgeo.shape({'type': 'polygon', 'coordinates': tuple(newCoord)})
                        #mydict['geom'] = geojson.dumps(mapping(shapely.wkt.loads(polygon)))
                        #print(mydict['geom'])


                else:
                    mydict['geographic_extent_rectangle_north'] = 90.
                    mydict['geographic_extent_rectangle_south'] = -90.
                    mydict['geographic_extent_rectangle_west'] = -180.
                    mydict['geographic_extent_rectangle_east'] = 180.
            else:
                for item in self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']:
                    #print(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle'][item])
                    if self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle'][item] == None:
                        self.logger.warning('Missing geographical element, will not process the file.')
                        mydict['metadata_status'] = 'Inactive'
                        raise Warning('Missing spatial bounds')

                mydict['geographic_extent_rectangle_north'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north'])
                mydict['geographic_extent_rectangle_south'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south'])
                mydict['geographic_extent_rectangle_east'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east'])
                mydict['geographic_extent_rectangle_west'] = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west'])
                """
                Check if bounding box is correct
                """
                if not mydict['geographic_extent_rectangle_north'] >= mydict['geographic_extent_rectangle_south']:
                    self.logger.warning('Northernmost boundary is south of southernmost, will not process...')
                    mydict['metadata_status'] = 'Inactive'
                    raise Warning('Error in spatial bounds')
                if not mydict['geographic_extent_rectangle_east'] >= mydict['geographic_extent_rectangle_west']:
                    self.logger.warning('Easternmost boundary is west of westernmost, will not process...')
                    mydict['metadata_status'] = 'Inactive'
                    raise Warning('Error in spatial bounds')
                if mydict['geographic_extent_rectangle_east'] > 180 or mydict['geographic_extent_rectangle_west'] > 180 or  mydict['geographic_extent_rectangle_east'] < -180 or mydict['geographic_extent_rectangle_west'] < -180:
                    self.logger.warning('Longitudes outside valid range, will not process...')
                    mydict['metadata_status'] = 'Inactive'
                    raise Warning('Error in longitude bounds')
                if mydict['geographic_extent_rectangle_north'] > 90 or mydict['geographic_extent_rectangle_south'] > 90 or  mydict['geographic_extent_rectangle_north'] < -90 or mydict['geographic_extent_rectangle_south'] < -90:
                    self.logger.warning('Latitudes outside valid range, will not process...')
                    mydict['metadata_status'] = 'Inactive'
                    raise Warning('Error in latitude bounds')

                """
                Finalise bbox
                """
                if '@srsName' in self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle'].keys():
                    mydict['geographic_extent_rectangle_srsName'] = self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['@srsName'],
                mydict['bbox'] = "ENVELOPE("+self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']+","+self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']+","+ self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']+","+ self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']+")"

                #Check if we have a point or a boundingbox
                if float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']) == float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']):
                    if float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']) == float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']):
                        point = shpgeo.Point(float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']),float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']))
                        #print(point.y)
                        mydict['polygon_rpt'] = point.wkt

                        print(mapping(point))

                        #mydict['geom'] = geojson.dumps(mapping(point))

                else:
                    bbox = box(float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']), float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']), float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']), float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']), ccw=False)
                    #print(bbox)
                    polygon = bbox.wkt
                    print(polygon)
                    #p = shapely.geometry.polygon.orient(shapely.wkt.loads(polygon), sign=1.0)
                    #print(p.exterior.is_ccw)
                    mydict['polygon_rpt'] = polygon
                    #print(mapping(shapely.wkt.loads(polygon)))
                    #print(geojson.dumps(mapping(loads(polygon))))
                    #pGeo = shpgeo.shape({'type': 'polygon', 'coordinates': tuple(newCoord)})
                    #mydict['geom'] = geojson.dumps(mapping(p))
                    #print(mydict['geom'])

        """ Add location element?? """
        #self.logger.info('Add location element?')

        """ Dataset production status """
        self.logger.info("Processing dataset production status")
        if 'mmd:dataset_production_status' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:dataset_production_status'],
                    dict):
                mydict['dataset_production_status'] = self.mydoc['mmd:mmd']['mmd:dataset_production_status']['#text']
            else:
                mydict['dataset_production_status'] = str(self.mydoc['mmd:mmd']['mmd:dataset_production_status'])

        """ Dataset language """
        self.logger.info("Processing dataset language")
        if 'mmd:dataset_language' in self.mydoc['mmd:mmd']:
            mydict['dataset_language'] = str(self.mydoc['mmd:mmd']['mmd:dataset_language'])

        """ Operational status """
        self.logger.info("Processing dataset operational status")
        if 'mmd:operational_status' in self.mydoc['mmd:mmd']:
            mydict['operational_status'] = str(self.mydoc['mmd:mmd']['mmd:operational_status'])

        """ Access constraints """
        self.logger.info("Processing dataset access constraints")
        if 'mmd:access_constraint' in self.mydoc['mmd:mmd']:
            mydict['access_constraint'] = str(self.mydoc['mmd:mmd']['mmd:access_constraint'])

        """ Use constraint """
        self.logger.info("Processing dataset use constraints")
        if 'mmd:use_constraint' in self.mydoc['mmd:mmd'] and self.mydoc['mmd:mmd']['mmd:use_constraint'] != None:
            # Need both identifier and resource for use constraint
            if 'mmd:identifier' in self.mydoc['mmd:mmd']['mmd:use_constraint'] and 'mmd:resource' in self.mydoc['mmd:mmd']['mmd:use_constraint']:
                mydict['use_constraint_identifier'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint']['mmd:identifier'])
                mydict['use_constraint_resource'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint']['mmd:resource'])
            else:
                self.logger.warning('Both license identifier and resource need to be present to index this properly')
                mydict['use_constraint_identifier'] =  "Not provided"
                mydict['use_constraint_resource'] =  "Not provided"
            if 'mmd:license_text' in self.mydoc['mmd:mmd']['mmd:use_constraint']:
                mydict['use_constraint_license_text'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint']['mmd:license_text'])

        """ Personnel """
        self.logger.info("Processing dataset personnel")
        if 'mmd:personnel' in self.mydoc['mmd:mmd']:
            personnel_elements = self.mydoc['mmd:mmd']['mmd:personnel']

            if isinstance(personnel_elements, dict): #Only one element
                personnel_elements = [personnel_elements] # make it an iterable list

            # Facet elements
            mydict['personnel_role'] = []
            mydict['personnel_name'] = []
            mydict['personnel_organisation'] = []
            # Fix role based lists
            for role in personnel_role_LUT:
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

            # Fill lists with information
            for personnel in personnel_elements:
                role = personnel['mmd:role']
                if not role:
                    self.logger.warning('No role available for personnel')
                    break
                if role not in personnel_role_LUT:
                    self.logger.warning('Wrong role provided for personnel')
                    break
                for entry in personnel:
                    entry_type = entry.split(':')[-1]
                    if entry_type == 'role':
                        mydict['personnel_{}_role'.format(personnel_role_LUT[role])].append(personnel[entry])
                        mydict['personnel_role'].append(personnel[entry])
                    else:
                        # Treat address specifically and handle faceting elements personnel_role, personnel_name, personnel_organisation.
                        if entry_type == 'contact_address':
                            for el in personnel[entry]:
                                el_type = el.split(':')[-1]
                                if el_type == 'address':
                                    mydict['personnel_{}_{}'.format(personnel_role_LUT[role], el_type)].append(personnel[entry][el])
                                else:
                                    mydict['personnel_{}_address_{}'.format(personnel_role_LUT[role], el_type)].append(personnel[entry][el])
                        elif entry_type == 'name':
                            mydict['personnel_{}_{}'.format(personnel_role_LUT[role], entry_type)].append(personnel[entry])
                            mydict['personnel_name'].append(personnel[entry])
                        elif entry_type == 'organisation':
                            mydict['personnel_{}_{}'.format(personnel_role_LUT[role], entry_type)].append(personnel[entry])
                            mydict['personnel_organisation'].append(personnel[entry])
                        else:
                            mydict['personnel_{}_{}'.format(personnel_role_LUT[role], entry_type)].append(personnel[entry])

        """ Data center """
        self.logger.info("Processing data center")
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
        self.logger.info("Processing data access")
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
            Added handling of namespace in identifiers
        """
        self.logger.info("Processing related dataset")
        self.parent = None
        if 'mmd:related_dataset' in self.mydoc['mmd:mmd']:
            idrepls = [':','/','.']
            if isinstance(self.mydoc['mmd:mmd']['mmd:related_dataset'],
                    list):
                self.logger.warning('Too many fields in related_dataset...')
                for e in self.mydoc['mmd:mmd']['mmd:related_dataset']:
                    if '@mmd:relation_type' in e:
                        if e['@mmd:relation_type'] == 'parent':
                            if '#text' in dict(e):
                                mydict['related_dataset'] = e['#text']
                                mydict['related_dataset_id'] = e['#text']
                                for e in idrepls:
                                    mydict['related_dataset_id'] = mydict['related_dataset_id'].replace(e,'-')
            else:
                """ Not sure if this is used?? """
                if '#text' in dict(self.mydoc['mmd:mmd']['mmd:related_dataset']):
                    mydict['related_dataset'] = self.mydoc['mmd:mmd']['mmd:related_dataset']['#text']
                    mydict['related_dataset_id'] = self.mydoc['mmd:mmd']['mmd:related_dataset']['#text']
                    for e in idrepls:
                        mydict['related_dataset_id'] = mydict['related_dataset_id'].replace(e,'-')

        """ Storage information """
        self.logger.info("Processing storage information")
        if 'mmd:storage_information' in self.mydoc['mmd:mmd'] and self.mydoc['mmd:mmd']['mmd:storage_information'] != None:
            if 'mmd:file_name' in self.mydoc['mmd:mmd']['mmd:storage_information'] and self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_name'] != None:
                mydict['storage_information_file_name'] = str(self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_name'])
            if 'mmd:file_location' in self.mydoc['mmd:mmd']['mmd:storage_information'] and self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_location'] != None:
                mydict['storage_information_file_location'] = str(self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_location'])
            if 'mmd:file_format' in self.mydoc['mmd:mmd']['mmd:storage_information'] and self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_format'] != None:
                mydict['storage_information_file_format'] = str(self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_format'])
            if 'mmd:file_size' in self.mydoc['mmd:mmd']['mmd:storage_information'] and self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_size'] != None:
                if isinstance(self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_size'], dict):
                    mydict['storage_information_file_size'] = str(self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_size']['#text'])
                    mydict['storage_information_file_size_unit'] = str(self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:file_size']['@unit'])
                else:
                    self.logger.warning("Filesize unit not specified, skipping field")
            if 'mmd:checksum' in self.mydoc['mmd:mmd']['mmd:storage_information'] and self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:checksum'] != None:
                if isinstance(self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:checksum'], dict):
                    mydict['storage_information_file_checksum'] = str(self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:checksum']['#text'])
                    mydict['storage_information_file_checksum_type'] = str(self.mydoc['mmd:mmd']['mmd:storage_information']['mmd:checksum']['@type'])
                else:
                    self.logger.warning("Checksum type is not specified, skipping field")


        """ Related information """
        self.logger.info("Processing related information")
        if 'mmd:related_information' in self.mydoc['mmd:mmd']:

            related_information_elements = self.mydoc['mmd:mmd']['mmd:related_information']

            if isinstance(related_information_elements, dict): #Only one element
                related_information_elements = [related_information_elements] # make it an iterable list

            for related_information in related_information_elements:
                value = related_information['mmd:type']
                if value in related_information_LUT.keys():
                    #if list does not exist, create it
                    if 'related_url_{}'.format(related_information_LUT[value]) not in mydict.keys():
                        mydict['related_url_{}'.format(related_information_LUT[value])] = []
                        mydict['related_url_{}_desc'.format(related_information_LUT[value])] = []

                    #append elements to lists
                    mydict['related_url_{}'.format(related_information_LUT[value])].append(related_information['mmd:resource'])
                    if 'mmd:description' in related_information and related_information['mmd:description'] is not None:
                        mydict['related_url_{}_desc'.format(related_information_LUT[value])].append(related_information['mmd:description'])
                    else:
                        mydict['related_url_{}_desc'.format(related_information_LUT[value])].append('Not Available')

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
        """
        Added double indexing of GCMD keywords. keywords_gcmd  (and keywords_wigos) are for faceting in SolR.
        What is shown in data portal is keywords_keyword.
        """
        self.logger.info("Processing keywords")
        if 'mmd:keywords' in self.mydoc['mmd:mmd']:
            mydict['keywords_keyword'] = []
            mydict['keywords_vocabulary'] = []
            mydict['keywords_gcmd'] = []
            mydict['keywords_wigos'] = [] # Not used yet
            # If there is only one keyword list
            if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], dict):
                if isinstance(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'],str):
                    if self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary'] == "GCMDSK":
                        mydict['keywords_gcmd'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])
                    mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])
                    mydict['keywords_vocabulary'].append(self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary'])
                else:
                    for i in range(len(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])):
                        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'][i],str):
                            if self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary'] == "GCMDSK":
                                mydict['keywords_gcmd'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'][i])
                            mydict['keywords_vocabulary'].append(self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary'])
                            mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'][i])
            # If there are multiple keyword lists
            elif isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], list):
                for i in range(len(self.mydoc['mmd:mmd']['mmd:keywords'])):
                    if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'][i],dict):
                        # Check for empty lists
                        if len(self.mydoc['mmd:mmd']['mmd:keywords'][i]) < 2:
                            continue
                        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'],list):
                            for j in range(len(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'])):
                                if self.mydoc['mmd:mmd']['mmd:keywords'][i]['@vocabulary'] == "GCMDSK":
                                    mydict['keywords_gcmd'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'][j])
                                mydict['keywords_vocabulary'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['@vocabulary'])
                                mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'][j])
                        else:
                            if self.mydoc['mmd:mmd']['mmd:keywords'][i]['@vocabulary'] == "GCMDSK":
                                mydict['keywords_gcmd'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'])
                            mydict['keywords_vocabulary'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['@vocabulary'])
                            mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'])

            else:
                if self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary'] == "GCMDSK":
                    mydict['keywords_gcmd'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])
                mydict['keywords_vocabulary'].append(self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary'])
                mydict['keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword'])

        """ Project """
        self.logger.info("Processing project")
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
        self.logger.info("Processing platform")
        # FIXME add check for empty sub elements...
        if 'mmd:platform' in self.mydoc['mmd:mmd']:
            platform_elements = self.mydoc['mmd:mmd']['mmd:platform']
            if isinstance(platform_elements, dict): #Only one element
                platform_elements = [platform_elements] # make it an iterable list
            elif isinstance(platform_elements, str):
                # If comma separated string (by some reason), split...
                platform_elements = platform_elements.split(',')

            for platform in platform_elements:
                print(platform)
                for platform_key, platform_value in platform.items():
                    if isinstance(platform_value,dict): # if sub element is ordered dict
                        print('Platform is in a dict...')
                        for kkey, vvalue in platform_value.items():
                            element_name = 'platform_{}_{}'.format(platform_key.split(':')[-1],kkey.split(':')[-1])
                            if not element_name in mydict.keys(): # create key in mydict
                                mydict[element_name] = []
                                mydict[element_name].append(vvalue)
                            else:
                                mydict[element_name].append(vvalue)
                    else: #sub element is not ordered dicts
                        print('Issue with platform as not a dict...')
                        element_name = 'platform_{}'.format(platform_key.split(':')[-1])
                        if not element_name in mydict.keys(): # create key in mydict. Repetition of above. Should be simplified.
                            mydict[element_name] = []
                            mydict[element_name].append(platform_value)
                        else:
                            mydict[element_name].append(platform_value)

                # Add platform_sentinel for NBS
                initial_platform = mydict['platform_long_name'][0]
                if initial_platform is not None:
                    if initial_platform.startswith('Sentinel'):
                        mydict['platform_sentinel'] = initial_platform[:-1]

        """ Activity type """
        self.logger.info("Processing activity type")
        if 'mmd:activity_type' in self.mydoc['mmd:mmd']:
            mydict['activity_type'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:activity_type'], list):
                for activity_type in self.mydoc['mmd:mmd']['mmd:activity_type']:
                    mydict['activity_type'].append(activity_type)
            else:
                mydict['activity_type'].append(self.mydoc['mmd:mmd']['mmd:activity_type'])

        """ Dataset citation """
        self.logger.info("Processing dataset citation")
        if 'mmd:dataset_citation' in self.mydoc['mmd:mmd']:
            dataset_citation_elements = self.mydoc['mmd:mmd']['mmd:dataset_citation']

            #Only one element
            if isinstance(dataset_citation_elements, dict):
                # make it an iterable list
                dataset_citation_elements = [dataset_citation_elements]

            for dataset_citation in dataset_citation_elements:
                for k, v in dataset_citation.items():
                    element_suffix = k.split(':')[-1]
                    """
                    Fix to handle IMR records
                    Consider to add to MMD/SolR in the future
                    """
                    if element_suffix == "edition":
                        continue
                    """
                    Fix issue between MMD and SolR schema, SolR requires full datetime, MMD not. Also fix any errors in harvested data...
                    """
                    if element_suffix == 'publication_date':
                        if v is None or "Not Available" in v:
                            continue
                        # Check if time format is correct
                        if re.search("T\d{2}:\d{2}:\d{2}:\d{2}Z", v):
                            tmpstr = re.sub("T\d{2}:\d{2}:\d{2}:\d{2}Z", "T12:00:00Z", v)
                            v = tmpstr
                        elif re.search('T\d{2}:\d{2}:\d{2}', v):
                            if not re.search('Z$', v):
                                v += 'Z'
                        elif not re.search("T\d{2}:\d{2}:\d{2}Z", v):
                            v += 'T12:00:00Z'
                    mydict['dataset_citation_{}'.format(element_suffix)] = v

        """
        Quality control
        """
        self.logger.info("Processing quality control information")
        if 'mmd:quality_control' in self.mydoc['mmd:mmd'] and self.mydoc['mmd:mmd']['mmd:quality_control'] != None:
            mydict['quality_control'] = str(self.mydoc['mmd:mmd']['mmd:quality_control'])

        """ Adding MMD document as base64 string"""
        self.logger.info("Packaging MMD XML as base64 string")
        # Check if this can be simplified in the workflow.
        xml_root = ET.parse(str(self.filename))
        xml_string = ET.tostring(xml_root)
        encoded_xml_string = base64.b64encode(xml_string)
        xml_b64 = (encoded_xml_string).decode('utf-8')
        mydict['mmd_xml_file'] = xml_b64

        ## Set default parent child relation. No parent, no child.
        """Set defualt parent/child flags"""
        self.logger.info("Setting default parent/child relations")
        mydict['isParent'] = "false"
        mydict['isChild'] = "false"

##        with open(self.mydoc['mmd:mmd']['mmd:metadata_identifier']+'.txt','w') as myfile:
##            #pickle.dump(mydict,myfile)
##            myjson = json.dumps(mydict)
##            myfile.write(myjson)

        return mydict

class IndexMMD:
    """ Class for indexing SolR representation of MMD to SolR server. Requires
    a list of dictionaries representing MMD as input.
    """

    def __init__(self, mysolrserver, always_commit=False, authentication=None, no_feature=False):
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

        # Feature extraction
        self.no_feature = no_feature

        # Connecting to core
        try:
            self.solrc = pysolr.Solr(mysolrserver, always_commit=always_commit, timeout=1020, auth=authentication)
            self.logger.info("Connection established to: %s", str(mysolrserver))
        except Exception as e:
            self.logger.error("Something failed in SolR init: %s", str(e))
            self.logger.info("Add a sys.exit?")

    #Function for sending explicit commit to solr
    def commit(self):
        self.solrc.commit()

    """
    Primary function to index records, rewritten to expect list input
    """
    def index_record(self, records2ingest, addThumbnail, wms_layer=None, wms_style=None, wms_zoom_level=0, add_coastlines=True, projection=ccrs.PlateCarree(), wms_timeout=120, thumbnail_extent=None):
        # FIXME, update the text below Øystein Godøy, METNO/FOU, 2023-03-19
        """ Add thumbnail to SolR
            Args:
                input_record() : input MMD file to be indexed in SolR
                addThumbnail (bool): If thumbnail should be added or not
                wms_layer (str): WMS layer name
                wms_style (str): WMS style name
                wms_zoom_level (float): Negative zoom. Fixed value added in
                                        all directions (E,W,N,S)
                add_coastlines (bool): If coastlines should be added
                projection (ccrs): Cartopy projection object or name (i.e. string)
                wms_timeout (int): timeout for WMS service
                thumbnail_extent (list): Spatial extent of the thumbnail in
                                      lat/lon [x0, x1, y0, y1]
            Returns:
                bool
        """

        mmd_records = list()
        norec = len(records2ingest)
        i = 1
        for input_record in records2ingest:
            self.logger.info("====>")
            self.logger.info("Processing record %d of %d", i, norec)
            i += 1
            # Do some checking of content
            self.id = input_record['id']
            if input_record['metadata_status'] == 'Inactive':
                self.logger.warning('This record will be set inactive...')
                #return False
            myfeature = None
            """
            If OGC WMS is available, no point in looking for featureType in OPeNDAP.
            """
            if 'data_access_url_ogc_wms' in input_record and addThumbnail:
                self.logger.info("Checking thumbnails...")
                getCapUrl = input_record['data_access_url_ogc_wms']
                if not myfeature:
                    self.thumbnail_type = 'wms'
                self.wms_layer = wms_layer
                self.wms_style = wms_style
                self.wms_zoom_level = wms_zoom_level
                self.add_coastlines = add_coastlines
                self.projection = projection
                self.wms_timeout = wms_timeout
                self.thumbnail_extent = thumbnail_extent
                thumbnail_data = self.add_thumbnail(url=getCapUrl)

                if not thumbnail_data:
                    self.logger.warning('Could not properly parse WMS GetCapabilities document')
                    # If WMS is not available, remove this data_access element from the XML that is indexed
                    del input_record['data_access_url_ogc_wms']
                else:
                    input_record.update({'thumbnail_data':thumbnail_data})
            elif (not self.no_feature) and 'data_access_url_opendap' in input_record:
                # Thumbnail of timeseries to be added
                # Or better do this as part of get_feature_type?
                try:
                    myfeature = self.get_feature_type(input_record['data_access_url_opendap'])
                except Exception as e:
                    self.logger.warning("Something failed while retrieving feature type: %s", str(e))
                if myfeature:
                    self.logger.info('feature_type found: %s', myfeature)
                    input_record.update({'feature_type':myfeature})
            else:
                self.logger.info('Neither gridded nor discrete sampling geometry found in this record...')

            self.logger.info("Adding records to list...")
            mmd_records.append(input_record)

        """
        Send information to SolR
        """
        self.logger.info("Adding records to SolR core.")
        try:
            self.solrc.add(mmd_records)
        except Exception as e:
            self.logger.error("Something failed in SolR adding document: %s", str(e))
            return False
        self.logger.info("%d records successfully added...", len(mmd_records))

        del mmd_records

        return True

    def add_thumbnail(self, url, thumbnail_type='wms'):
        """ Add thumbnail to SolR
            Args:
                type: Thumbnail type. (wms, ts)
            Returns:
                thumbnail: base64 string representation of image
        """
        self.logger.info('Processing %s',url)
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
            self.logger.info('Creating WMS thumbnail for layer: {}'.format(wms_layer))

        # Checking styles
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
        ax.spines['geo'].set_visible(False)
        #ax.outline_patch.set_visible(False)
        ##ax.background_patch.set_visible(False)
        fig.patch.set_alpha(0)
        fig.set_alpha(0)
        fig.set_figwidth(400*px)
        fig.set_figheight(400*px)
        ##fig.set_dpi(100)
        ##ax.background_patch.set_alpha(1)

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
            del data

        thumbnail_b64 = (b'data:image/png;base64,' +
                encode_string).decode('utf-8')
        del encode_string

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
            self.logger.error("Something failed extracting featureType: %s", str(e))
            raise
        ds.close()

        if featureType not in ['point', 'timeSeries', 'trajectory','profile','timeSeriesProfile','trajectoryProfile']:
            self.logger.warning("The featureType found - %s - is not valid", featureType)
            self.logger.warning("Fixing this locally")
            if featureType == "TimeSeries":
                featureType = 'timeSeries'
            elif featureType == "timeseries":
                featureType = 'timeSeries'
            elif featureType == "timseries":
                featureType = 'timeSeries'
            else:
                self.logger.warning("The featureType found is a new typo...")

            #raise

        return(featureType)

    # FIXME check if can be deleted, Øystein Godøy, METNO/FOU, 2023-03-21
    # Not sure if this is needed onwards, but keeping for now.
    def search(self):
        """ Require Id as input """
        try:
            results = solr.search('mmd_title:Sea Ice Extent', df='text_en', rows=100)
        except Exception as e:
            self.logger.error("Something failed during search: %s", str(e))

        return results

    """
    Use solr real-time get to check if a parent is already indexed,
    and have been marked as parent
    """
    def find_parent_in_index(self, id):
        res = requests.get(mySolRc+'/get?id='+id, auth=authentication)
        res.raise_for_status()
        return res.json()

    """
    Update the parent document we got from solr.
    some fields need to be removed for solr to accept the update.
    """
    def solr_updateparent(self, parent):
        if 'full_text' in parent:
            parent.pop('full_text')
        if 'bbox__maxX' in parent:
            parent.pop('bbox__maxX')
        if 'bbox__maxY' in parent:
            parent.pop('bbox__maxY')
        if 'bbox__minX' in parent:
            parent.pop('bbox__minX')
        if 'bbox__minY' in parent:
            parent.pop('bbox__minY')
        if 'bbox_rpt' in parent:
            parent.pop('bbox_rpt')
        if 'ss_access' in parent:
            parent.pop('ss_access')
        if '_version_' in parent:
            parent.pop('_version_')

        parent['isParent'] = True
        return parent

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
    """ FIXME
    if args.level2:
        l2flg = True
    """

    # Read config file
    with open(args.cfgfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    # Specify map projection
    if args.map_projection:
        map_projection = args.map_projection
    else:
        map_projection = cfg['wms-thumbnail-projection']
    if map_projection == 'Mercator':
        mapprojection = ccrs.Mercator()
    elif map_projection == 'PlateCarree':
        mapprojection = ccrs.PlateCarree()
    elif map_projection == 'PolarStereographic':
        mapprojection = ccrs.Stereographic(central_longitude=0.0,central_latitude=90., true_scale_latitude=60.)
    else:
        raise Exception('Map projection is not properly specified in config')

    #Enable basic authentication if configured.
    if 'auth-basic-username' in cfg and 'auth-basic-password' in cfg:
        username = cfg['auth-basic-username']
        password = cfg['auth-basic-password']
        mylog.info("Setting up basic authentication")
        if username == '' or password == '':
            raise Exception('Authentication username and/or password are configured, but have blank strings')
        else:
            authentication = HTTPBasicAuth(username,password)
    else:
        authentication = None
        mylog.info("Authentication disabled")
    #Get solr server config
    SolrServer = cfg['solrserver']
    myCore = cfg['solrcore']

    # Set up connection to SolR server
    mySolRc = SolrServer+myCore
    mysolr = IndexMMD(mySolRc, args.always_commit, authentication, args.no_feature)

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
    elif args.directory:
        try:
            myfiles = os.listdir(args.directory)
        except Exception as e:
            mylog.error("Something went wrong in decoding cmd arguments: %s", e)
            sys.exit(1)

    fileno = 0
    myfiles_pending = []
    files2ingest = []
    pendingfiles2ingest = []
    parentids = set()
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
        else:
            wms_layer = None
        if args.thumbnail_style:
            wms_style = args.thumbnail_style
        else:
            wms_style =  None
        if args.thumbnail_zoom_level:
            wms_zoom_level = args.thumbnail_zoom_level
        else:
            wms_zoom_level=0
        if args.add_coastlines:
            wms_coastlines = args.add_coastlines
        else:
            wms_coastlines=True
        if args.thumbnail_extent:
            thumbnail_extent = [int(i) for i in args.thumbnail_extent[0].split(' ')]
        else:
            thumbnail_extent = None

        mylog.info('\n\tProcessing file: %d - %s',fileno, myfile)

        try:
            mydoc = MMD4SolR(myfile)
        except Exception as e:
            mylog.error('Could not handle file: %s %s', myfile, e)
            continue
        mylog.info('Checking MMD elements.')
        try:
            mydoc.check_mmd()
        except Exception as e:
            mylog.error('File: %s is not compliant with MMD specification', myfile)
            continue
        fileno += 1

        """
        Convert to the SolR format needed
        """
        mylog.info('Converting to SolR format.')
        try:
            newdoc = mydoc.tosolr()
        except Exception as e:
            mylog.warning('Could not process the file: %s', myfile)
            mylog.warning('Message returned: %s', e)
            continue

        if (not args.no_thumbnail) and ('data_access_url_ogc_wms' in newdoc):
            tflg = True
        else:
            tflg = False

        """
        Checking datasets to see if they are children.
        Datasets that are not children are all set to Level-1.
        Make some corrections based on experience for harvested records...
        """
        mylog.info('Parsing parent/child relations.')
        if 'related_dataset' in newdoc:
            # Special fix for NPI
            newdoc['related_dataset'] = newdoc['related_dataset'].replace('https://data.npolar.no/dataset/','')
            newdoc['related_dataset'] = newdoc['related_dataset'].replace('http://data.npolar.no/dataset/','')
            newdoc['related_dataset'] = newdoc['related_dataset'].replace('http://api.npolar.no/dataset/','')
            newdoc['related_dataset'] = newdoc['related_dataset'].replace('.xml','')
            # Skip if DOI is used to refer to parent, that isn't consistent.
            if 'doi.org' in newdoc['related_dataset']:
                continue
            # Fix special characters that SolR doesn't like
            idrepls = [':','/','.']
            myparentid = newdoc['related_dataset']
            for e in idrepls:
                myparentid = myparentid.replace(e,'-')
            # If related_dataset is present, set this dataset as a child using isChild and dataset_type
            newdoc.update({"isChild": "true"})
            newdoc.update({"dataset_type": "Level-2"})
            parentids.add(myparentid)
        else:
            newdoc.update({"isParent": "false"})
            newdoc.update({"dataset_type": "Level-1"})

        # Update list of files to process
        files2ingest.append(newdoc)

    # Check if parents are in the existing list
    for id in parentids:
        if not any(d['id'] == id for d in files2ingest):
            # Check if already ingested and update if so
            # FIXME, need more robustness...
            mylog.warning('This part of parent/child relations is yet not tested.')
            continue
            parent = mysolr.find_parent_in_index(id)
            parent = mysolr.solr_updateparent(parent)
            mysolr.solrc.add([parent])
        else:
            # Assuming found in the current batch of files, then set to parent... Not sure if this is needed onwards, but discussion on how isParent works is needed Øystein Godøy, METNO/FOU, 2023-03-31
            i = 0
            for rec in files2ingest:
                if rec['id'] == id:
                    if 'isParent' in rec:
                        if rec['isParent'] ==  'true':
                            if rec['dataset_type'] == 'Level-1':
                                continue
                            else:
                                files2ingest[i].update({'dataset_type': 'Level-1'})
                        else:
                            files2ingest[i].update({'isParent': 'true'})
                    else:
                        files2ingest[i].update({'isParent': 'true'})
                        files2ingest[i].update({'dataset_type': 'Level-1'})
                i += 1

    if len(files2ingest) == 0:
        mylog.info('No files to ingest.')
        sys.exit()

    # Do the ingestion FIXME
    # Check if thumbnail specification need to be changed
    mylog.info("Indexing datasets")
    """
    Split list into sublists before indexing (and retrieving WMS thumbnails etc)
    """
    mystep = 2500
    myrecs = 0
    for i in range(0,len(files2ingest),mystep):
        mylist = files2ingest[i:i+mystep]
        myrecs += len(mylist)
        try:
            mysolr.index_record(records2ingest=mylist, addThumbnail=tflg)
        except Exception as e:
            mylog.warning('Something failed during indexing %s', e)
        mylog.info('%d records out of %d have been ingested...', myrecs, len(files2ingest))
        del mylist

    if myrecs != len(files2ingest):
        mylog.warning('Inconsistent number of records processed.')
    # Report status
    mylog.info("Number of files processed were: %d", len(files2ingest))

    # Add a commit to solr at end of run, according to magnarem auto commit is done every 10 minutes
    if args.always_commit:
        mylog.info("Committing the input to SolR. This may take some time.")
        mysolr.commit()


if __name__ == "__main__":
    main(sys.argv[1:])
