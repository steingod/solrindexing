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
import math
import re
from collections import OrderedDict
import cartopy.crs as ccrs
import cartopy
import matplotlib.pyplot as plt
from owslib.wms import WebMapService
import base64
#import h5netcdf.legacyapi as netCDF4
import netCDF4
import logging
import lxml.etree as ET
from logging.handlers import TimedRotatingFileHandler
from time import sleep
import pickle
from shapely.geometry import *
from shapely.wkt import loads
from shapely.ops import transform
#from shapely.geometry import mapping
import geojson
import pyproj
import shapely.geos
import shapely.geometry as shpgeo
import shapely.wkt

import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from concurrent import futures as Futures
import threading
import requests
import itertools
from pathlib import Path
import h5netcdf
import validators

from enum import Enum
import json
from typing import List, Union

from shapely import affinity
from shapely.geometry import GeometryCollection, Polygon, mapping

import copy
from functools import reduce
import math
from typing import List, Union
from enum import Enum
import sys

from shapely.geometry import GeometryCollection, LineString, Polygon
from shapely.ops import split
from functools import reduce
from shapely.geometry.base import BaseGeometry


class AcceptedGeojsonTypes(Enum):
    Polygon = 'Polygon'
    MultiPolygon = 'MultiPolygon'

class OutputFormat(Enum):
    Geojson = 'geojson'
    Polygons = 'polygons'
    GeometryCollection = 'geometrycollection'



def check_crossing(lon1: float, lon2: float, validate: bool = True, dlon_threshold: float = 180.0):
    """
    Assuming a minimum travel distance between two provided longitude coordinates,
    checks if the 180th meridian (antimeridian) is crossed.
    """
    if validate and (any(abs(x) > dlon_threshold) for x in [lon1, lon2]):
        raise ValueError("longitudes must be in degrees [-180.0, 180.0]")   
    return abs(lon2 - lon1) > dlon_threshold

def remove_interiors(poly):
    """
    Close polygon holes by limitation to the exterior ring.

    Arguments
    ---------
    poly: shapely.geometry.Polygon
        Input shapely Polygon

    Returns
    ---------
    Polygon without any interior holes
    """
    if poly.interiors:
        return Polygon(list(poly.exterior.coords))
    else:
        return poly


def translate_polygons(
    geometry_collection: GeometryCollection,
    output_format: OutputFormat = OutputFormat.Geojson
) -> Union[List[dict], List[Polygon], GeometryCollection]:

    geo_polygons = []
    for polygon in geometry_collection.geoms:
        (minx, _, maxx, _) = polygon.bounds
        if minx < -180:
            geo_polygon = affinity.translate(polygon, xoff = 360)
        elif maxx > 180:
            geo_polygon = affinity.translate(polygon, xoff = -360)
        else:
            geo_polygon = polygon

        geo_polygons.append(geo_polygon)

    if output_format == OutputFormat.Polygons:
        result = geo_polygons
    if output_format == OutputFormat.Geojson:
        result = [json.dumps(mapping(p)) for p in geo_polygons]
    elif output_format == OutputFormat.GeometryCollection:
        result = GeometryCollection(geo_polygons)

    return result


def split_coords(src_coords: List[List[List[float]]]) -> GeometryCollection:
    coords_shift = copy.deepcopy(src_coords)
    shell_minx = sys.float_info.max
    shell_maxx = sys.float_info.min

    # it is possible that the shape provided may be defined as more than 360
    #   degrees in either direction. Under these circumstances the shifted polygon
    #   would cross both the 180 and the -180 degree representation of the same
    #   meridian. This is not allowed, but checked for using the len(split_meriditans)
    split_meridians = set()

    for ring_index, ring in enumerate(coords_shift):
        if len(ring) < 1: 
            continue
        else:
            ring_minx = ring_maxx = ring[0][0]
            crossings = 0

        for coord_index, (lon, _) in enumerate(ring[1:], start=1):
            lon_prev = ring[coord_index - 1][0] # [0] corresponds to longitude coordinate
            if check_crossing(lon, lon_prev, validate=False):
                direction = math.copysign(1, lon - lon_prev)
                coords_shift[ring_index][coord_index][0] = lon - (direction * 360.0)
                crossings += 1

            x_shift = coords_shift[ring_index][coord_index][0]
            if x_shift < ring_minx: ring_minx = x_shift
            if x_shift > ring_maxx: ring_maxx = x_shift

        # Ensure that any holes remain contained within the (translated) outer shell
        if (ring_index == 0): # by GeoJSON definition, first ring is the outer shell
            shell_minx, shell_maxx = (ring_minx, ring_maxx)
        elif (ring_minx < shell_minx):
            ring_shift = [[x + 360, y] for (x, y) in coords_shift[ring_index]]
            coords_shift[ring_index] = ring_shift
            ring_minx, ring_maxx = (x + 360 for x in (ring_minx, ring_maxx))
        elif (ring_maxx > shell_maxx):
            ring_shift = [[x - 360, y] for (x, y) in coords_shift[ring_index]]
            coords_shift[ring_index] = ring_shift
            ring_minx, ring_maxx = (x - 360 for x in (ring_minx, ring_maxx))

        if crossings: # keep track of meridians to split on
            if ring_minx < -180: split_meridians.add(-180)
            if ring_maxx > 180: split_meridians.add(180)

    n_splits = len(split_meridians)
    if n_splits == 0:
        shell, *holes = src_coords
        split_polygons = GeometryCollection([Polygon(shell, holes)])
    elif n_splits == 1:
        split_lon = split_meridians.pop()
        meridian = [[split_lon, -90.0], [split_lon, 90.0]]
        splitter = LineString(meridian)

        shell, *holes = coords_shift
        split_polygons = split(Polygon(shell, holes), splitter)
    else:
        raise NotImplementedError(
            """Splitting a Polygon by multiple meridians (MultiLineString) 
               not supported by Shapely"""
        )
    return split_polygons


def split_polygon(geojson: dict, output_format: OutputFormat = OutputFormat.Geojson) -> Union[
    List[dict], List[Polygon], GeometryCollection
]:
    """
    Given a GeoJSON representation of a Polygon, returns a collection of
    'antimeridian-safe' constituent polygons split at the 180th meridian, 
    ensuring compliance with GeoJSON standards (https://tools.ietf.org/html/rfc7946#section-3.1.9)
    Assumptions:
      - Any two consecutive points with over 180 degrees difference in
        longitude are assumed to cross the antimeridian
      - The polygon spans less than 360 degrees in longitude (i.e. does not wrap around the globe)
      - However, the polygon may cross the antimeridian on multiple occasions
    Parameters:
        geojson (dict): GeoJSON of input polygon to be split. For example:
                {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [179.0, 0.0], [-179.0, 0.0], [-179.0, 1.0],
                            [179.0, 1.0], [179.0, 0.0]
                        ]
                    ]
                }
        output_format (str): Available options: "geojson", "polygons", "geometrycollection"
                             If "geometrycollection" returns a Shapely GeometryCollection.
                             Otherwise, returns a list of either GeoJSONs or Shapely Polygons
    Returns:
        List[dict]/List[Polygon]/GeometryCollection: antimeridian-safe polygon(s)
    """
    geotype = AcceptedGeojsonTypes(geojson['type'])
    if geotype is AcceptedGeojsonTypes.Polygon:
        split_polygons = split_coords(geojson['coordinates'])
    elif geotype is AcceptedGeojsonTypes.MultiPolygon:
        split_polygons = reduce(
            GeometryCollection.union,
            (split_coords(coords) for coords in geojson['coordinates'])
        )
    return translate_polygons(split_polygons, output_format)



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
    #parser.add_argument('-f','--feature_type',help='Extract featureType during ingestion (to be done automatically).', action='store_true')
    parser.add_argument('-r','--remove',help='Remove the dataset with the specified identifier (to be replaced by searchindex).')
    parser.add_argument('-2','--level2',action='store_true', help='Operate on child core.')

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
    mylog.setLevel(logging.ERROR)
    #logging.basicConfig(level=logging.INFO,
    #        format='%(asctime)s - %(levelname)s - %(message)s')
    myformat = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.ERROR)
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

    def __init__(self, mydoc):
        # Set up logging
        self.logger = logging.getLogger('indexdata.MMD4SolR')
        self.logger.info('Creating an instance of IndexMMD')
        """ set variables in class """
        #self.filename = filename
        self.jsonld = []
        self.mydoc = mydoc


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
        try:
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
        except:
            self.logger.warning('\n\t Something went wrong with gcmd')
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
            #print(myvalue)
            if re.search(r'\+\d\d:\d\dZ$', myvalue) is not None:
                myvalue = re.sub(r'\+\d\d:\d\d','',myvalue)
            mydate = dateutil.parser.isoparse(myvalue)
            #print(myvalue)
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
                              'Extended metadata':'ext_metadata',
                              'Scientific publication':'scientific_publication',
                              'Data paper':'data_paper',
                              'Data management plan':'data_management_plan',
                              'Other documentation':'other_documentation',
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
        if myid == 'Unkown':
            return None
        """ Last metadata update """
        if 'mmd:last_metadata_update' in self.mydoc['mmd:mmd']:
            last_metadata_update = self.mydoc['mmd:mmd']['mmd:last_metadata_update']

            lmu_datetime = []
            lmu_type = []
            lmu_note = []
            # FIXME check if this works correctly
            # NOTE: Fix for some nbs xml files that have the wrong structure
            if isinstance(last_metadata_update, str):
                lmu_datetime.append(str(last_metadata_update).strip())
            elif isinstance(last_metadata_update['mmd:update'], dict): #Only one last_metadata_update element
                    lmu_datetime.append(str(last_metadata_update['mmd:update']['mmd:datetime']))
                    lmu_type.append(last_metadata_update['mmd:update']['mmd:type'])
                    if 'mmd:note' in last_metadata_update['mmd:update']:
                        lmu_note.append(last_metadata_update['mmd:update']['mmd:note'])
                    else:
                        lmu_note.append('Not provided')
            else: # multiple last_metadata_update elements
                for i,e in enumerate(last_metadata_update['mmd:update']):
                    lmu_datetime.append(str(e['mmd:datetime']))
                    lmu_type.append(e['mmd:type'])
                    if 'mmd:note' in e.keys():
                        lmu_note.append(e['mmd:note'])

            i = 0
            for myel in lmu_datetime:
                i+=1
                if myel.endswith('Z'):
                    continue
                else:
                    lmu_datetime[i-1] = myel+'Z'
            #Check  and fixdate format validity
            for i, date in enumerate(lmu_datetime):
                test = re.match(DATETIME_REGEX, date)
                if not test:
                    #print(type(date))
                    if re.search(r'\+\d\d:\d\dZ$', date) is not None:
                        date = re.sub(r'\+\d\d:\d\d','',date)
                    newdate = dateutil.parser.parse(date)
                    date = newdate.strftime('%Y-%m-%dT%H:%M:%SZ')
                    #print(date)
                    lmu_datetime[i] = date
                    #self.logger.error("Dateformat not solr-compatible, document will fail during indexing")
            mydict['last_metadata_update_datetime'] = lmu_datetime
            mydict['last_metadata_update_type'] = lmu_type
            mydict['last_metadata_update_note'] = lmu_note

        """ Metadata status """
        #print(mydict)
        if not 'mmd:metadata_status' in self.mydoc['mmd:mmd']:
                #print('Did not find metadata_status for document %s. Setting to Inactive'%(mydict['id']))
                mydict['metadata_status'] = 'Inactive'
                return None
        elif isinstance(self.mydoc['mmd:mmd']['mmd:metadata_status'],dict):
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
                    if e['@xml:lang'] == 'no':
                        mydict['title_no'] = e['#text']
                elif '@lang' in e:
                    if e['@lang'] == 'en':
                        mydict['title'] = e['#text']
                    if e['@lang'] == 'no':
                        mydict['title_no'] = e['#text']
        else:
            if isinstance(self.mydoc['mmd:mmd']['mmd:title'],dict):
                if '@xml:lang' in self.mydoc['mmd:mmd']['mmd:title']:
                    if self.mydoc['mmd:mmd']['mmd:title']['@xml:lang'] == 'en':
                        mydict['title'] = self.mydoc['mmd:mmd']['mmd:title']['#text']
                    if self.mydoc['mmd:mmd']['mmd:title']['@xml:lang'] == 'no':
                        mydict['title_no'] = self.mydoc['mmd:mmd']['mmd:title']['#text']
                if '@lang' in self.mydoc['mmd:mmd']['mmd:title']:
                    if self.mydoc['mmd:mmd']['mmd:title']['@lang'] == 'en':
                        mydict['title'] = self.mydoc['mmd:mmd']['mmd:title']['#text']
                    if self.mydoc['mmd:mmd']['mmd:title']['@lang'] == 'no':
                        mydict['title_no'] = self.mydoc['mmd:mmd']['mmd:title']['#text']
            else:
                mydict['title'] = str(self.mydoc['mmd:mmd']['mmd:title'])

        """ abstract """
        if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'], list):
            for e in self.mydoc['mmd:mmd']['mmd:abstract']:
                if '@xml:lang' in e:
                    if e['@xml:lang'] == 'en':
                        mydict['abstract'] = e['#text']
                    if e['@xml:lang'] == 'no':
                        mydict['abstract_no'] = e['#text']
                elif '@lang' in e:
                    if e['@lang'] == 'en':
                        mydict['abstract'] = e['#text']
                    if e['@lang'] == 'no':
                        mydict['abstract_no'] = e['#text']
        else:
            if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'],dict):
                if '@xml:lang' in self.mydoc['mmd:mmd']['mmd:abstract']:
                    if self.mydoc['mmd:mmd']['mmd:abstract']['@xml:lang'] == 'en':
                        mydict['abstract'] = self.mydoc['mmd:mmd']['mmd:abstract']['#text']
                    if self.mydoc['mmd:mmd']['mmd:abstract']['@xml:lang'] == 'no':
                        mydict['abstract_no'] = self.mydoc['mmd:mmd']['mmd:abstract']['#text']
                if '@lang' in self.mydoc['mmd:mmd']['mmd:abstract']:
                    if self.mydoc['mmd:mmd']['mmd:abstract']['@lang'] == 'en':
                        mydict['abstract'] = self.mydoc['mmd:mmd']['mmd:abstract']['#text']
                    if self.mydoc['mmd:mmd']['mmd:abstract']['@lang'] == 'no':
                        mydict['abstract_no'] = self.mydoc['mmd:mmd']['mmd:abstract']['#text']
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
                #test = re.match(DATETIME_REGEX, mydict['temporal_extent_start_date'])
                #if not test:
                #    self.logger.error("Start date is not solr-compatible")

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

                            #print(mapping(point))
                            #mydict['geom'] = geojson.dumps(mapping(point))
                    else:
                        #fix out of bounds
                        #minX = min(lonvals)
                        #maxX = max(la)
                        #if min(lonvals)

                        bbox = box(min(lonvals), min(latvals), max(lonvals), max(latvals))

                        #print("First conditition")
                        #print(bbox)
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

                #print("Second conditition")
                #Check if we have a point or a boundingbox
                if float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']) == float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']):
                    if float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']) == float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']):
                        point = shpgeo.Point(float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']),float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']))
                        #print(point.y)
                        mydict['polygon_rpt'] = point.wkt

                        #print(mapping(point))

                        #mydict['geom'] = geojson.dumps(mapping(point))

                else:
                    bbox = box(float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']), float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']), float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']), float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']), ccw=False)
                    #print(bbox)
                    polygon = bbox.wkt
                    #print(polygon)
                    #p = shapely.geometry.polygon.orient(shapely.wkt.loads(polygon), sign=1.0)
                    #print(p.exterior.is_ccw)
                    mydict['polygon_rpt'] = polygon
                    #print(mapping(shapely.wkt.loads(polygon)))
                    #print(geojson.dumps(mapping(loads(polygon))))
                    #pGeo = shpgeo.shape({'type': 'polygon', 'coordinates': tuple(newCoord)})
                    #mydict['geom'] = geojson.dumps(mapping(p))
                    #print(mydict['geom'])

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
                self.logger.warning('Both license identifier and resource need to be present to index this properly')
                mydict['use_constraint_identifier'] =  "Not provided"
                mydict['use_constraint_resource'] =  "Not provided"
            if 'mmd:license_text' in self.mydoc['mmd:mmd']['mmd:use_constraint']:
                mydict['use_constraint_license_text'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint']['mmd:license_text'])

        """ Personnel """
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
                    if data_access_wms_layers is not None:
                        mydict[data_access_wms_layers_string] = [ i for i in data_access_wms_layers.values()][0]

        """ Related dataset """
        """ TODO """
        """ Remember to add type of relation in the future ØG """
        """ Only interpreting parent for now since SolR doesn't take more
            Added handling of namespace in identifiers
        """
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
                                for e in idrepls:
                                    mydict['related_dataset'] = mydict['related_dataset'].replace(e,'-')
            else:
                """ Not sure if this is used?? """
                if '#text' in dict(self.mydoc['mmd:mmd']['mmd:related_dataset']):
                    mydict['related_dataset'] = self.mydoc['mmd:mmd']['mmd:related_dataset']['#text']
                    for e in idrepls:
                        mydict['related_dataset'] = mydict['related_dataset'].replace(e,'-')

        """ Storage information """
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
        if 'mmd:related_information' in self.mydoc['mmd:mmd']:

            related_information_elements = self.mydoc['mmd:mmd']['mmd:related_information']

            if isinstance(related_information_elements, dict): #Only one element
                related_information_elements = [related_information_elements] # make it an iterable list

            for related_information in related_information_elements:
                for key, value in related_information.items():
                    element_name = 'related_information_{}'.format(key.split(':')[-1])

                    if value in related_information_LUT.keys():
                        mydict['related_url_{}'.format(related_information_LUT[value])] = related_information['mmd:resource']
                        if 'mmd:description' in related_information:
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
        # Added double indexing of GCMD keywords. keywords_gcmd  (and keywords_wigos) are for faceting in SolR. What is shown in data portal is keywords_keyword.
        if 'mmd:keywords' in self.mydoc['mmd:mmd']:
            mydict['keywords_keyword'] = []
            mydict['keywords_vocabulary'] = []
            mydict['keywords_gcmd'] = []
            mydict['keywords_wigos'] = [] # Not used yet
            # If there is only one keyword list
            if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], dict):
                #if not 'mmd_keyword' in self.mydoc['mmd:mmd']['mmd:keywords']:
                #    print('missing keywords for dataset %s' % (mydict['metadata_identifier']))
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
        # FIXME add check for empty sub elements...
        if 'mmd:platform' in self.mydoc['mmd:mmd']:

            platform_elements = self.mydoc['mmd:mmd']['mmd:platform']
            if isinstance(platform_elements, dict): #Only one element
                platform_elements = [platform_elements] # make it an iterable list

            for platform in platform_elements:
                for platform_key, platform_value in platform.items():
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
                if initial_platform is not None:
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
                    # Fix issue between MMD and SolR schema, SolR requires full datetime, MMD not.
                    if element_suffix == 'publication_date':
                        if v is not None:
                            solrdate = re.match(DATETIME_REGEX, v)
                            if not solrdate:
                                v+='T12:00:00Z'
                            test = re.match(DATETIME_REGEX, v)
                            if not test:
                                #print(type(date))
                                if re.search(r'\+\d\d:\d\dZ$', v) is not None:
                                  date = re.sub(r'\+\d\d:\d\d','',v)
                                newdate = dateutil.parser.parse(date)
                                v = newdate.strftime('%Y-%m-%dT%H:%M:%SZ')

                    mydict['dataset_citation_{}'.format(element_suffix)] = v

        """ Quality control """
        if 'mmd:quality_control' in self.mydoc['mmd:mmd'] and self.mydoc['mmd:mmd']['mmd:quality_control'] != None:
            mydict['quality_control'] = str(self.mydoc['mmd:mmd']['mmd:quality_control'])

        """ Adding MMD document as base64 string"""
        # Check if this can be simplified in the workflow.
        xml_mmd = xmltodict.unparse(self.mydoc)
        #print(type(xml_mmd))
        #xml_root = ET.parse(xml_mmd)
        #xml_string = ET.tostring(xml_root)
        xml_mmd_bytes = xml_mmd.encode('utf-8')
        base64_bytes = base64.b64encode(xml_mmd_bytes)
        #encoded_xml_string = base64.b64encode(xml_string)
        xml_b64 = (base64_bytes).decode('utf-8')
        mydict['mmd_xml_file'] = xml_b64

##        with open(self.mydoc['mmd:mmd']['mmd:metadata_identifier']+'.txt','w') as myfile:
##            #pickle.dump(mydict,myfile)
##            myjson = json.dumps(mydict)
##            myfile.write(myjson)

        #print(mydict)
        #sys.exit(1)
        return mydict

class IndexMMD:
    """ Class for indexing SolR representation of MMD to SolR server. Requires
    a list of dictionaries representing MMD as input.
    """

    def __init__(self, mysolrserver, always_commit=False, authentication=None):
        # Set up logging
        self.logger = logging.getLogger('indexdata.IndexMMD')
        self.logger.info('Creating an instance of IndexMMD')

        # level variables

        self.level = None

        self.docList = list()

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
            self.solrc = pysolr.Solr(mysolrserver, always_commit=always_commit, timeout=1020, auth=authentication)
            self.logger.info("Connection established to: %s", str(mysolrserver))
        except Exception as e:
            self.logger.error("Something failed in SolR init: %s", str(e))

    #Function for sending explicit commit to solr
    def commit(self):
        self.solrc.commit()

    def index_record(self, input_record, addThumbnail, level=None, wms_layer=None, wms_style=None, wms_zoom_level=0, add_coastlines=True, projection=ccrs.PlateCarree(), wms_timeout=120, thumbnail_extent=None, feature_type=None):
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
            self.logger.warning('Skipping record')
            return False
        myfeature = None
        if 'data_access_url_opendap' in input_record:
            """Thumbnail of timeseries to be added
            Or better do this as part of get_feature_type?"""
            if feature_type is None:
                try:
                    myfeature = self.get_feature_type(input_record['data_access_url_opendap'])
                except Exception as e:
                    self.logger.warning("Something failed while retrieving feature type: %s", str(e))
                    #raise RuntimeError('Something failed while retrieving feature type')
                if myfeature:
                    self.logger.info('feature_type found: %s', myfeature)
                    input_record.update({'feature_type':myfeature})
            elif feature_type == 'Skip':
                myfeature = None
            else:
                myfeature = feature_type

        self.id = input_record['id']
        if 'data_access_url_ogc_wms' in input_record and addThumbnail == True:
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

        self.logger.info("Adding records to core...")

        #mmd_record = list()
        #mmd_record.append(input_record)

        # try:
        #     self.solrc.add(mmd_record)
        # except Exception as e:
        #     self.logger.error("Something failed in SolR adding document: %s", str(e))
        #     return False
        # self.logger.info("Record successfully added.")

        return input_record

    def add_level2(self, myl2record, addThumbnail=False, projection=ccrs.Mercator(), wmstimeout=120, wms_layer=None, wms_style=None, wms_zoom_level=0, add_coastlines=True, wms_timeout=120, thumbnail_extent=None, feature_type=None):
        """ Add a level 2 dataset, i.e. update level 1 as well """
        mmd_record2 = list()

        # Fix for NPI data...
        myl2record['related_dataset'] = myl2record['related_dataset'].replace('http://data.npolar.no/dataset/','')
        myl2record['related_dataset'] = myl2record['related_dataset'].replace('https://data.npolar.no/dataset/','')
        myl2record['related_dataset'] = myl2record['related_dataset'].replace('http://api.npolar.no/dataset/','')
        myl2record['related_dataset'] = myl2record['related_dataset'].replace('.xml','')

        # Add additonal helper fields for handling in SolR and Drupal
        myl2record['isChild'] = 'true'

        myfeature = None
        if 'data_access_url_opendap' in myl2record:
            """Thumbnail of timeseries to be added
            Or better do this as part of get_feature_type?"""
            if feature_type == None:
                try:
                    myfeature = self.get_feature_type(myl2record['data_access_url_opendap'])
                except Exception as e:
                    self.logger.error("Something failed while retrieving feature type: %s", str(e))
                    #raise RuntimeError('Something failed while retrieving feature type')
                if myfeature:
                    self.logger.info('feature_type found: %s', myfeature)
                    myl2record.update({'feature_type':myfeature})

        self.id = myl2record['id']
        # Add thumbnail for WMS supported datasets
        if 'data_access_url_ogc_wms' in myl2record and addThumbnail:
            self.logger.info("Checking tumbnails...")
            if not myfeature:
                self.thumbnail_type = 'wms'
            self.wms_layer = wms_layer
            self.wms_style = wms_style
            self.wms_zoom_level = wms_zoom_level
            self.add_coastlines = add_coastlines
            self.projection = projection
            self.wms_timeout = wms_timeout
            self.thumbnail_extent = thumbnail_extent
            if 'data_access_url_ogc_wms' in myl2record.keys():
                getCapUrl = myl2record['data_access_url_ogc_wms']
                try:
                    thumbnail_data = self.add_thumbnail(url=getCapUrl)
                except Exception as e:
                    self.logger.error("Something failed in adding thumbnail: %s", str(e))
                    warnings.warning("Couldn't add thumbnail.")

        if addThumbnail and thumbnail_data:
            myl2record.update({'thumbnail_data':thumbnail_data})

        mmd_record2.append(myl2record)

        """ Retrieve level 1 record """
        myparid = myl2record['related_dataset']
        idrepls = [':','/','.']
        for e in idrepls:
            myparid = myparid.replace(e,'-')
        try:
            myresults = self.solrc.search('id:' + myparid, **{'wt':'python','rows':100})
        except Exception as e:
            self.logger.error("Something failed in searching for parent dataset, " + str(e))

        # Check that only one record is returned
        if len(myresults) != 1:
            self.logger.warning("Didn't find unique parent record, skipping record")
            return
        # Convert from pySolr results object to dict and return.
        for result in myresults:
            if 'full_text' in result:
                result.pop('full_text')
            if 'bbox__maxX' in result:
                result.pop('bbox__maxX')
            if 'bbox__maxY' in result:
                result.pop('bbox__maxY')
            if 'bbox__minX' in result:
                result.pop('bbox__minX')
            if 'bbox__minY' in result:
                result.pop('bbox__minY')
            if 'bbox_rpt' in result:
                result.pop('bbox_rpt')
            if 'ss_access' in result:
                result.pop('ss_access')
            if '_version_' in result:
                result.pop('_version_')
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

        ##print(myresults)

        # """ Index level 2 dataset """
        # try:
        #     self.solrc.add(mmd_record2)
        # except Exception as e:
        #     raise Exception("Something failed in SolR add level 2", str(e))
        # self.logger.info("Level 2 record successfully added.")

        # """ Update level 1 record with id of this dataset """
        # try:
        #     self.solrc.add(mmd_record1)
        # except Exception as e:
        #     raise Exception("Something failed in SolR update level 1 for level 2", str(e))
        # self.logger.info("Level 1 record successfully updated.")
        mmd_record1.extend(mmd_record2)
        return mmd_record1

    def add_thumbnail(self, url, thumbnail_type='wms'):
        """ Add thumbnail to SolR
            Args:
                type: Thumbnail type. (wms, ts)
            Returns:
                thumbnail: base64 string representation of image
        """
        print(url)
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
        fig.set_figwidth(4.5)
        fig.set_figheight(4.5)
        fig.set_dpi(100)
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
        myopendap.strip()
        # Open as OPeNDAP
        try:
            ds = netCDF4.Dataset(myopendap)
        except Exception as e:
            self.logger.error("Something failed reading dataset: %s" %e)
            return None
        # Try to get the global attribute featureType
        try:
            featureType = ds.getncattr('featureType')
        except Exception as e:
            self.logger.warning("no featureType: %s" % e)
            ds.close()
            return None
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

        return featureType

    def delete_level1(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        self.logger.info("Deleting %s from level 1.", datasetid)
        try:
            self.solrc.delete(id=datasetid)
        except Exception as e:
            self.logger.error("Something failed in SolR delete: %s", str(e))
            raise

        self.logger.info("Record successfully deleted from Level 1 core")

    def delete_level2(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        self.logger.info("Deleting %s from level 2.", datasetid)
        try:
            self.solr2.delete(id=datasetid)
        except Exception as e:
            self.logger.error("Something failed in SolR delete: %s", str(e))
            raise

        self.logger.info("Records successfully deleted from Level 2 core")

    def delete_thumbnail(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        self.logger.info("Deleting %s from thumbnail core.", datasetid)
        try:
            self.solrt.delete(id=datasetid)
        except Exception as e:
            self.logger.error("Something failed in SolR delete: %s", str(e))
            raise

        self.logger.info("Records successfully deleted from thumbnail core")

    def search(self):
        """ Require Id as input """
        try:
            results = self.mysolr.search('mmd_title:Sea Ice Extent', df='text_en', rows=100)
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

## Some test functions for multiprocessing and multi threading
def concurrently(fn, inputs, *, max_concurrency=5):
    """
    Calls the function ``fn`` on the values ``inputs``.
    ``fn`` should be a function that takes a single input, which is the
    individual values in the iterable ``inputs``.
    Generates (input, output) tuples as the calls to ``fn`` complete.
    See https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/ for an explanation
    of how this function works.
    """
    # Make sure we get a consistent iterator throughout, rather than
    # getting the first element repeatedly.
    fn_inputs = iter(inputs)

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fn, input): input
            for input in itertools.islice(fn_inputs, max_concurrency)
        }

        while futures:
            done, _ = Futures.wait(
                futures, return_when=Futures.FIRST_COMPLETED, timeout=None
            )

            for fut in done:
                original_input = futures.pop(fut)
                yield original_input, fut.result()

            for input in itertools.islice(fn_inputs, len(done)):
                fut = executor.submit(fn, input)
                futures[fut] = input



def get_feature_type(myopendap):
    """ Set feature type from OPeNDAP """
    myopendap.strip()
    # Open as OPeNDAP
    try:
        ds = netCDF4.Dataset(str(myopendap), 'r')
    except Exception as e:
        print("Something failed reading dataset: %s" % e)
        return None
    # Try to get the global attribute featureType
    try:
        featureType = ds.getncattr('featureType')
    except Exception as e:
        #print("no featureType: %s" % e)
        ds.close()
        return None
    ds.close()

    if featureType not in ['point', 'timeSeries', 'trajectory','profile','timeSeriesProfile','trajectoryProfile']:
        if featureType == "TimeSeries":
            featureType = 'timeSeries'
        elif featureType == "timeseries":
            featureType = 'timeSeries'
        elif featureType == "timseries":
            featureType = 'timeSeries'
        else:
            return None

        #raise

    return featureType
 #load mmd and return contents
def load_file(filename):
    """
    Load xml file and convert to dict using xmltodict
    """
    filename = filename.strip()
    try:
        file = Path(filename)
    except Exception as e:
        print('Not a valid filepath %s error was %s' %(filename,e))
        return None
    with open(file, encoding='utf-8') as fd:
        try:
            xmlfile = fd.read()
        except Exception as e:
            print('Clould not read file %s error was %s' %(filename,e))
            return None
        try:
            mmddict = xmltodict.parse(xmlfile)
        except Exception as e:
            print('Could not parse the xmlfile: %s  with error %s' %(filename,e))
            return None
        return mmddict

# Load multiple mmd files using multi-threading
def load_files(filelist):
    """
    Multithreaded function to load files using the load_file function
    """
    with ThreadPoolExecutor(threads) as exe:
        # load files
        futures = [exe.submit(load_file, name) for name in filelist]
        # collect data
        mmd_list = [future.result() for future in futures]
        # return data and file paths
        return (mmd_list)

def solr_updateparent(parent):
    """
    Update the parent document we got from solr.
    some fields need to be removed for solr to accept the update.
    """
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

def find_parent_in_index(id):
    """
    Use solr real-time get to check if a parent is already indexed,
    and have been marked as parent
    """
    res = requests.get(mySolRc+'/get?id='+id, auth=authentication)
    res.raise_for_status()
    return res.json()

def flip(x, y):
    """Flips the x and y coordinate values"""
    return y, x

def process_feature_type(tmpdoc):
    """
    Look for feature type and update document
    """
    tmpdoc_ = tmpdoc
    if 'data_access_url_opendap' in tmpdoc:
        dapurl = str(tmpdoc['data_access_url_opendap'])
        valid = validators.url(dapurl)
        if not valid:
            return tmpdoc_
        #print(dapurl)
        
        try:
            with netCDF4.Dataset(dapurl, 'r') as f:
              
                #if attribs is not None:
                for att in f.ncattrs():
                    if att == 'featureType':
                        featureType = getattr(f, att)
                        #print(featureType)
                        if featureType not in ['point', 'timeSeries', 'trajectory','profile','timeSeriesProfile','trajectoryProfile']:
                            if featureType == "TimeSeries":
                                featureType = 'timeSeries'
                            elif featureType == "timeseries":
                                featureType = 'timeSeries'
                            elif featureType == "timseries":
                                featureType = 'timeSeries'
                            else:
                                featureType = None
                        if featureType:
                            tmpdoc_.update({'feature_type':featureType})
                    
                    #Check if we have plogon.
                    #print("netcdf")
                    if att == 'geospatial_bounds':
                        polygon = getattr(f, att)
                        polygon_ = shapely.wkt.loads(polygon)
                        type = polygon_.geom_type
                        #print(type)
                        if type == 'Point':
                            point = polygon_.wkt
                            tmpdoc.update({'geospatial_bounds': point})
                        else:
                            polygon = transform(flip,polygon_)
                            # #print(poly)
                            ccw = polygon.exterior.is_ccw
                            if not ccw:
                                polygon = polygon.reverse()
                           
                            # pp = json.dumps(mapping(polygon))
                            # poly: dict = json.loads(pp)
                            
                            # res = split_polygon(poly, OutputFormat.Polygons)
                            # mp = shapely.MultiPolygon(res)
                            #tmp = reduce(BaseGeometry.union, res)
                            #print(mp.wkt)
                
                            #res_poly = json.loads(res) #print(res.wkt)
                            #print(res.wkt)
                            #polygon_ = shapely.force_3d(polygon_)
                            #polygon = polygon_.wkt
                            polygon = remove_interiors(polygon)
                            #polygon = transform(flip,polygon_).wkt
                            #polygon_geojson = json.dumps(shapely.geometry.mapping(polygon_))
                            #print(polygon_geojson)
                            #gj = geojson.GeoJSON()
                            #gf = geojson.Feature(tmpdoc['id'],polygon_)
                            #gj.update(gf)
                            #polygon = polygon.simplify(0.5)       
                            #print(gj)
                            #tmpdoc.update({'polygon_rpt': polygon.wkt})
                            tmpdoc.update({'geospatial_bounds': polygon.wkt})
                            #Check if we have plogon.
                    if att == 'geospatial_bounds_crs':
                        crs = getattr(f, att)
                        tmpdoc_.update({'geographic_extent_polygon_srsName':crs})

                return tmpdoc_
        except Exception as e:
            print("Something failed reading netcdf %s"% e)
            print(dapurl)
           
        
    return tmpdoc_

def mmd2solr(mmd,status,mysolr,file):
    """
    Convert mmd dict to solr dict

    Check for presence of children and mark them as children.
    If children found return parentid together with the solrdoc
    """

    #(mmd, status) = doc
    if mmd is None:
        print("Warning file %s was not parsed" %file)
        return (None,status)
    #print(mmd, status)
    mydoc = MMD4SolR(mmd)
    try:
        mydoc.check_mmd()
    except Exception as e:
        print("File %s did not pass the mmd check, cannot index. Reason: %s" % (file,e))
        return(None,status)

    #DEBUG MMD
    #import pprint
    #pp = pprint.PrettyPrinter(indent=2, depth=6)
    #print(mmd)
    #sys.exit(1)
    #print(mmd['mmd:mmd']['mmd:metadata_identifier'])
    #print(type(mmd['mmd:mmd']['mmd:last_metadata_update']['mmd:update']))

    #Convert mmd xml dict to solr dict
    try:
        tmpdoc = mydoc.tosolr()
    except Exception as e:
        print("File %s could not be converted to solr document. Reason: %s" % (file,e))
        return(None,status)

    #File could not be processed
    if tmpdoc is None:
        print("Warning solr document for file %s was empty" % (file))
        return (None,status)
    if 'id' not in tmpdoc:
        print("WARNING  file %s have no id. Missing metadata_identifier?" % file)
        return (None,status)

    if tmpdoc['id'] == None or tmpdoc['id'] == 'Unknown':
        print("Skipping process file %s. Metadata identifier: Unknown, or missing" % file)
        return (None,status)
    #if tmpdoc['metadata_status'] != "Active":
    #    print("WARNING indexed file %s  with metadata_status: %s" %(file, tmpdoc['metadata_status']))
    #print(tmpdoc)
    #Check if dates are ok. If not we cannot index, report.
    try:
        (start_date,) = tmpdoc['temporal_extent_start_date']
    except Exception as e:
        print("Could not find start date in  %s. Reason: %s" % (file,e))
        return(None,status)
    #print(start_date)
    test = re.match(DATETIME_REGEX, start_date)

    if not test:
        print('Incomaptible start date %s in document % s, file: %s' %(tmpdoc['temporal_extent_start_date'],tmpdoc['metadata_identifier'],file))

        return (None, status)
    if 'temporal_extent_end_date' in tmpdoc:
        try:
            (end_date,) = tmpdoc['temporal_extent_end_date']
        except Exception as e:
            print("Could extract end date in  %s. Reason: %s" % (file,e))
            return(None,status)

        test = re.match(DATETIME_REGEX, end_date)
        if not test:
            print('Incomaptible end date %s in document %s ' %(tmpdoc['temporal_extent_start_date'],tmpdoc['metadata_identifier']))
            return (None, status)

    tmpstatus = "OK"
    #Check geographic extent is ok
    try:
        if round(tmpdoc['geographic_extent_rectangle_north'][0]) not in range(-91,91):
            tmpstatus = None
        if round(tmpdoc['geographic_extent_rectangle_south'][0]) not in range(-91,91):
            tmpstatus = None
        if round(tmpdoc['geographic_extent_rectangle_east'][0]) not in range(-181,181):
            tmpstatus = None
        if round(tmpdoc['geographic_extent_rectangle_west'][0]) not in range(-181,181):
            tmpstatus = None
    except Exception as e:
        print("Missing information in document %s. Reason %s" % (file,e))

    if tmpstatus != "OK":
         print('Incomaptible geographic extent for docid:  %s for file %s' % (tmpdoc['id'],file))
         return (None,status)
    #pp.pprint(tmpdoc)
    #print(tmpdoc['related_dataset'],type)
    #Check if we have a child pointing to a parent
    # if feature_type is None:
    #     process_feature_type(tmpdoc)
    if 'polygon_rpt' in tmpdoc:
        value = tmpdoc['polygon_rpt']
        if 'POLYGON' in value or 'POINT' in value:
            polygon_ = shapely.wkt.loads(value)
            type = polygon_.geom_type
            if type == 'Point':
                point = polygon_.wkt
                tmpdoc.update({'geospatial_bounds': point})
            else:
                #polygon = transform(flip,polygon_)
                pp = json.dumps(mapping(polygon_))
                poly: dict = json.loads(pp)
                #print(poly)
              
                res = split_polygon(poly, OutputFormat.Polygons)
                #tmp = reduce(BaseGeometry.union, res)
                for p in res:
                    pol = remove_interiors(p)
                    ccw = pol.exterior.is_ccw
                    if not ccw:
                        pol = pol.reverse()
                           
                    
                mp = shapely.MultiPolygon(pol)
                # print(mp)
                # #print(polygon.wkt)
                # res_poly = shape(res.pop())
                # print(res_poly)
                #polygon_ = shapely.force_3d(polygon_)
                #polygon = polygon_.wkt

                #polygon = transform(flip,polygon_).wkt
                #polygon_geojson = json.dumps(shapely.geometry.mapping(polygon_))
                #print(polygon_geojson)
                #gj = geojson.GeoJSON()
                #gf = geojson.Feature(tmpdoc['id'],polygon_)
                #gj.update(gf)
                        
                #print(gj)
                tmpdoc.update({'geospatial_bounds': mp.wkt})
                tmpdoc.update({'polygon_rpt': mp.wkt})

    #Override frature_type if set in config
    if feature_type != "Skip" and feature_type is not None:
        tmpdoc.update({'feature_type':feature_type})
    #If we got level2 flag from cmd arguments we make it a child/Level-2
    if l2flg:
        tmpdoc.update({'dataset_type':'Level-2'})
        tmpdoc.update({'isChild': True })

    if 'related_dataset' in tmpdoc:
        #print("got related dataset")
        if isinstance(tmpdoc['related_dataset'],str):
            #print("processing child")
            #Manipulate the related_dataset id to solr id
            # Special fix for NPI
            tmpdoc['related_dataset'] = tmpdoc['related_dataset'].replace('https://data.npolar.no/dataset/','')
            tmpdoc['related_dataset'] = tmpdoc['related_dataset'].replace('http://data.npolar.no/dataset/','')
            tmpdoc['related_dataset'] = tmpdoc['related_dataset'].replace('http://api.npolar.no/dataset/','')
            tmpdoc['related_dataset'] = tmpdoc['related_dataset'].replace('.xml','')
            # Skip if DOI is used to refer to parent, that isn't consistent.
            if not 'doi.org' in tmpdoc['related_dataset']:
                #Update document with child specific fields
                tmpdoc.update({'dataset_type':'Level-2'})
                tmpdoc.update({'isChild': True})

                # Fix special characters that SolR doesn't like
                idrepls = [':','/','.']
                myparentid = tmpdoc['related_dataset']
                for e in idrepls:
                    myparentid = myparentid.replace(e,'-')

                status = myparentid.strip()
                tmpdoc.update({'related_dataset': myparentid})
                #pp.pprint(tmpdoc)
                #print(status)
    else:
        #Assume we have level-1 doc that are not parent
        tmpdoc.update({'dataset_type':'Level-1'})
        tmpdoc.update({'isParent': False})

    return (tmpdoc,status)

#Pocess and tranforms multiple mmd files
def process_mmd(mmd_list, status_list):
    """
    Mutithreaded processing of mmd2solr conversion
    """
    with ThreadPoolExecutor(threads) as exe:
        arglist = zip(mmd_list,status_list)
        # convert mmd to solr doc
        futures = [exe.submit(mmd2solr, item) for item in arglist]
        # collect data
        result = [future.result() for future in futures]
        solr_docs, status = zip(*result)
        #print(solr_docs, status)
        # return data and file paths
        return solr_docs,status

#add documents to solr
def add2solr(docs,msg_callback):
    try:
        solrcon.add(docs)
    except Exception as e:
        print("Some documents failed to be added to solr. reason: %s" % e)
    #print("indexed %s documents" % len(docs))
    msg_callback("%s, PID: %s completed indexing %s documents!" % (threading.current_thread().name, threading.get_native_id(),len(docs)))

#mmessage callback for index trheads
def msg_callback(msg):
    print(msg)

#Process the mmd list and index to solr
def bulkindex(filelist,mysolr, chunksize):

    #Define some lists to keep track of the processing
    parent_ids_pending = list()  # Keep track of pending parent ids
    parent_ids_processed =list() # Keep track parent ids already processed
    parent_ids_found = list()    # Keep track of parent ids found

    #Total files given to the bulkindexer
    total_in = len(filelist)

    #keep track of batch process
    indexthreads = list()
    files_processed = 0
    docs_indexed = 0
    docs_skipped = 0
    it = 1
    doc_ids_processed = set()
    print("######### BATCH START ###########################")
    for i in range(0, len(filelist), chunksize):
        # select a chunk
        files = filelist[i:(i + chunksize)]
        docs = list()
        statuses = list()

        ######################## STARTING THREADS ########################
        #Load each file using multiple threads, and process documents as files are loaded
        ###################################################################
        for(file, mmd) in concurrently(fn=load_file, inputs=files):

            # Get the processed document and its status
            doc,status = mmd2solr(mmd,None,mysolr,file)

            # Add the document and the status to the document-list
            docs.append(doc)
            statuses.append(status)
        ################################## THREADS FINISHED ##################

        # Check if we got some children in the batch pointing to a parent id
        parentids = set([element for element in statuses if element != None])
        #print(parentids)

        # Check if the parent(s) of the children(s) was found before. If not, we add them to found.
        for pid in parentids:
            if pid not in parent_ids_found:
                parent_ids_found.append(pid)
            if pid not in parent_ids_pending and pid not in parent_ids_processed:
                parent_ids_pending.append(pid)

        # Check if the parent(s) of the children(s) we found was processed. If so, we do not process agian
        for pid in parent_ids_processed:
            if pid in parentids:
                parentids.remove(pid)
        for pid in parent_ids_found:
            if pid in parentids:
                parentids.remove(pid)

        # Files processed so far
        files_processed += len(files)

        # Gnereate a list of documents to send to solr.
        # Documents that could not be opened, parsed or converted to solr documents are skipped
        docs_= len(docs) # Number of documents processed
        docs = [el for el in docs if el != None] # List of documents that can be indexed
        docs_skipped += (docs_ - len(docs)) # Update # of skipped documents

        #keep track of all document ids we have indexed, so we do not have to check solr for a parent more than we need
        docids_ = [doc['id'] for doc in docs]
        doc_ids_processed.update(docids_)

        # print("===========================================================================================================")
        # print("=== BAtch no. %s ========= Files processed %s ==== Docs processed %s  ==== Docs skipped %s ==== Docs indexed %s ======" %(it, files_processed, len(docs), docs_skipped_,docs_indexed))
        # print("Parent ids found: %s" % len(parent_ids_found))
        # print("Parent ids pending: %s" % len(parent_ids_pending))
        # print("Parent ids processed: %s" % len(parent_ids_processed))
        # print("Parent ids pending list: %s" % parent_ids_pending)
        # print("===========================================================================================================")

        # TODO: SEGFAULT NEED TO INVESTIGATE
        # Process feature types here, using the concurrently function,
        dap_docs = [doc for doc in docs if 'data_access_url_opendap' in doc]
        #print('dap docs: %s' % len(dap_docs))
        #print('nodap docs: %s' % len(docs)) 
        if feature_type is None:
            ######################## STARTING THREADS ########################
            #Load each file using multiple threads, and process documents as files are loaded
            ###################################################################
            for(doc, newdoc) in concurrently(fn=process_feature_type, inputs=dap_docs, max_concurrency=10):
                docs.remove(doc)
                docs.append(newdoc)
            ################################## THREADS FINISHED ##################


        #Run over the list of parentids found in this chunk, and look for the parent
        parent_found = False
        for pid in parentids:
            #print("checking parent: %s" % pid)
            #Firs we check if the parent dataset are in our jobs
            myparent = None
            parent = [el for el in docs if el['id'] == pid]
            #print("parents found in this chunk: %s" % parent)

            #Check if we have the parent in this chunk
            if len(parent) > 0:
                myparent = parent.pop()
                myparent_ = myparent
                #print("parent found in current chunk: %s " % myparent['id'])
                parent_found = True
                if myparent['isParent'] is False:
                    #print('found pending parent %s in this job.' % pid)
                    #print('updating pending parent')

                    docs.remove(myparent) #Remove original
                    myparent_.update({'isParent': True})
                    docs.append(myparent_)

                    #Remove from pending list
                    if pid in parent_ids_pending:
                        parent_ids_pending.remove(pid)

                    #add to processed list for reference
                    parent_ids_processed.append(pid)

            #Check if the parent is already in the index, and flag it as parent if not done already
            if pid in doc_ids_processed and not parent_found:
                myparent = find_parent_in_index(pid)

                if myparent is not None:
                    #if not found in the index, we store it for later
                    if myparent['doc'] is None:
                        if pid not in parent_ids_pending:
                            #print('parent %s not found in index. storing it for later' % pid)
                            parent_ids_pending.append(pid)

                    #If found in index we update the parent
                    else:
                        if myparent['doc'] is not None:
                            #print("parent found in index: %s, isParent: %s" %(myparent['doc']['id'], myparent['doc']['isParent']))
                            #Check if already flagged
                            if myparent['doc']['isParent'] is False:
                                #print('Update on indexed parent %s, isParent: True' % pid)
                                mydoc = solr_updateparent(myparent['doc'])
                                #print(mydoc)
                                doc_ = mydoc
                                try:
                                    solrcon.add([doc_])
                                except Exception as e:
                                    print("Could update parent on index. reason %s",e)

                                #Update lists
                                parent_ids_processed.append(pid)

                                #Remove from pending list
                                if pid in parent_ids_pending:
                                    parent_ids_pending.remove(pid)

        #Last we check if parents pending previous chunks is in this chunk
        ppending = set(parent_ids_pending)
        #print(" == Checking Pending == ")
        for pid in  ppending:
            #Firs we check if the parent dataset are in our jobs
            myparent = None
            parent = [el for el in docs if el['id'] == pid]

            if len(parent) > 0:
                myparent = parent.pop()
                myparent_ = myparent
                #print("pending parent found in current chunk: %s " % myparent['id'])
                parent_found = True
                if myparent['isParent'] is False:
                    #print('found unprocessed pending parent %s in this job.' % pid)
                    #print('updating parent')

                    docs.remove(myparent) #Remove original
                    myparent_.update({'isParent': True})
                    docs.append(myparent_)

                    #Remove from pending list
                    if pid in parent_ids_pending:
                        parent_ids_pending.remove(pid)

                    #add to processed list for reference
                    parent_ids_processed.append(pid)

            #If the parent was proccesd, asume it was indexed before flagged
            if pid in doc_ids_processed and not parent_found:
                myparent = find_parent_in_index(pid)

                #If we did not find the parent in this job, check if it was already indexed
                if myparent['doc'] is not None:
                    #print("pending parent found in index: %s, isParent: %s" %(myparent['doc']['id'], myparent['doc']['isParent']))

                    if myparent['doc']['isParent'] is False:
                        #print('Update on indexed parent %s, isParent: True' % pid)
                        #print('before: ' , myparent)
                        mydoc_ = solr_updateparent(myparent['doc'])
                        mydoc = mydoc_
                        #doc = {'id': pid, 'isParent': True}
                        try:
                            solrcon.add([mydoc])
                        except Exception as e:
                            print("Could not update parent on index. reason %s",e)

                        #Update lists
                        parent_ids_processed.append(pid)

                        #Remove from pending list
                        if pid in parent_ids_pending:
                            parent_ids_pending.remove(pid)

        # TODO: Add posibility to not index datasets that are already in the index
            # 1. Generate a list of doc ids from the docs to be indexed.
            # 2. Search in solr for the ids
            # 3. If the document was indexed
                # remove document from docs to be indexed


        #Keep track of docs indexed and batch iteration
        docs_indexed += len(docs)
        it += 1

        # Send processed documents to solr  for indexing as a new thread.
        # max threads is set in config
        indexthread = threading.Thread(target=add2solr, name="Index thread %s" % (len(indexthreads)+1), args=(docs,msg_callback))
        indexthreads.append(indexthread)
        indexthread.start()

        #If we have reached maximum threads, we wait until finished
        if len(indexthreads) >= threads:
            for thr in indexthreads[:-1]:
                thr.join()

     #   print("===================================")
     #   print("Added %s documents to solr. Total: %s" % (len(docs),docs_indexed))
     #   print("===================================")


    ############### BATCH LOOP END  ############################
    # wait for any threads still running to complete
    for thr in indexthreads:
        thr.join()


    #Last we assume all pending parents are in the index
    ppending = set(parent_ids_pending)
    print(" The last parents should be in index.")
    for pid in  ppending:
        myparent = None
        myparent = find_parent_in_index(pid)
        if myparent['doc'] is not None:
            #print("pending parent found in index: %s, isParent: %s" %(myparent['doc']['id'], myparent['doc']['isParent']))

            if myparent['doc']['isParent'] is False:
                #print('Update on indexed parent %s, isParent: True' % pid)
                #print('before: ' , myparent)
                mydoc_ = solr_updateparent(myparent['doc'])

                #doc = {'id': pid, 'isParent': True}
                try:
                    solrcon.add([mydoc_])
                except Exception as e:
                    print("Could not update parent on index. reason %s",e)
                        #Update lists
                parent_ids_processed.append(pid)

                #Remove from pending list
                if pid in parent_ids_pending:
                    parent_ids_pending.remove(pid)

    #####################################################################################
    print("====== BATCH END == %s files processed in %s iterations, using batch size %s =======" % (len(filelist),it, chunksize))
    print("Parent ids found: %s" % len(parent_ids_found))
    print("Parent ids pending: %s" % len(parent_ids_pending))
    print("Parent ids processed: %s" % len(parent_ids_processed))
    print("Parent ids pending list: %s" % parent_ids_pending)
    print("======================================================================================")

    #summary of possible missing parents
    missing = list(set(parent_ids_found) - set(parent_ids_processed))
    print('Missing parents in input. %s' % missing)
    docs_failed = total_in - docs_indexed
    if docs_failed != 0:
        print('**WARNING** %s documents could not be indexed. check output and logffile.' % docs_failed)
         #print(parent_ids_pending)
    print("===================================================================")
    print("%s files processed and %s documents indexed. %s documents was skipped" %(files_processed,docs_indexed,docs_skipped))
    print("===================================================================")
    print("Total files given as input: %s " % len(filelist))

    return docs_indexed

def main(argv):
    #Global date regexp to validate solr dates
    global DATETIME_REGEX
    DATETIME_REGEX = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(\.\d+)?Z$"  # NOQA: E501
)

    # start time
    st = time.perf_counter()
    pst = time.process_time()
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

    #Set given arguments as global variables for subprocesses to access.
    global tflg
    global l2flg
    global fflg
    tflg = l2flg = fflg = False
    if args.level2:
        l2flg = True

    # Read config file
    with open(args.cfgfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    #Global mapprojection
    global mapprojection
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

    #Global wms config from cmd arguments
    # FIXME, need a better way of handling this, WMS layers should be interpreted automatically, this way we need to know up fron whether WMS makes sense or not and that won't work for harvesting
    global wms_layer
    global wms_style
    global wms_zoom_level
    global wms_coastlines
    global thumbnail_extent
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

    #Enable basic authentication if configured.
    global authentication
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

    #Set batch size from config.
    batch = 1000 # Default to 1000
    if 'batch-size' in cfg:
        batch = cfg["batch-size"]

    workers = 2 # Default to 2
    if 'workers' in cfg:
        workers = cfg["workers"]

    global threads
    threads = 8
    if 'threads' in cfg:
        threads = cfg["threads"]

    #Should we commit to solr at the end of execution?
    end_solr_commit = False
    if 'end-solr-commit' in cfg:
        if cfg['end-solr-commit'] is True:
            end_solr_commit = cfg['end-solr-commit']
    #Get config for feature_type handeling
    global feature_type
    feature_type=None
    if 'skip-feature-type' in cfg:
        if cfg['skip-feature-type'] is True:
            feature_type = 'Skip'
    if 'override-feature-type' in cfg:
        feature_type=cfg['override-feature-type']

    #Get solr server config
    SolrServer = cfg['solrserver']
    myCore = cfg['solrcore']

    # Set up connection to SolR server
    global mySolRc
    mySolRc = SolrServer+myCore
    #print(mySolRc)
    global solrcon
    try:
        solrcon = pysolr.Solr(mySolRc, always_commit=False, timeout=1020, auth=authentication)
        print("Connection established to: %s" % mySolRc)
    except Exception as e:
        print("Something failed while connecting to solr %s with error: %s" %(mySolRc,e))
        sys.exit(1)
    mysolr = IndexMMD(mySolRc, args.always_commit, authentication)
    docList = list()
    # Find files to process
    # FIXME remove l2 and thumbnail cores, reconsider deletion herein
    if args.input_file:
        myfiles = [args.input_file]
        batch = 1 #Batch always 1 when single file is indexed
        workers = 1 #We only need one worker to process one file
    elif args.list_file:
        try:
            f2 = open(args.list_file, "r")
        except IOError as e:
            mylog.error('Could not open file: %s %e', args.list_file, e)
            sys.exit(1)
        myfiles = f2.readlines()
        f2.close()
    elif args.remove:
        mysolr.delete_level1(args.remove)
        sys.exit()
    elif args.remove and args.level2:
        mysolr.delete_level2(args.remove)
        sys.exit()
    elif args.remove and args.thumbnail:
        mysolr.delete_thumbnail(args.remove)
        sys.exit()
    elif args.directory:
        try:
            myfiles = os.listdir(args.directory)
            if len(myfiles) == 0:
                mylog.error("Given directory %s is empty", args.directory)
            myfiles = [os.path.join(args.directory, f) for f in os.listdir(args.directory) if f.endswith('.xml')]

        except Exception as e:
            mylog.error("Could not find directory: %s", e)
            sys.exit(1)
    # We only process files that have xml extensions.
    # Remove files we do not use
    #myfiles = [x for x in myfiles if not x.endswith('.xml')]
    if len(myfiles) == 0:
        mylog.error('No files to process. exiting')
        sys.exit(1)


    fileno = 0
    print("Files to process: ", len(myfiles))
    # If batch size is set to greater than number of files provided for processing,
    # we set the batch size to the number of files
    if batch > len(myfiles):
        batch = len(myfiles)
        if batch < 100:
            workers = 1 # With small list of files more workers just make extra overhead

    if feature_type != "Skip" and feature_type is not None:
        print( " ** WARNING!! ** feature type is set to override. stop process now if this is a mistake (Ctrl C)")
        sleep(5)
    #Start the indexing
    print("Indexing with batch size %s" % batch)
    processed = bulkindex(myfiles,mysolr,batch)





    #     """ Do not search for metadata_identifier, always used id...  """
    #     # Convert MMD dict to solr doc
    #     try:
    #         newdoc = mydoc.tosolr()
    #     except Exception as e:
    #         mylog.warning('Could  convert MMD dict to solr doc: %s', e)
    #         continue
    #     if (newdoc['metadata_status'] == "Inactive"):
    #         continue
    #     if (not args.no_thumbnail) and ('data_access_url_ogc_wms' in newdoc):
    #         tflg = True
    #     # Do not directly index children unless they are requested to be children. Do always assume that the parent is included in the indexing process so postpone the actual indexing to allow the parent to be properly indexed in SolR.
    #     if 'related_dataset' in newdoc:
    #         # Special fix for NPI
    #         newdoc['related_dataset'] = newdoc['related_dataset'].replace('https://data.npolar.no/dataset/','')
    #         newdoc['related_dataset'] = newdoc['related_dataset'].replace('http://data.npolar.no/dataset/','')
    #         newdoc['related_dataset'] = newdoc['related_dataset'].replace('http://api.npolar.no/dataset/','')
    #         newdoc['related_dataset'] = newdoc['related_dataset'].replace('.xml','')
    #         # Skip if DOI is used to refer to parent, that isn't consistent.
    #         if 'doi.org' in newdoc['related_dataset']:
    #             continue
    #         # Fix special characters that SolR doesn't like
    #         idrepls = [':','/','.']
    #         myparent = newdoc['related_dataset']
    #         for e in idrepls:
    #             myparent = myparent.replace(e,'-')
    #         #myresults = mysolr.solrc.search('id:' + newdoc['related_dataset'], **{'wt':'python','rows':100})
    #         myresults = mysolr.solrc.search('id:' + myparent, **{'wt':'python','rows':100})
    #         if len(myresults) == 0:
    #             mylog.warning("No parent found. Staging for second run.")
    #             myfiles_pending.append(myfile)
    #             continue
    #         elif not l2flg:
    #             mylog.warning("Parent found, but assumes parent will be reindexed, thus postponing indexing of children until SolR is updated.")
    #             myfiles_pending.append(myfile)
    #             continue
    #     mylog.info("Indexing dataset: %s", myfile)
    #     if l2flg:
    #         solrDocList = mysolr.add_level2(mydoc.tosolr(), addThumbnail=tflg, projection=mapprojection, wmstimeout=120, wms_layer=wms_layer, wms_style=wms_style, wms_zoom_level=wms_zoom_level, add_coastlines=wms_coastlines, wms_timeout=cfg['wms-timeout'], thumbnail_extent=thumbnail_extent, feature_type=feature_type)
    #         docList.extend(solrDocList)
    #     else:
    #         if tflg:
    #             try:
    #                 solrDoc = mysolr.index_record(input_record=mydoc.tosolr(), addThumbnail=tflg, wms_layer=wms_layer,wms_style=wms_style, wms_zoom_level=wms_zoom_level, add_coastlines=wms_coastlines, projection=mapprojection,  wms_timeout=cfg['wms-timeout'],thumbnail_extent=thumbnail_extent, feature_type=feature_type)
    #                 docList.append(solrDoc)
    #             except Exception as e:
    #                 mylog.warning('Something failed during indexing %s', e)
    #         else:
    #             try:
    #                 solrDoc = mysolr.index_record(input_record=mydoc.tosolr(), addThumbnail=tflg, feature_type=feature_type)
    #                 docList.append(solrDoc)
    #             except Exception as e:
    #                 mylog.warning('Something failed during indexing %s', e)
    #     if not args.level2:
    #         l2flg = False
    #     tflg = False
    #     if len(docList) == batch:
    #         try:
    #             print("Adding documents to solr", str(len(docList)))
    #             mysolr.solrc.add(docList)
    #         except Exception as e:
    #             self.logger.error("Something failed in SolR adding document: %s", str(e))
    #         docList = list()

    #     #self.logger.info("Record successfully added.")
    # if len(docList) > 0:
    #     try:
    #         print("Adding %s documents to solr", str(len(docList)))
    #         mysolr.solrc.add(docList)
    #     except Exception as e:
    #         self.logger.error("Something failed in SolR adding document: %s", str(e))
    #     docList = list()
    # # Now process all the level 2 files that failed in the previous
    # # sequence. If the Level 1 dataset is not available, this will fail at
    # # level 2. Meaning, the section below only ingests at level 2.
    # docList = list()
    # fileno = 0
    # if len(myfiles_pending)>0 and not args.always_commit:
    #     mylog.info('Processing files that were not possible to process in first take. Waiting 20 minutes to allow SolR to update recently ingested parent datasets. ')
    #     #sleep(20*60)
    #     mysolr.commit()
    # for myfile in myfiles_pending:
    #     mylog.info('\tProcessing L2 file: %d - %s',fileno, myfile)
    #     try:
    #         mydoc = MMD4SolR(myfile)
    #     except Exception as e:
    #         mylog.warning('Could not handle file: %s', e)
    #         continue
    #     mydoc.check_mmd()
    #     fileno += 1
    #     """ Do not search for metadata_identifier, always used id...  """
    #     """ Check if this can be used???? """
    #     newdoc = mydoc.tosolr()
    #     if 'data_access_resource' in newdoc.keys():
    #         for e in newdoc['data_access_resource']:
    #             #print('>>>>>e', e)
    #             if (not nflg) and "OGC WMS" in (''.join(e)):
    #                 tflg = True
    #     # Skip file if not a level 2 file
    #     if 'related_dataset' not in newdoc:
    #         continue
    #     mylog.info("Indexing dataset: %s", myfile)
    #     # Ingest at level 2
    #     solrDocList = mysolr.add_level2(mydoc.tosolr(), addThumbnail=tflg, projection=mapprojection, wmstimeout=120, wms_layer=wms_layer, wms_style=wms_style, wms_zoom_level=wms_zoom_level, add_coastlines=wms_coastlines, wms_timeout=cfg['wms-timeout'], thumbnail_extent=thumbnail_extent)
    #     docList.extend(solrDocList)
    #     tflg = False
    #     if len(docList) == batch:
    #         try:
    #             print("Adding %s documents to solr", str(len(docList)))
    #             mysolr.solrc.add(docList)
    #         except Exception as e:
    #             self.logger.error("Something failed in SolR adding document: %s", str(e))
    #         docList = list()
    # if len(docList) > 0:
    #     try:
    #         print("Adding %s documents to solr", str(len(docList)))
    #         mysolr.solrc.add(docList)
    #     except Exception as e:
    #         self.logger.error("Something failed in SolR adding document: %s", str(e))
    #     docList = list()

    # Report status
    #mylog.info("Number of files processed were: %d", len(myfiles))
    #rint("Number of files processed were: %d" % processed)
    #with open("bulkidx2.json", "w") as f:
    #    for doc in docList:
    #        f.write(json.dumps(doc))
    #        f.write("\n")
    #add a commit to solr at end of run
    # get the end time
    et = time.perf_counter()
    pet = time.process_time()
    elapsed_time = et - st
    pelt = pet -pst
    skipped = len(myfiles)-processed
    print("Processed %s documents" % processed)
    print("Files / documents failed: %s" % skipped)
    print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
    print('CPU time:', time.strftime("%H:%M:%S", time.gmtime(pelt)))
    if end_solr_commit:
        st = time.perf_counter()
        mysolr.commit()
        et = time.perf_counter()



if __name__ == "__main__":
    main(sys.argv[1:])
