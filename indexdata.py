#!/usr/bin/python3
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
    Øystein Godøy, METNO/FOU, 2019-05-31
        Integrated modifications from Trygve Halsne and Massimo Di Stefano
    Øystein Godøy, METNO/FOU, 2018-04-19
        Added support for level 2

NOTES:
    - under rewrite...
    - Should support ingestion of directories as well...

"""

import sys
import os.path
import getopt
import subprocess
import pysolr
import xmltodict
import dateutil.parser
import warnings
import json
from collections import OrderedDict
import cartopy.crs as ccrs
import cartopy
import matplotlib.pyplot as plt
from owslib.wms import WebMapService
import base64
import netCDF4


def usage():
    print('')
    print('Usage: ' + sys.argv[0] + ' -i <dataset_name> -c <core_name> [-h]')
    print('\t-h: dump this text')
    print('\t-i: index an individual dataset')
    print('\t-l: index individual datasetis from list file')
    print('\t-d: index a directory with multiple datasets')
    print('\t-c: core name (e.g. normap, sios, nbs)')
    print('\t-2: index level 2 dataset')
    print('\t-t: index a single thumbnail (no argument, require -i or -d)')
    print('\t-f: index a single feature type (no argument, require -i or -d)')
    print('')
    sys.exit(2)


class MMD4SolR:
    """ Read and check MMD files, convert to dictionary """

    def __init__(self, filename):
        """ set variables in class """
        self.filename = filename
        with open(self.filename, encoding='utf-8') as fd:
            self.mydoc = xmltodict.parse(fd.read())

    def check_mmd(self):
        """ Check and correct MMD if needed """
        """ Remember to check that multiple fields of abstract and title
        have set xml:lang= attributes... """

        """
        Check for presence of required elements
        Temporal and spatial extent are not required as of now.
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
        print('\n')
        for requirement in mmd_requirements.keys():
            if requirement in self.mydoc['mmd:mmd']:
                if len(self.mydoc['mmd:mmd'][requirement]) > 1:
                    print(self.mydoc['mmd:mmd'][requirement])
                    print('\t' + requirement + ' is present and non empty')
                    mmd_requirements[requirement] = True

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
                                       'geoscientificinformation',
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
                               'YOPP'],
            'mmd:dataset_production_status': ['Planned',
                                              'In Work',
                                              'Complete',
                                              'Obsolete'],
        }
        for element in mmd_controlled_elements.keys():
            print('Checking ' + element)
            if element in self.mydoc['mmd:mmd']:
                if type(self.mydoc['mmd:mmd'][element]) is list:
                    if all(elem in mmd_controlled_elements[element] for elem in self.mydoc['mmd:mmd'][element]):
                        print('\t' + element + ' is all good...')
                    else:
                        print('\t' + element + ' contains non valid content')
                        print('(' + self.mydoc['mmd:mmd'][element] + ')')
                else:
                    if self.mydoc['mmd:mmd'][element] in mmd_controlled_elements[element]:
                        print('\t' + element + ' is all good...')
                    else:
                        print('\t' + element + ' contains non valid content')
                        print('(' + self.mydoc['mmd:mmd'][element] + ')')

        """
        Check that keywords also contain GCMD keywords
        Need to check contents more specifically...
        """
        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], list):
            i = 0
            gcmd = False
            print(type(self.mydoc['mmd:mmd']['mmd:keywords']))
            print(len(self.mydoc['mmd:mmd']['mmd:keywords']))
            # TODO: remove unused for loop
            # Switch to using e instead of self.mydoc...
            for e in self.mydoc['mmd:mmd']['mmd:keywords']:
                if str(self.mydoc['mmd:mmd']['mmd:keywords'][i]).upper() == 'GCMD':
                    gcmd = True
                    break
                i += 1
            if not gcmd:
                print('Keywords in GCMD are not available')
        else:
            if not str(self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary']).upper() == 'GCMD':
                # warnings.warn('Keywords in GCMD are not available')
                print('Keywords in GCMD are not available')

        """ Modify dates if necessary """
        if 'mmd:temporal_extent' in self.mydoc['mmd:mmd']:
            for mykey in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                mydate = dateutil.parser.parse(str(self.mydoc['mmd:mmd']['mmd:temporal_extent'][mykey]))
                self.mydoc['mmd:mmd']['mmd:temporal_extent'][mykey] = mydate.strftime('%Y-%m-%dT%H:%M:%SZ')

    def tosolr(self):
        """ Collect required elements """
        mydict = OrderedDict({"id": str(self.mydoc['mmd:mmd']['mmd:metadata_identifier']),
                              "mmd_metadata_identifier": str(self.mydoc['mmd:mmd']['mmd:metadata_identifier']),
                              "mmd_metadata_status": str(self.mydoc['mmd:mmd']['mmd:metadata_status'])
                              })
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
            mydict['mmd_title'] = str(self.mydoc['mmd:mmd']['mmd:title']['#text'])

        """ abstract """
        if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'], list):
            i = 0
            for e in self.mydoc['mmd:mmd']['mmd:abstract']:
                if self.mydoc['mmd:mmd']['mmd:abstract'][i]['@xml:lang'] == 'en':
                    mydict['mmd_abstract'] = self.mydoc['mmd:mmd']['mmd:abstract'][i]['#text'].encode('utf-8')
                i += 1
        else:
            mydict['mmd_abstract'] = str(self.mydoc['mmd:mmd']['mmd:abstract']['#text'])

        """ Last metadata update """
        if 'mmd:last_metadata_update' in self.mydoc['mmd:mmd']:
            mydict['mmd_last_metadata_update'] = str(self.mydoc['mmd:mmd']['mmd:last_metadata_update'])

        """ Dataset production status """
        if 'mmd:dataset_production_status' in self.mydoc['mmd:mmd']:
            mydict['mmd_dataset_production_status'] = str(self.mydoc['mmd:mmd']['mmd:dataset_production_status'])

        """ Dataset status """
        if 'mmd:metadata_status' in self.mydoc['mmd:mmd']:
            mydict['mmd_metadata_status'] = str(self.mydoc['mmd:mmd']['mmd:metadata_status'])

        """ Collection """
        if 'mmd:collection' in self.mydoc['mmd:mmd']:
            mydict['mmd_collection'] = []
            # if len(self.mydoc['mmd:mmd']['mmd:collection']) > 1: #Does not work on single collection
            if isinstance(self.mydoc['mmd:mmd']['mmd:collection'], list):  # Does not work on single collection
                i = 0
                for e in self.mydoc['mmd:mmd']['mmd:collection']:
                    mydict['mmd_collection'].append(
                        self.mydoc['mmd:mmd']['mmd:collection'][i].encode('utf-8'))
                    i += 1
            else:
                mydict['mmd_collection'] = self.mydoc['mmd:mmd']['mmd:collection'].encode('utf-8')

        """ ISO TopicCategory """
        if 'mmd:iso_topic_category' in self.mydoc['mmd:mmd']:
            mydict['mmd_iso_topic_category'] = []
            mydict['mmd_iso_topic_category'].append(self.mydoc['mmd:mmd']['mmd:iso_topic_category'])

        """ Keywords """
        """ Should structure this on GCMD only at some point """
        if 'mmd:keywords' in self.mydoc['mmd:mmd']:
            mydict['mmd_keywords_keyword'] = []
            print(self.mydoc['mmd:mmd']['mmd:keywords'])
            if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'], list):
                for i in range(len(self.mydoc['mmd:mmd']['mmd:keywords'])):
                    print(i,type(self.mydoc['mmd:mmd']['mmd:keywords'][i]))
                    print(i,(self.mydoc['mmd:mmd']['mmd:keywords'][i]))
                    if 'mmd:keyword' not in self.mydoc['mmd:mmd']['mmd:keywords'][i]:
                        continue
                    if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'],str):
                        mydict['mmd_keywords_keyword'].append(self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword'])
                    else:
                        for e in (self.mydoc['mmd:mmd']['mmd:keywords'][i]['mmd:keyword']):
                            mydict['mmd_keywords_keyword'].append(e)
            else:
                for e in (self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword']):
                    mydict['mmd_keywords_keyword'].append(e)

        """ Temporal extent """
        if 'mmd:temporal_extent' in self.mydoc['mmd:mmd']:
            mydict["mmd_temporal_extent_start_date"] = str(
                self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:start_date']),
            if 'mmd:end_date' in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                mydict["mmd_temporal_extent_end_date"] = str(
                    self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:end_date']),

        """ Geographical extent """
        if 'mmd:geographic_extent' in self.mydoc['mmd:mmd']:
            mydict['mmd_geographic_extent_rectangle_north'] = float(
                self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']),
            mydict['mmd_geographic_extent_rectangle_south'] = float(
                self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']),
            mydict['mmd_geographic_extent_rectangle_east'] = float(
                self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']),
            mydict['mmd_geographic_extent_rectangle_west'] = float(
                self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']),

        """ Data access """
        """ Double check this ØG """
        """ Especially description """
        if 'mmd:data_access' in self.mydoc['mmd:mmd']:
            mydict['mmd_data_access_resource'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:data_access'], list):
                i = 0
                # TODO: remove unused for loop
                # Switch to using e instead of self.mydoc...
                for e in self.mydoc['mmd:mmd']['mmd:data_access']:
                    mydict['mmd_data_access_resource'].append(
                        '\"'.encode('utf-8') +
                        self.mydoc['mmd:mmd']['mmd:data_access'][i]['mmd:type'].encode('utf-8') +
                        '\":\"'.encode('utf-8') + self.mydoc['mmd:mmd']['mmd:data_access'][i]['mmd:resource'].encode(
                            'utf-8') +
                        '\",description\":'.encode('utf-8')
                    )
                    i += 1
            else:
                mydict['mmd_data_access_resource'] = [
                    '\"' + self.mydoc['mmd:mmd']['mmd:data_access']['mmd:type'] +
                    '\":\"' + self.mydoc['mmd:mmd']['mmd:data_access']['mmd:resource'] + '\"'
                ]

        """ Related information """
        """ Must be updated to hold mutiple ØG """
        mydict['mmd_related_information_resource'] = []
        if 'mmd:related_information' in self.mydoc['mmd:mmd']:
            # There can potentially be several related_information sections.
            # Need to handle this later. TODO
            # Assumes all child elements are present if parent is found
            mystring = '\"' + self.mydoc['mmd:mmd']['mmd:related_information']['mmd:type'] + '\":\"' + self.mydoc['mmd:mmd']['mmd:related_information']['mmd:resource'] + '\",\"description\":'
            mydict['mmd_related_information_resource'].append(mystring)

        """ Related dataset """
        """ TODO """
        """ Remember to add type of relation in the future ØG """
        if 'mmd:related_dataset' in self.mydoc['mmd:mmd']:
            if '#text' in dict(self.mydoc['mmd:mmd']['mmd:related_dataset']):
                mydict['mmd_related_dataset'] = self.mydoc['mmd:mmd']['mmd:related_dataset']['#text']

        """ Project """
        if 'mmd:project' in self.mydoc['mmd:mmd']:
            # mydict['mmd_project_short_name'].append(
            #        self.mydoc['mmd:mmd']['mmd:project']['mmd:short_name'].encode('utf-8'))
            # mydict['mmd_project_long_name'].append(
            #        self.mydoc['mmd:mmd']['mmd:project']['mmd:long_name'].encode('utf-8'))
            if self.mydoc['mmd:mmd']['mmd:project']['mmd:short_name']:
                mydict['mmd_project_short_name'] = self.mydoc['mmd:mmd']['mmd:project']['mmd:short_name'].encode(
                    'utf-8')
            if self.mydoc['mmd:mmd']['mmd:project']['mmd:long_name']:
                mydict['mmd_project_long_name'] = self.mydoc['mmd:mmd']['mmd:project']['mmd:long_name'].encode('utf-8')

        """ Access constraints """
        if 'mmd:access_constraint' in self.mydoc['mmd:mmd']:
            mydict['mmd_access_constraint'] = str(self.mydoc['mmd:mmd']['mmd:access_constraint'])

        """ Use constraint """
        if 'mmd:use_constraint' in self.mydoc['mmd:mmd']:
            mydict['mmd_use_constraint'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint'])

        """ Data center """
        if 'mmd:data_center' in self.mydoc['mmd:mmd']:
            if 'mmd:long_name' in self.mydoc['mmd:mmd']['mmd:data_center']['mmd:data_center_name']:
                mydict['mmd_data_center_data_center_name_long_name'] = str(self.mydoc['mmd:mmd']['mmd:data_center']['mmd:data_center_name']['mmd:long_name'])
            if 'mmd:short_name' in self.mydoc['mmd:mmd']['mmd:data_center']['mmd:data_center_name']:
                mydict['mmd_data_center_data_center_name_short_name'] = str(self.mydoc['mmd:mmd']['mmd:data_center']['mmd:data_center_name']['mmd:short_name'])
            if 'mmd:data_center_url' in self.mydoc['mmd:mmd']['mmd:data_center']:
                mydict['mmd_data_center_data_center_url'] = str(self.mydoc['mmd:mmd']['mmd:data_center']['mmd:data_center_url'])
            if 'mmd:contact' in self.mydoc['mmd:mmd']['mmd:data_center']:
                mydict['mmd_data_center_contact_name'] = str(self.mydoc['mmd:mmd']['mmd:data_center']['mmd:contact']['mmd:name'])
                mydict['mmd_data_center_contact_role'] = str(self.mydoc['mmd:mmd']['mmd:data_center']['mmd:contact']['mmd:role'])
                mydict['mmd_data_center_contact_email'] = str(self.mydoc['mmd:mmd']['mmd:data_center']['mmd:contact']['mmd:email'])

        """ Personnel """
        """ Need to check this again, should restructure cores ØG """
        mydict['mmd_personnel_name'] = []
        mydict['mmd_personnel_email'] = []
        mydict['mmd_personnel_organisation'] = []
        mydict['mmd_personnel_role'] = []
        if 'mmd:personnel' in self.mydoc['mmd:mmd']:
            if isinstance(self.mydoc['mmd:mmd']['mmd:personnel'], list):
                for e in self.mydoc['mmd:mmd']['mmd:personnel']:
                    print(e)
                    mydict['mmd_personnel_name'].append(e['mmd:name'])
                    mydict['mmd_personnel_role'].append(e['mmd:role'])
                    mydict['mmd_personnel_organisation'].append(e['mmd:organisation'])
                    mydict['mmd_personnel_email'].append(e['mmd:email'])
            else:
                e = self.mydoc['mmd:mmd']['mmd:personnel']
                mydict['mmd_personnel_name'].append(e['mmd:name'])
                mydict['mmd_personnel_role'].append(e['mmd:role'])
                mydict['mmd_personnel_organisation'].append(e['mmd:organisation'])
                mydict['mmd_personnel_email'].append(e['mmd:email'])

        """ Activity type """
        if 'mmd:activity_type' in self.mydoc['mmd:mmd']:
            mydict['mmd_activity_type'] = str(self.mydoc['mmd:mmd']['mmd:activity_type'])

        return mydict


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
            self.solr1 = pysolr.Solr(mysolrserver)
        except Exception as e:
            print("Something failed in SolR init", str(e))
        print("Connection established to: " + str(mysolrserver))

        # Connect to L2
        mysolrserver2 = mysolrserver.replace('-l1', '-l2')
        try:
            self.solr2 = pysolr.Solr(mysolrserver2)
        except Exception as e:
            print("Something failed in SolR init", str(e))
        print("Connection established to: " + str(mysolrserver2))

        # Connect to thumbnail
        mysolrservert = mysolrserver.replace('-l1', '-thumbnail')
        try:
            self.solrt = pysolr.Solr(mysolrservert)
        except Exception as e:
            print("Something failed in SolR init", str(e))
        print("Connection established to: " + str(mysolrservert))

    def add_level1(self, myrecord, addThumbnail=False, addFeature=False):
        """ Add a level 1 dataset """
        print("Adding records to Level 1 core...")
        mylist = list()
        # print(myrecord)
        # print(json.dumps(myrecord, indent=4))
        mylist.append(myrecord)
        # print(mylist)
        try:
            self.solr1.add(mylist)
        except Exception as e:
            print("Something failed in SolR add", str(e))
        print("Level 1 record successfully added.")

        # print(mylist[0]['mmd_data_access_resource'])
        if addThumbnail:
            print("Checking tumbnails...")
            darlist = self.darextract(mylist[0]['mmd_data_access_resource'])
            print(darlist)
            print(type(darlist))
            try:
                if 'OGC WMS' in darlist:
                    getCapUrl = darlist['OGC WMS']
                    wms_layer = 'ice_concentration'  # NOTE: need to parse/read the  mmd_data_access_wms_layers_wms_layer
                    myprojection = ccrs.Stereographic(central_longitude=0.0,
                            central_latitude=90., true_scale_latitude=60.)
                    #myprojection = ccrs.NorthPolarStereo(central_longitude=0.0)
                    myprojection = ccrs.Mercator()
                    #myprojection = ccrs.PlateCarree()
                    self.add_thumbnail(url=darlist['OGC WMS'],
                                       layer=wms_layer, projection=myprojection)
                elif 'OPeNDAP' in darlist:
                    # To be added
                    print('')
            except Exception as e:
                print("Something failed in adding thumbnail, " + str(e))
                raise Warning("Couldn't add thumbnail.")
        elif addFeature:
            try:
                self.set_feature_type(mylist)
            except Exception as e:
                print("Something failed in adding feature type, " + str(e))

    def add_level2(self, myl2record, addThumbnail=False, addFeature=False):
        """ Add a level 2 dataset, i.e. update level 1 as well """
        mylist2 = list()
        mylist2.append(myl2record)

        """ Retrieve level 1 record """
        #print(self.solr1.url)
        #print(self.solr2.url)
        #print(self.solrt.url)
        #print('>>> ',myl2record['mmd_metadata_identifier'])
        #print('>>> ','parent: '+myl2record['mmd_related_dataset'])
        try:
            # TODO: remove unused variable
            myresults = self.solr1.search('mmd_metadata_identifier:' +
                    myl2record['mmd_related_dataset'], df='', rows=100)
        except Exception as e:
            Warning("Something failed in searching for parent dataset, " + str(e))

        #print("Saw {0} result(s).".format(len(myresults)))
        # Clean the loop below, shouldn't be necessary as only one parent
        # should be present... TODO
        if len(myresults) != 1:
            raise Warning("Didn't find unique parent record")
        for result in myresults:
            result.pop('full_text')
            myresults = result
        # Check that the parent found has mmd_related_dataset set and
        # update this
        if 'mmd_related_dataset' in dict(myresults):
            myresults['mmd_related_dataset'].append(myl2record['mmd_metadata_identifier'])
        else:
            print('mmd_related_dataset not found in parent, creating it...')
            myresults['mmd_related_dataset'] = []
            print('Adding ', myl2record['mmd_metadata_identifier'],' to ',myl2record['mmd_related_dataset'])
            myresults['mmd_related_dataset'].append(myl2record['mmd_metadata_identifier'])
        mylist1 = list()
        mylist1.append(myresults)

        """ Index level 2 dataset """
        try:
            self.solr2.add(mylist2)
        except Exception as e:
            raise Exception("Something failed in SolR add level 2", str(e))
        print("Level 2 record successfully added.")

        """ Update level 1 record with id of this dataset """
        try:
            self.solr1.add(mylist1)
        except Exception as e:
            raise Exception("Something failed in SolR update level 1 for level 2", str(e))
        print("Level 1 record successfully updated.")

    def add_thumbnail(self, url, layer, zoom_level=0, projection=ccrs.PlateCarree(), type='wms'):
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
        if type == 'wms':
            thumbnail = self.create_wms_thumbnail(url, layer, zoom_level, projection)
        elif type == 'ts':
            thumbnail = 'TMP'  # create_ts_thumbnail(...)
        else:
            print('Invalid thumbnail type: {}').format(type)
            sys.exit(2)
        #print(thumbnail)
        # return thumbnail

        # Prepare input to SolR
        mylist = []
        myrecord['mmd_metadata_identifier'] = ''
        myrecord['thumbnail_data'] = thumbnail

    def create_wms_thumbnail(self, url, layer, zoom_level=0, projection=ccrs.PlateCarree()):
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

        wms = WebMapService(url)
        available_layers = list(wms.contents)
        if layer not in available_layers:
            layer = available_layers[0]
        wms_extent = wms.contents[available_layers[0]].boundingBoxWGS84
        # TODO: remove unused for variable?
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
        ax.set_extent(cartopy_extent_zoomed)

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
                wms_kwargs={'transparent': True,
                    'color':'boxfill/redblue'})
        ax.coastlines(linewidth=0.5)

        fig.savefig('thumbnail.png', format='png', bbox_inches='tight')
        #fig.savefig('thumbnail.png', format='png')
        plt.close('all')
        sys.exit() # while testing

        with open('thumbnail.png', 'rb') as infile:
            data = infile.read()
            encode_string = base64.b64encode(data)

        thumbnail_b64 = b'data:image/png;base64,' + encode_string

        os.remove('thumbnail.png')

        return thumbnail_b64
        # with open('image_b64.txt','wb') as outimg:
        #    outimg.write(b'data:image/png;base64,'+encode_string)

    def create_ts_thumbnail(self):
        """ Create a base64 encoded thumbnail """

    def set_feature_type(self, mymd):
        """ Set feature type from OPeNDAP """
        print("Now in set_feature_type")
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
            print("Something failed reading dataset", str(e))

        # Try to get the global attribute featureType
        try:
            featureType = ds.getncattr('featureType')
        except Exception as e:
            print("Something failed reading dataset", str(e))
            raise Warning('Could not find featureType')
        ds.close()

        mydict = OrderedDict({
            "id": mymd[0]['mmd_metadata_identifier'],
            "mmd_metadata_identifier": mymd[0]['mmd_metadata_identifier'],
            "feature_type": featureType,
        })
        mylist = list()
        mylist.append(mydict)

        try:
            self.solrt.add(mylist)
        except Exception as e:
            print("Something failed in SolR add", str(e))
            raise Warning("Something failed in SolR add" + str(e))

        print("Successfully added feature type for OPeNDAP.")

    def delete(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        print("Deleting ", datasetid)
        try:
            self.solr.delete(id=datasetid)
        except Exception as e:
            print("Something failed in SolR delete", str(e))

        print("Records successfully deleted")

    def search(self):
        """ Require Id as input """
        try:
            results = solr.search('mmd_title:Sea Ice Extent', df='text_en', rows=100)
        except Exception as e:
            print("Something failed: ", str(e))

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
    mylog = "mylogfile.txt"
    try:
        f = open(mylog, "w")
    except OSError as e:
        print(e)

    cflg = iflg = dflg = tflg = fflg = lflg = l2flg = rflg = False
    try:
        opts, args = getopt.getopt(argv, "hi:d:c:l:r:tf2", ["ifile=", "ddir=", "core=", "list=", "remove="])
    except getopt.GetoptError:
        print(sys.argv[0] + ' -i <inputfile>')
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
        elif opt in ("-r"):
            deleteid = arg
            rflg = True

    if not cflg or (not iflg and not dflg and not lflg and not rflg):
        usage()

    if l2flg:
        myLevel = "l2"
    else:
        myLevel = "l1"

    SolrServer = 'http://yourserver/solr/'
    SolrServer = 'http://157.249.176.182:8080/solr/'

    # Must be fixed when supporting multiple levels
    # Not needed with the new init of pySolr. ØG
    #if l2flg:
    #    mySolRc = SolrServer + myCore + "-l2"
    #else:
    #    mySolRc = SolrServer + myCore + "-l1"
    #mySolRtn = SolrServer + myCore + "-thumbnail"
    mySolRc = SolrServer+myCore+'-l1'

    # Find files to process
    if iflg:
        myfiles = [infile]
    elif lflg:
        f2 = open(infile, "r")
        myfiles = f2.readlines()
        f2.close()
    elif rflg:
        print("Deleting dataset " + deleteid + " from " + mySolRc)
        mysolr = IndexMMD(mySolRc)
        mysolr.delete([deleteid])
        sys.exit()
    elif dflg:
        try:
            myfiles = os.listdir(ddir)
        except Exception as e:
            print("Something went wrong in decoding cmd arguments: " + str(e))
            sys.exit(1)

    # mysolrlist = list() # might be used later...
    if dflg and l2flg:
        print('Commented out')
        """
        # Until the indexing utility actually works as expected...
        print("Indexing a Level 2 directory in "+mySolRc)
        f.write("Indexing a Level 2 directory in "+mySolRc)
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
            print("Indexing a single thumbnail in "+mySolRtn)
            myproc = subprocess.check_output(['/usr/bin/java',
                '-jar','metsis-metadata-jar-with-dependencies.jar',
                'index-thumbnail',
                '--sourceDirectory', ddir, '--server', mySolRtn,
                '--wmsVersion', '1.3.0'])
            print("Return value: " + str(myproc))
            f.write(myproc)
        if fflg:
            print("Indexing a single feature type in "+mySolRtn)
            myproc = subprocess.check_output(['/usr/bin/java',
                '-jar','metsis-metadata-jar-with-dependencies.jar',
                'index-feature',
                '--sourceDirectory', ddir, '--server', mySolRtn])
            print("Return value: " + str(myproc))
            f.write(myproc)
        """
    else:
        for myfile in myfiles:
            # Decide files to operate on
            if lflg:
                myfile = myfile.rstrip()
            if dflg:
                myfile = os.path.join(ddir, myfile)

            # Index files
            mydoc = MMD4SolR(myfile)  # while testing
            mydoc.check_mmd()
            # print(mydoc.tosolr())
            mysolr = IndexMMD(mySolRc)
            if iflg or lflg:
                print("Indexing dataset " + myfile)
                if l2flg:
                    mysolr.add_level2(mydoc.tosolr(), tflg, fflg)
                else:
                    mysolr.add_level1(mydoc.tosolr(), tflg, fflg)

            sys.exit()  # while testing

            print("Indexing a single file in " + mySolRc)
            f.write("\n======\nIndexing " + myfile)
            if not os.path.isfile(myfile):
                print(myfile + " does not exist")
                sys.exit(1)
            myproc = subprocess.check_output(['/usr/bin/java',
                                              '-jar',
                                              'metsis-metadata-jar-with-dependencies.jar',
                                              'index-single-metadata',
                                              '--level',
                                              myLevel,
                                              '--metadataFile',
                                              myfile,
                                              '--server',
                                              mySolRc])
            f.write(myproc)
            # print("Return value: " + str(myproc))
            if tflg:
                print("Indexing a single thumbnail in " + mySolRtn)
                myproc = subprocess.check_output(['/usr/bin/java',
                                                  '-jar',
                                                  'metsis-metadata-jar-with-dependencies.jar',
                                                  'index-single-thumbnail',
                                                  '--metadataFile',
                                                  myfile,
                                                  '--server',
                                                  mySolRtn,
                                                  '--wmsVersion',
                                                  '1.3.0'])
                # print("Thumbnail indexing: " + mySolRtn)
                # print("Return value: " + str(myproc))
                f.write(myproc)
            if fflg:
                print("Indexing a single feature type in " + mySolRtn)
                myproc = subprocess.check_output(['/usr/bin/java',
                                                  '-jar',
                                                  'metsis-metadata-jar-with-dependencies.jar',
                                                  'index-single-feature',
                                                  '--metadataFile',
                                                  myfile,
                                                  '--server',
                                                  mySolRtn])
                # print("Return value: " + str(myproc))
                f.write(myproc)
            f.write(myproc)

    # Report status
    f.write("Number of files processed were:" + str(len(myfiles)))
    f.close()


if __name__ == "__main__":
    main(sys.argv[1:])
