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
#from checkMMD_v5 import CheckMMD
import warnings
import json
from collections import OrderedDict

#Packages for generating thumbnails
import cartopy.crs as ccrs
import cartopy
import matplotlib.pyplot as plt
from owslib.wms import WebMapService
import base64

def usage():
    print('')
    print('Usage: '+sys.argv[0]+' -i <dataset_name> -c <core_name> [-h]')
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

class MMD4SolR():
    """ Read and check MMD files, convert to dictionary """
    def __init__(self, filename):
        """ set variables in class """
        self.filename = filename
        with open(self.filename) as fd:
            self.mydoc = xmltodict.parse(fd.read())

    def check_mmd(self):
        """ Check and correct MMD if needed """
        """ Remember to check that multiple fields of abstract and title
        have set xml:lang= attributes... """

        """
        Check for presence of required elements
        Temporal and spatial extent are not required as of now.
        """
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
                    print('\t'+requirement+' is present and non empty')
                    mmd_requirements[requirement] = True


        """
        Check for correct vocabularies where necessary
        Change to external files (SKOS), using embedded files for now
        """
        mmd_controlled_elements = {
                'mmd:iso_topic_category':
                    ['farming','biota','boundaries',
                    'climatologyMeteorologyAtmosphere', 'economy','elevation',
                    'environment','geoscientificinformation','health',
                    'imageryBaseMapsEarthCover','inlandWaters','location',
                    'oceans','planningCadastre','society','structure',
                    'transportation','utilitiesCommunication'],
                'mmd:collection':
                    ['ACCESS','ADC','APPL','CC','DAM','DOKI','GCW',
                    'NBS','NMAP','NMDC','NSDN','SIOS','YOPP'],
                'mmd:dataset_production_status':
                    ['Planned', 'In Work', 'Complete', 'Obsolete'],
                }
        for element in mmd_controlled_elements.keys():
            print('Checking '+element)
            if element in self.mydoc['mmd:mmd']:
                if type(self.mydoc['mmd:mmd'][element]) is list:
                    if all(elem in mmd_controlled_elements[element] for elem in self.mydoc['mmd:mmd'][element]):
                            print('\t'+element+' is all good...')
                    else:
                        print('\t'+element+' contains non valid content')
                        print('('+self.mydoc['mmd:mmd'][element]+')')
                else:
                    if self.mydoc['mmd:mmd'][element] in mmd_controlled_elements[element]:
                            print('\t'+element+' is all good...')
                    else:
                        print('\t'+element+' contains non valid content')
                        print('('+self.mydoc['mmd:mmd'][element]+')')



        """
        Check that keywords also contain GCMD keywords
        Need to check contents more specifically...
        """
        if isinstance(self.mydoc['mmd:mmd']['mmd:keywords'],list):
            i = 0
            gcmd = False
            for e in self.mydoc['mmd:mmd']['mmd:keywords']:
                if str(self.mydoc['mmd:mmd']['mmd:keywords'][i]).upper() == 'GCMD':
                    gcmd = True
                    break;
                i += 1
            if not gcmd:
                print('Keywords in GCMD are not available')
        else:
            if not str(self.mydoc['mmd:mmd']['mmd:keywords']['@vocabulary']).upper() == 'GCMD':
                #warnings.warn('Keywords in GCMD are not available')
                print('Keywords in GCMD are not available')

        """ Modify dates if necessary """
        if 'mmd:temporal_extent' in self.mydoc['mmd:mmd']:
            for mykey in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                mydate = dateutil.parser.parse(str(self.mydoc['mmd:mmd']['mmd:temporal_extent'][mykey]))
                self.mydoc['mmd:mmd']['mmd:temporal_extent'][mykey] = mydate.strftime('%Y-%m-%dT%H:%M:%SZ')


    def tosolr(self):
        """ Collect required elements """
        mydict = OrderedDict({
                "id":
                    str(self.mydoc['mmd:mmd']['mmd:metadata_identifier']),
                "mmd_metadata_identifier":
                    str(self.mydoc['mmd:mmd']['mmd:metadata_identifier']),
                "mmd_metadata_status":
                    str(self.mydoc['mmd:mmd']['mmd:metadata_status']),
                })

        """ title """
        if isinstance(self.mydoc['mmd:mmd']['mmd:title'], list):
            i=0
            for e in self.mydoc['mmd:mmd']['mmd:title']:
                if self.mydoc['mmd:mmd']['mmd:title'][i]['@xml:lang'] == 'en':
                    mydict['mmd_title'] = self.mydoc['mmd:mmd']['mmd:title'][i]['#text'].encode('utf-8')
                i+=1
        else:
            mydict['mmd_title'] = str(self.mydoc['mmd:mmd']['mmd:title']['#text'])

        """ abstract """
        if isinstance(self.mydoc['mmd:mmd']['mmd:abstract'],list):
            i=0
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
            #if len(self.mydoc['mmd:mmd']['mmd:collection']) > 1: #Does not work on single collection
            if isinstance(self.mydoc['mmd:mmd']['mmd:collection'],list): #Does not work on single collection
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
            for e in (self.mydoc['mmd:mmd']['mmd:keywords']['mmd:keyword']):
                mydict['mmd_keywords_keyword'].append(e)

        """ Temporal extent """
        if 'mmd:temporal_extent' in self.mydoc['mmd:mmd']:
            mydict["mmd_temporal_extent_start_date"] = str(self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:start_date']),
            if 'mmd:end_date' in self.mydoc['mmd:mmd']['mmd:temporal_extent']:
                mydict["mmd_temporal_extent_end_date"] = str(self.mydoc['mmd:mmd']['mmd:temporal_extent']['mmd:end_date']),

        """ Geographical extent """
        if 'mmd:geographic_extent' in self.mydoc['mmd:mmd']:
                mydict['mmd_geographic_extent_rectangle_north'] = float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:north']),
                mydict['mmd_geographic_extent_rectangle_south'] = float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:south']),

                mydict['mmd_geographic_extent_rectangle_east'] = float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east']),
                mydict['mmd_geographic_extent_rectangle_west'] = float(self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west']),

        """ Data access """
        """ Double check this ØG """
        """ Especially description """
        if 'mmd:data_access' in self.mydoc['mmd:mmd']:
            mydict['mmd_data_access_resource'] = []
            if isinstance(self.mydoc['mmd:mmd']['mmd:data_access'],list):
                i = 0
                for e in self.mydoc['mmd:mmd']['mmd:data_access']:
                    mydict['mmd_data_access_resource'].append(
                            '\"'.encode('utf-8')+self.mydoc['mmd:mmd']['mmd:data_access'][i]['mmd:type'].encode('utf-8')+'\":\"'.encode('utf-8')+self.mydoc['mmd:mmd']['mmd:data_access'][i]['mmd:resource'].encode('utf-8')+'\",description\":'.encode('utf-8')
                            )
                    i += 1
            else:
                mydict['mmd_data_access_resource'] = [
                        '\"'+self.mydoc['mmd:mmd']['mmd:data_access']['mmd:type']+'\":\"'+self.mydoc['mmd:mmd']['mmd:data_access']['mmd:resource']+'\"'
                        ]

        """ Related information """
        """ Must be updated to hold mutiple ØG """
        mydict['mmd_related_information_resource'] =  []
        if 'mmd:related_information' in self.mydoc['mmd:mmd']:
            for related_information in self.mydoc['mmd:mmd']['mmd:related_information']:
                mydict['mmd_related_information_resource'].append(
                        #'\"'+self.mydoc['mmd:mmd']['mmd:related_information']['mmd:type'].encode('utf-8')+'\":\"'+self.mydoc['mmd:mmd']['mmd:related_information']['mmd:resource'].encode('utf-8')+'\",\"description\":'#+self.mydoc['mmd:mmd']['mmd:related_information']['mmd:description'].encode('utf-8')
                        '\"'.encode('utf-8')+related_information['mmd:type'].encode('utf-8')+'\":\"'.encode('utf-8')+related_information['mmd:resource'].encode('utf-8')+'\",\"description\":'.encode('utf-8')

                        )

        """ Related dataset """
        """ Remember to add type of relation in the future ØG """
        if 'mmd:related_dataset' in self.mydoc['mmd:mmd']:
            mydict['mmd_related_dataset'] = str(self.mydoc['mmd:mmd']['mmd:related_dataset'])

        """ Project """
        if 'mmd:project' in self.mydoc['mmd:mmd']:
            #mydict['mmd_project_short_name'].append(
            #        self.mydoc['mmd:mmd']['mmd:project']['mmd:short_name'].encode('utf-8'))
            #mydict['mmd_project_long_name'].append(
            #        self.mydoc['mmd:mmd']['mmd:project']['mmd:long_name'].encode('utf-8'))
            mydict['mmd_project_short_name'] = self.mydoc['mmd:mmd']['mmd:project']['mmd:short_name'].encode('utf-8')
            mydict['mmd_project_long_name'] = self.mydoc['mmd:mmd']['mmd:project']['mmd:long_name'].encode('utf-8')

        """ Access constraints """
        if 'mmd:access_constraint' in self.mydoc['mmd:mmd']:
            mydict['mmd_access_constraint'] = str(self.mydoc['mmd:mmd']['mmd:access_constraint'])

        """ Use constraint """
        if 'mmd:use_constraint' in self.mydoc['mmd:mmd']:
            mydict['mmd_use_constraint'] = str(self.mydoc['mmd:mmd']['mmd:use_constraint'])

        """ Data center """
        """ This may be missing curently, not easy to find out """
        if 'mmd:data_center' in self.mydoc['mmd:mmd']:
            if 'mmd:long_name' in self.mydoc['mmd:mmd']['mmd:data_center']:
                mydict['mmd_data_center'] = str(self.mydoc['mmd:mmd']['mmd:data_center']['mmd:long_name'])

        """ Personnel """
        """ Need to check this again, should restructure cores ØG """
        #if
        #    mydict['mmd_personnel_name'] =
        #    mydict['mmd_personnel_email'] =
        #    mydict['mmd_personnel_organisation'] =
        #    mydict['mmd_personnel_role'] =

        """ Activity type """
        if 'mmd:activity_type' in self.mydoc['mmd:mmd']:
            mydict['mmd_activity_type'] = str(self.mydoc['mmd:mmd']['mmd:activity_type'])

        return(mydict)

class IndexMMD():
    """ requires a list of dictionaries representing MMD as input """
    def __init__(self,mysolrserver):
        """ Is it just as wise to just attach all 3 cores used? ØG """
        """ Then we can deceide on where to put records afterwards """
        #self.mmd4solr = list()
        #self.mmd4solr.append(mmd4solr)
        try:
            self.solr = pysolr.Solr(mysolrserver)
        except Exception as e:
            print("Something failed in SolR init", str(e))
        print("Connected to SolR server...")

    def add_level1(self,myrecord,add_thumbnail=True):
        """ Add a level 1 dataset """
        print("Adding records...")
        mylist = list()
        #print(myrecord)
        #print(json.dumps(myrecord, indent=4))
        mylist.append(myrecord)
        print(mylist)
        try:
            self.solr.add(mylist)
        except Exception as e:
            print("Something failed in SolR add", str(e))
        print("Level 1 record successfully added.")

        if add_thumbnail:
            try:
                #Traverse mmd_data_access_resource to find OGC WMS information
                for dar in mylist[0]['mmd_data_access_resource']:
                    elements = dar.split(b'\"')
                    if elements[1] == b'OGC WMS':
                        getCapUrl = elements[3]
                        wms_layer = 'amplitude_vv' #NOTE: need to parse/read the  mmd_data_access_wms_layers_wms_layer
                        zoom_level = 15
                        self.add_thumbnail(url=getCapUrl, layer=wms_layer, zoom_level=zoom_level)


            except Exception as e:
                print("Something failed in adding thumbnail, " + str(e))

    def add_level2(self,myl2record):
        """ Add a level 2 dataset, i.e. update level 1 as well """
        """ Retrieve level 1 record """
        try:
            myresults = "TEST"
            self.solr.search('mmd_metadata_identifier:'+myl2record['mmd_metadata_identifier'], df='', rows=100)
        except Exception as e:
            print("Something failed in searching for parent dataset, " + str(e))

        #""" Update level 1 record with id of this dataset """

        #""" Index level 2 dataset """

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
            thumbnail = 'TMP'#create_ts_thumbnail(...)
        else:
            print('Invalid thumbnail type: {}').format(type)
            sys.exit(2)
        print(thumbnail)
        #return thumbnail


    def create_wms_thumbnail(self, url, layer, zoom_level=0 ,projection=ccrs.PlateCarree()):
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
        #if self.
        getCapabilities_url = url#self.mylist['mmd_data_access_resource']['OGC WMS']

        wms = WebMapService(getCapabilities_url.decode('utf-8'))
        wms_extent = wms.contents[layer].boundingBoxWGS84
        cartopy_extent = [wms_extent[0], wms_extent[2],wms_extent[1],wms_extent[3]]
        cartopy_extent_zoomed = [wms_extent[0]-zoom_level,
                                 wms_extent[2]+zoom_level,
                                 wms_extent[1]-zoom_level,
                                 wms_extent[3]+zoom_level]
        max_extent = [-180.0, 180.0, -90.0, 90.0]

        for i,extent in enumerate(cartopy_extent_zoomed):
            if i%2 == 0:
                if extent < max_extent[i]:
                    cartopy_extent_zoomed[i] = max_extent[i]
            else:
                if extent > max_extent[i]:
                    cartopy_extent_zoomed[i] = max_extent[i]

        subplot_kw = dict(projection=projection)
        fig, ax = plt.subplots(subplot_kw=subplot_kw)
        ax.set_extent(cartopy_extent_zoomed)

        land_mask = cartopy.feature.NaturalEarthFeature(category='physical',
                        scale='50m', facecolor='gray', name='land')
        ax.add_feature(land_mask, zorder=0, edgecolor='black')

        # transparent background
        ax.outline_patch.set_visible(False)
        ax.background_patch.set_visible(False)
        fig.patch.set_alpha(0)
        fig.set_alpha(0)
        ax.background_patch.set_alpha(1)

        ax.add_wms(wms, layer,wms_kwargs={'transparent':True})

        fig.savefig('thumbnail.png',format='png')
        plt.close('all')

        with open('thumbnail.png','rb') as infile:
            data=infile.read()
            encode_string = base64.b64encode(data)

        thumbnail_b64 = b'data:image/png;base64,'+encode_string

        os.remove('thumbnail.png')

        return thumbnail_b64
        #with open('image_b64.txt','wb') as outimg:
        #    outimg.write(b'data:image/png;base64,'+encode_string)

    def create_ts_thumbnail(self):
        """ Create a base64 encoded thumbnail """

    def set_feature_type(self):
        """ Set feature type from OPeNDAP """

    def delete(self):
        """ Require ID as input """
        try:
            self.solr.delete(id=['doc_1', 'doc_2'])
        except Exception as e:
            print("Something failed in SolR delete", str(e))

        print("Records successfully deleted")

    def search(self):
        """ Require Id as input """
        try:
            results = solr.search('mmd_title:Sea Ice Extent', df='text_en',rows=100)
        except Exception as e:
            print("Something failed: ", str(e))

        return(results)

def main(argv):

    mylog = "mylogfile.txt"
    try:
        f = open(mylog,"w")
    except OSError as e:
        print(e)

    cflg = iflg = dflg = tflg = fflg = lflg = l2flg = False
    try:
        opts, args = getopt.getopt(argv,"hi:d:c:l:tf2",["ifile=", "ddir=",
            "core=", "list="])
    except getopt.GetoptError:
        print(sys.argv[0]+' -i <inputfile>')
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

    SolrServer = 'http://yourserver/solr/'
    # Must be fixed when supporting multiple levels
    if l2flg:
        mySolRc = SolrServer + myCore + "-l2"
    else:
        mySolRc = SolrServer + myCore + "-l1"
    mySolRtn = SolrServer + myCore + "-thumbnail"

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
            print(os.error)
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
            if lflg:
                myfile = myfile.rstrip()
            if dflg:
                myfile = os.path.join(ddir,myfile)
            # Index files


            mydoc = MMD4SolR(myfile) # while testing
            mydoc.check_mmd()
            #print(mydoc.tosolr())
            mysolr = IndexMMD(mySolRc)
            mysolr.add_level1(mydoc.tosolr())
            mysolr.create_wms_thumbnail
            sys.exit() # while testing

            print("Indexing a single file in "+mySolRc)
            f.write("\n======\nIndexing "+ myfile)
            if not os.path.isfile(myfile):
                print(myfile+" does not exist")
                sys.exit(1)
            myproc = subprocess.check_output(['/usr/bin/java',
                '-jar','metsis-metadata-jar-with-dependencies.jar',
                'index-single-metadata',
                '--level', myLevel,
                '--metadataFile', myfile,
                '--server', mySolRc])
            f.write(myproc)
            #print("Return value: " + str(myproc))
            if tflg:
                print("Indexing a single thumbnail in "+mySolRtn)
                myproc = subprocess.check_output(['/usr/bin/java',
                    '-jar','metsis-metadata-jar-with-dependencies.jar',
                    'index-single-thumbnail',
                    '--metadataFile', myfile,
                    '--server', mySolRtn,
                    '--wmsVersion', '1.3.0'])
                #print("Thumbnail indexing: " + mySolRtn)
                #print("Return value: " + str(myproc))
                f.write(myproc)
            if fflg:
                print("Indexing a single feature type in "+mySolRtn)
                myproc = subprocess.check_output(['/usr/bin/java',
                    '-jar','metsis-metadata-jar-with-dependencies.jar',
                    'index-single-feature',
                    '--metadataFile', myfile,
                    '--server', mySolRtn])
                #print("Return value: " + str(myproc))
                f.write(myproc)
            f.write(myproc)

    # Report status
    f.write("Number of files processed were:" + str(len(myfiles)))
    f.close()


if __name__ == "__main__":
    main(sys.argv[1:])
