#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    Reading title and the time of the last metadata update of the metadata
    file, this software generates a UUID for the dataset.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2016-06-08
"""

import sys
import os
import getopt
import uuid
import datetime
import xml.etree.ElementTree as ET

def usage():
  print 'Usage: '+sys.argv[0]+' -i <mdfile>'  
  sys.exit(2)


def main(argv):
   infile = None
   try:
       opts, args = getopt.getopt(argv,"hi:",["ifile="])
   except getopt.GetoptError:
      print str(err) 
      usage()
   for opt, arg in opts:
      if opt == '-h':
         print sys.argv[0]+' -n <dataset_name>'
         sys.exit()
      elif opt in ("-i", "--ifile"):
         infile = arg
      else:
          assert False, 'Unhandled option'

   if infile is None: 
       usage()


   # Parse the XML file
   tree = ET.parse(infile)
   #root = tree.getroot()
   try:
       mytitle = tree.find('{http://www.met.no/schema/mmd}title').text
   except:
       print "title is missing from metadata file"
       sys.exit(2)
   try:
       mylastupdate = tree.find('{http://www.met.no/schema/mmd}last_metadata_update').text
   except:
       print "last_metadata_update is missing from metadata file"
       sys.exit(2)
   #print mytitle
   #print mylastupdate

   # Get the time of creation for the metadatafile
   #dstime = os.path.getctime(infile)
   filename = "https://arcticdata.met.no/ds/"+os.path.basename(infile)+"-"
   #filename += datetime.datetime.utcfromtimestamp(dstime).strftime("%Y%m%dT%H%M%S")
   filename += mylastupdate

   print uuid.uuid5(uuid.NAMESPACE_URL,filename)


if __name__ == "__main__":
   main(sys.argv[1:])
