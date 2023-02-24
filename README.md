# solrindexing
Useful tools and wrappers used for indexing MMD in SolR. This software is
developed for use in the context of Arctic Data Centre, supported through
projects SIOS KC and Norwegian Scientific Data Network.

## Bulkindexing with multiprocessing and multithreading
- The number of files given as input is devided between x worker processes given in config
- Each worker process, will futher split its incoming files into batches given in config 
- Each worker process read the files using multithreading and each solr add command are spawned as a new thread so we can continue processing while solr process the input documents
* Each process will use multithrading for I/O operations, like reading MMD-files and extracting attributes via OPeNDAP

## TODO
 * Refactor the bulkindexing code.
 * Add gml parsing of spatial bounds if found in MMD
 * Clean up code and geometry stuff
 * Remove unused imports and update requirements.yml

## Improvements
* Would lxml parsing benefit us compared to xmltodict? lxml are probably faster, and the code will maybe look more clean if implemented properly.

* Make thumbnail extraction as a separate module, to be able to create thumbnails without indexing. I.E store the generated thumbnails on disk.
