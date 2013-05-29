nanoDB
======

Tools for the NANOGrav database

nanoUpload.py
---------------
A script for uploading raw data files to the NANOGrav FTP server
hosted at Cornell and the data archive hosted at UBC.  The following
python packages are are also required:

- sys, os (all available from http://pypi.python.org)
- pyfits (http://www.stsci.edu/institute/software_hardware/pyfits)
- pyslalib (https://github.com/scottransom/pyslalib)
- nanoCredntials, nanoDBTools (included in this repo)

nanoCredentials.py
------------------
A file for storing Cornell FTP and UBC ssh credentials.  Contact Adam
Brazier (abrazier@astro.cornell.edu) or Ingrid Stairs
(stairs@astro.ubc.ca) to obtain the relevant information.

nanoDBTools.py
--------------
A module containi utilities classes and functions for connecting to
the Cornell and UBC servers.
