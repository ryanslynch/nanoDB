nanoDB
======

Tools for the NANOGrav database

nanoUploader.py
---------------
A script for uploading raw data files to the NANOGrav FTP server
hosted at Cornell.  The following python packages are are also
required:

- M2Crypto, paramiko, sys, os (all available from
  http://pypi.python.org)
- pyfits (http://www.stsci.edu/institute/software_hardware/pyfits)
- pyslalib (https://github.com/scottransom/pyslalib)
- nanoCredntials (included in this repo)

nanoCredentials.py
------------------
A file for storing Cornell FTP and UBC ssh credentials.  Contact Adam
Brazier (abrazier@astro.cornell.edu) or Ingrid Stairs
(stairs@astro.ubc.ca) to obtain the relevant information.
