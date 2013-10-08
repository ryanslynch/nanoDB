#! /usr/bin/python
from __future__ import print_function

"""
Upload fold-mode pulsar data taken with GUPPI, PUPPI, and/or ASP to
the NANOGrav FTP server hosted at Cornell University and the NANOGrav
data archive hosted at UBC.  If possible, create an ephemeris
(\"par\" file) and upload it, too (only available for PSRFITS GUPPI
and PUPPI data).

Author       : Ryan S. Lynch
Contact      : ryan.lynch@nanograv.org
Last Updated : 31 Jan. 2013
"""

import pyfits as PF
import sys, os, nanoDBTools
from pyslalib.slalib import sla_djcl

SECPERDAY = 86400.0

usage="""Usage: nanoUpload.py infiles
Arguments:
  infiles        A list of fold-mode pulsar files taken with GUPPI,
                 PUPPI, and/or ASP.  Typical wildcards are accepted.
Options:
  -h, --help     Print this help page

Author: Ryan S. Lynch (ryan.lynch@nanograv.org)
"""



def parse_arguments(args):
    """
    Parse the command line arguments and print help and usage
    information if necessary.

    Parameters
    ----------
    args : list
      The list of command line arguments.

    Returns
    -------
    args : list
      The list of command line arguments.  If the help flag or an
      unrecognized option is encountered, this will be empty.
    """
    
    if len(args) == 0 or "-h" in args or "--help" in args:
        print(usage)
        return []

    elif any([arg.startswith("-") for arg in args]):
        print("ERROR: Unrecognized option")
        print(usage)
        return []

    else:
        return args



def determine_filetype(filenm):
    """
    Determine the type of the given file.

    Parameters
    ----------
    filenm : str
      The file name whose type to determine.

    Returns
    filetype : str
      The file type.  May be \"META\", \"FITS\", or \"UNKNOWN\".
    """

    # Try opening the file with pyfits. If that works, assume a fits
    # file.
    try:
        f = PF.open(filenm)

    except IOError:
        # If pyfits.open didn't work, look for a line starting with
        # ProfileName and if found, assume a metadata file
        f = open(filenm, "r")

        if any(["ProfileName" in line for line in f.readlines()]):
            filetype = "META"

        else:
            filetype = "UNKNOWN"

    else:
        filetype = "FITS"

    finally:
      f.close()

      return filetype



def parse_archive(filenm):
    """
    Parse an input fits file header and create a remote path.

    Parameters
    ----------
    filenm : str
        The name of the input archive.

    Returns
    -------
    cornell_path : str
        The path to send to the Cornell FTP server.
    ubc_path : str
        The path to send to the UBC data archive.
    """

    # Open the file using pyfits and get the header
    hdulist = PF.open(filenm)
    hdr     = hdulist[0].header
    hdulist.close()
    source  = hdr["SRC_NAME"]
    backend = hdr["BACKEND"]

    if backend == "GUPPI2":
        # This is for uniformity between incoherent/coherent GUPPI modes
        backend = "GUPPI" 
        year = hdr["DATE-OBS"].split("-")[0]

    elif backend == "PUPPI":
        year = hdr["DATE-OBS"].split("-")[0]
    
    elif backend == "xASP":
        backend = "ASP"
        MJD     = hdr["STT_IMJD"] + hdr["STT_SMJD"]/SECPERDAY
        year    = str(sla_djcl(MJD)[0]) # Convert MJD to calendar date

    else:
        backend = None
        year    = None

    return source,backend,year



if __name__ == "__main__":
    # Parse the command line arguments
    args      = sys.argv[1:]
    infilenms = parse_arguments(args)
    if len(infilenms) == 0: sys.exit()

    # uploads will hold a tuple of the file names to upload and 
    # the upload paths for the remote sites
    uploads = [] 
    for infilenm in infilenms:
        # Determine input file types
        filetype = determine_filetype(infilenm)

        if filetype == "META":
            for entry in nanoDBTools.parse_metafile(infilenm):
                if entry["type"] == "archive":
                    source,backend,year = parse_archive(entry["ProfileName"])
                    
                    if backend is not None:
                        cpath   = os.path.join("NANOGrav", source, backend,
                                                year, "processed")
                        ubcpath = os.path.join("/", "dstore", "data",
                                               source.strip("B").strip("J"),
                                               backend.lower())
                        
                        uploads.append((entry["ProfileName"], cpath,
                                         ubcpath))
                    else:
                        print("WARNING: Skipping %s because it is is "\
                              "not from a recognized "\
                              "backend"%entry["ProfileName"])

            # Include the metadata file itself
            cpath   = "NANOGrav/loadingfiles"
            ubcpath = "/dstore/data/loadingfiles"
            uploads.append((infilenm, cpath, ubcpath))
            
        # Assume any fits files supplied via the command line are raw
        # data
        elif filetype == "FITS":
            source,backend,year = parse_archive(infilenm)

            if backend is not None:
                cpath   = os.path.join("NANOGrav", source, backend, year,
                                       "rawdata")
                ubcpath = os.path.join("/", "dstore", "data",
                                       source.strip("B").strip("J"),
                                       backend.lower())
              
                uploads.append((infilenm, cpath, ubcpath))

            else:
                print("WARNING: Skipping %s because it is is not from a "\
                      "recognized backend"%infilenm)

        elif filetype == "UNKNOWN":
            print("WARNING: Skipping %s because it is of unrecognized "\
                  "file type"%infilenm)

    
    cftp    = nanoDBTools.CornellFTP()
    ubcsftp = nanoDBTools.UBCSFTP()
    # We first upload everything to Cornell.  This duplicates some of
    # code but it prevents the connection from timing out due to
    # innactivity while large files are being uploaded to UBC.

    count = 0
    for upload in uploads:
        # Print a status message
        count += 1
        print("\rUploading %i of %i to Cornell FTP"%(count, len(uploads)),
              end="")
        sys.stdout.flush()
        
        try:
            cftp.upload(upload[0], upload[1])
            
        except Exception as e:
            print("\nWARNING: Failed to upload %s to Cornell FTP" % \
                  upload[0])
            print(e)

    # This just prints a new line
    print("")
    
    # Now upload to UBC
    count = 0
    for upload in uploads:
        count += 1
        print("\rUploading %i of %i to UBC archive"%(count, len(uploads)),
              end="")
        sys.stdout.flush()
        
        try:
          ubcsftp.upload(upload[0], upload[2])
          
        except Exception as e:
            print("\nWARNING: Failed to upload %s to UBC archive" % \
                  upload[0])
            print(e)

    print("")

    cftp.close()
    ubcsftp.close()
