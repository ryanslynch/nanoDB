#! /usr/bin/python

debug = True

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


def write_ephemeris(filenm):
    """
    Read ephemeris information from a PSRFITS archive and write a tempo-style
    \"par\" file for it.  Parameters with zero or empty-string values are
    not written.

    Parameters
    ----------
    filenm : str
        The name of the input archive.

    Returns
    -------
    status : int
        Zero indicates success.  One indicates failure.
    outfilenm : str
        The name of the output par file.
    """

    # Open the file using pyfits and get the header
    hdulist = PF.open(filenm)
    hdr     = hdulist[0].header

    if hdr["BACKEND"] == "GUPPI" or hdr["BACKEND"] == "PUPPI":
        scidata   = hdulist[1].data # The first extension is the ephemeris
        keys      = scidata.dtype.names # These are the parameter names
        values    = scidata[0]
        outfilenm = infilenm.replace(".fits", ".par")
        outfile   = open(outfilenm, "w")

        for key,value in zip(keys, values):
            # Only write non-zero and non-empty strings
            if  value != 0 and value != "":
                outfile.write("%-10s%18s\n"%(key,value))
            else: pass
            
        outfile.close()
        print("Ephemeris written to %s"%outfilenm)
        status = 0

    else:
        # ASP files have no ephemeris table, so do nothing for them
        outfilenm = None
        print("WARNING: Ephemeris not written because BACKEND != G/PUPPI")
        status   = 1

    hdulist.close()

    return status, outfilenm



def get_remote_paths(filenm):
    """
    Parse an input fits file header and create an remote path.

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
    backend = hdr["BACKEND"]
    source  = hdr["SRC_NAME"]

    if backend == "GUPPI" or backend == "PUPPI":
        year = hdr["DATE-OBS"].split("-")[0]
        
    elif backend == "xASP":
        backend = "ASP"
        MJD     = hdr["STT_IMJD"] + hdr["STT_SMJD"]/SECPERDAY
        year    = str(sla_djcl(MJD)[0]) # Convert MJD to calendar date

    else:
        print("WARNING: Unrecognized backend for %s!"%infilenm)
        print("         Only GUPPI, PUPPI and ASP are supported")
        exit(1)

    cornell_path = os.path.join(source, backend, year, "rawdata")
    if backend.lower() == "guppi": backend = backend + "2"
    ubc_path     = os.path.join(source.strip("B").strip("J"), backend.lower())

    return cornell_path, ubc_path



if __name__ == "__main__":

    # Get the list of command line arguments
    arguments = sys.argv[1:]

    # Check of the -h or --help flags and for any unrecognized options
    if len(arguments) == 0 or "-h" in arguments or "--help" in arguments:
        print(usage)
        sys.exit(0)

    elif any([arg.startswith("-") for arg in arguments]):
        print("ERROR: Unrecognized option")
        print(usage)
        sys.exit(1)

    # If the arguments look correct, proceed to upload
    else:
        # Connect to the remote sites
        cftp    = nanoDBTools.CornellFTP()
        ubcsftp = nanoDBTools.UBCSFTP()
        
        for infilenm in arguments:
            #ephemfile_status, ephemfilenm = write_ephemeris(infilenm)
            ephemfile_status = 1
            cornell_path, ubc_path  = get_remote_paths(infilenm)

            # Upload to a Test directory if in debugging mode
            if debug:
                cornell_path = os.path.join("NANOGrav", "Test", cornell_path)
                ubc_path     = os.path.join("/dstore", "data", "Test",
                                            ubc_path)

            else:
                cornell_path = os.path.join("NANOGrav", cornell_path)
                ubc_path     = os.path.join("/dstore", "data", ubc_path)

            try:
              cftp.upload(infilenm, cornell_path)
            except Exception as e:
                print("ERROR: Failed to upload %s to Cornell FTP server" % \
                      infilenm)
                print(e)

            if ephemfile_status == 0:
                try:
                  cftp.upload(ephemfilenm, cornell_path)
                except Exception as e:
                    print("ERROR: Failed to upload %s to Cornell FTP "\
                          "server"%ephemfilenm)
                    print(e)
                
            try:
              ubcsftp.upload(infilenm, ubc_path)
            except Exception as e:
                print("ERROR: Failed to upload %s to UBC data archive" % \
                      infilenm)
                print(e)

            if ephemfile_status == 0:
                try:
                  ubcsftp.upload(ephemfilenm, ubc_path)
                  os.remove(ephemfilenm)
                except Exception as e:
                    print("ERROR: Failed to upload %s to UBC data "\
                          "archive"%ephemfilenm)
                    print(e)

            if ephemfile_status == 0:
              os.remove(ephemfilenm)

        cftp.close()
        ubcsftp.close()
