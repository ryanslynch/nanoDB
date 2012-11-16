#! /usr/bin/python

debug = True

"""
Upload fold-mode pulsar data taken with GUPPI, PUPPI, and/or ASP to
the NANOGrav FTP server hosted at Cornell University.  If possible,
create an ephemeris (\"par\" file) and upload it, too (only available
for PSRFITS GUPPI and PUPPI data).

Author       : Ryan S. Lynch
Contact      : ryan.lynch@nanograv.org
Last Updated : 16 Nov. 2012
"""

import pyfits as PF
import subprocess as SP
import nanoCredentials, M2Crypto, sys, os
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
  

# For security reasons, FTP credentials are stored in a separate file
# and are assigned here.
hostname   = nanoCredentials.hostname
portnumber = nanoCredentials.portnumber
username   = nanoCredentials.username
password   = nanoCredentials.password

class CornellFTP(M2Crypto.ftpslib.FTP_TLS):
    """
    Connect to the Cornell FTP server using M2Crypto FTP_TLS.  Modified from
    PALFA CornellFTP class by Patrick Lazarus.
    """
    
    def __init__(self, host=hostname, port=portnumber, username=username,
                 password=password, *args, **kwargs):

        M2Crypto.ftpslib.FTP_TLS.__init__(self, *args, **kwargs)

        try:
            self.connect(host, port)
            self.auth_tls()
            self.set_pasv(1)
            self.login(username, password)

        except:
            print "Unable to connect to Cornell FTP server."
            print "Exiting..."
            exit(1)

        else:
            print "Successfully connected to Cornell FTP server."

    
    
    def __del__(self):
        
        if self.sock is not None:
            self.quit()
    
    
    def mdr(self, ftp_path):
        """
        Recursively make a directory on the FTP server.

        Parameters
        ----------
        ftp_path : str
            The path to create on the FTP server.

        Returns
        -------
        status : int
            Zero indicates success.  One indicates failure.
        """
        
        try:
            for directory in ftp_path.split("/"):                
                # Get the list of directories in the current working directory
                dirlst = self.nlst()
                if directory not in dirlst:
                    # Make the directory if needed
                    ret = self.mkd(directory)
                # Change into this directory
                ret = self.cwd(directory)
            
        except:
            print "Unable to create remote directory structure."
            status = 1

        else:
            print "Successfully created remote directory structure."
            status = 0

        finally:
            # Other scripts expect to start in the top-level directory, so
            # always return there
            self.cwd("/")

        return status


    def upload(self, filenm, ftp_path):
        """
        Upload a file to the FTP server.

        Parameters
        ----------
        filenm : str
            The name of the file to upload
        ftp_path : str
            The destination path on the FTP server.

        Returns
        -------
        status : int           
            Zero indicates success.  One indicates failure.
        """

        # full path + file name on FTP server
        ftpfn = os.path.join(ftp_path, filenm)

        if filenm not in self.nlst(ftp_path):
            f = open(filenm, 'r')
            self.sendcmd("TYPE I")
            print "Starting upload of %s"%filenm

            try:
                # The actual upload command
                self.storbinary("STOR "+ftpfn, f)
            
            except:
                print "Upload of %s failed"%filenm
                status = 1
                
            else:
                print "Finished upload of %s"%filenm

            finally:
                f.close()

            # Verify that the sizes of the uploaded and local file are the same
            ftp_size   = self.size(ftpfn)
            local_size = os.path.getsize(filenm)
            
            if ftp_size == local_size:
                print "Upload of %s successful."%filenm
                status = 0
                
            else:
                print "Verification of %s failed"%filenm
                print "Local file is not the same size as uploaded file (%d != %d)."%(local_size, ftp_size)
                status = 1

        else:
            print "%s already on FTP server.  Skipping..."%filenm
            status = 0
        
        return status


def write_ephemeris(infilenm):
    """
    Read ephemeris information from a PSRFITS archive and write a tempo-style
    \"par\" file for it.  Parameters with zero or empty-string values are
    not written.

    Parameters
    ----------
    infilenm : str
        The name of the input archive

    Returns
    -------
    outfilenm : str
        The name of the output par file
    status : int
        Zero indicates success.  One indicates failure.
    """
    
    hdulist = PF.open(infilenm)
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
        print "Ephemeris written to %s"%outfilenm
        status = 0

    else:
        # ASP files have no ephemeris table, so do nothing for them
        outfilenm = None
        print "WARNING: Ephemeris not written because BACKEND != G/PUPPI!"
        status   = 1

    hdulist.close()

    return (outfilenm,status)


def get_ftp_path(infilenm):
    """
    Parse an input fits file header and create an remote FTP path based on
    the source name and year (at start time UTC).

    Parameters
    ----------
    infilenm : str
        The name of the input archive

    Returns
    -------
    path : str
        The path to send to the FTP server
    """

    hdulist = PF.open(infilenm)
    hdr     = hdulist[0].header
    backend = hdr["BACKEND"]
    source  = hdr["SRC_NAME"]

    if backend == "GUPPI" or backend == "PUPPI":
        year = hdr["DATE-OBS"].split("-")[0]
        
    elif backend == "xASP":
        MJD  = hdr["STT_IMJD"] + hdr["STT_SMJD"]/SECPERDAY
        year = str(sla_djcl(MJD)[0]) # Convert MJD to calendar date

    else:
        print "WARNING: Unrecognized backend for %s!"%infilenm
        print "         Only GUPPI, PUPPI and ASP are supported."
        exit(1)

    path = os.path.join(source, "rawdata", year)

    return path


if __name__ == "__main__":
    arguments = sys.argv[1:] # Get the list of in-file names
    
    if len(arguments) == 0 or "-h" in arguments or "--help" in arguments:
        print usage
        sys.exit(0)

    elif any([arg.startswith("-") for arg in arguments]):
        print "ERROR: Unrecgonized option"
        print usage

    else:
        cftp = CornellFTP()
        
        for infilenm in arguments:
            print "\nWorking on %s..."%infilenm
            parfilenm, parfile_status = write_ephemeris(infilenm)
            
            if debug:
                ftp_path = os.path.join("NANOGrav", "Test",
                                        get_ftp_path(infilenm))
            
            else:
                ftp_path = os.path.join("NANOGrav", get_ftp_path(infilenm))
            
            mdr_status = cftp.mdr(ftp_path)
            
            upload_status = cftp.upload(infilenm, ftp_path)
        
            if parfile_status == 0:
                cftp.upload(parfilenm, ftp_path)

        cftp.close()
