#! /usr/bin/python

debug = False

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
import nanoCredentials, M2Crypto, sys, os, paramiko
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


# For security reasons, credentials are stored in a separate file and
# are assigned here.
cornell_hostname   = nanoCredentials.cornell_hostname
cornell_portnumber = nanoCredentials.cornell_portnumber
cornell_username   = nanoCredentials.cornell_username
cornell_password   = nanoCredentials.cornell_password

ubc_hostname = nanoCredentials.ubc_hostname
ubc_username = nanoCredentials.ubc_username
ubc_password = nanoCredentials.ubc_password


class CornellFTP(M2Crypto.ftpslib.FTP_TLS):
    """
    Connect to the Cornell FTP server using M2Crypto FTP_TLS.  Modified from
    PALFA Cornell FTP class by Patrick Lazarus.
    """
    
    def __init__(self, host=cornell_hostname, port=cornell_portnumber,
                 username=cornell_username, password=cornell_password,
                 *args, **kwargs):
        
        M2Crypto.ftpslib.FTP_TLS.__init__(self, *args, **kwargs)

        self.connect(host, port)
        self.auth_tls()
        self.set_pasv(1)
        self.login(username, password)
    
    
    def upload(self, filenm, remote_path):
        """
        Upload a file to the FTP server, recursively making the directory
        structure if necessary.

        Paramters
        ---------
        filenm : str
            The name of the local file to be uploaded.
        remote_path : str
            The remote destination path.

        Returns
        -------
        None
        """

        # First try to make the remote directory path
        for directory in remote_path.split("/"):
            # Get the list of directories in the current working directory
            dirlist = self.nlst() 
            if directory not in dirlist:
                self.mkd(directory) # Make the new directory if necessary
            self.cwd(directory) # Descend into the directory
        # The upload expects to start from the root directory, so go there
        self.cwd("/")

        remote_filenm = os.path.join(remote_path, filenm)
        # Now upload the file if it doesn't already exist
        if filenm not in self.nlst(remote_path):
            f = open(filenm, "r")
            self.sendcmd("TYPE I")
            # The actual upload command
            self.storbinary("STOR %s"%remote_filenm, f) 
            f.close()

            # Now verify that the uploaded file has the correct size
            remote_size = self.size(remote_filenm)
            local_size  = os.path.getsize(filenm)
            
            if remote_size == local_size:
                print("Successfully uploaded %s to Cornell FTP server"%filenm)
                
            else:
                print("WARNING: Verification of %s on "\
                      "Cornell FTP server failed"%(filenm))
                print("         Local file size (%d) does not match "\
                      "remote file size (%d)"%(local_size, remote_size))

        # Do nothing if the file already exists on the remote server
        else:
            print("%s already exists on Cornell FTP server.  Skipping..." \
                  %filenm)


        return None


class UBCSFTP(object):
    """
    Connect to the UBC data archive using a paramiko SSH client.
    """

    def __init__(self, host=ubc_hostname, username=ubc_username,
                 password=ubc_password):
        
        self.client = paramiko.SSHClient()
        # This tells paramiko to automatically accept unknown host keys
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(host, username=username, password=password)
        self.sftp = self.client.open_sftp()
    
    
    def upload(self, filenm, remote_path):
        """
        Upload a file to the archive using sftp, recursively making the
        directory structure if necessary.

        Paramters
        ---------
        filenm : str
            The name of the local file to be uploaded.
        remote_path : str
            The remote destination path.

        Returns
        -------
        None
        """

        # First try to make the remote directory path
        path = "/"
        for directory in remote_path.split("/"):
            try:
                path = os.path.join(path, directory)
                self.sftp.mkdir(path)
            # This will be thrown if the directory already exists.  In that
            # case, just keep going
            except IOError:
                pass
                
        remote_filenm = os.path.join(remote_path, filenm)
        # Now upload the file if it doesn't already exist
        if filenm not in self.sftp.listdir(remote_path):
            self.sftp.put(filenm, remote_filenm)
            
            # Now verify that the uploaded file has the correct size
            remote_size = self.sftp.stat(remote_filenm).st_size
            local_size  = os.path.getsize(filenm)
            if remote_size == local_size:
                print("Successfully uploaded %s to UBC data archive"%filenm)
                
            else:
                print("WARNING: Verification of %s on "\
                      "UBC data archive failed"%(filenm))
                print("         Local file size (%d) does not match "\
                      "remote file size (%d)"%(local_size, remote_size))

        # Do nothing if the file already exists on the remote server
        else:
            print("%s already exists on UBC data archive.  Skipping..."%filenm)
        
        
        return None


    def close(self):
        "Close the SSH client session."
        
        self.sftp.close()
        self.client.close()


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
        cftp    = CornellFTP()
        ubcsftp = UBCSFTP()
        
        for infilenm in arguments:
            ephemfile_status, ephemfilenm = write_ephemeris(infilenm)
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
            except:
                print("ERROR: Failed to upload %s to Cornell FTP server" % \
                      infilenm)

            try:
                ubcsftp.upload(infilenm, ubc_path)
            except:
                print("ERROR: Failed to upload %s to UBC data archive" % \
                      infilenm)

            if ephemfile_status == 0:
                try:
                    cftp.upload(ephemfilenm, cornell_path)
                except:
                    print("ERROR: Failed to upload %s to Cornell FTP "\
                          "server"%ephemfilenm)
                
                try:
                    ubcsftp.upload(ephemfilenm, ubc_path)
                except:
                    print("ERROR: Failed to upload %s to UBC data "\
                          "archive"%ephemfilenm)                    

        cftp.close()
        ubcsftp.close()
