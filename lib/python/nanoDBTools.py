import os, getpass, M2Crypto, paramiko, nanoCredentials

# For security reasons, credentials are stored in a separate file and
# are assigned here.
cornell_upload_hostname   = nanoCredentials.cornell_upload_hostname
cornell_upload_portnumber = nanoCredentials.cornell_upload_portnumber
cornell_upload_username   = nanoCredentials.cornell_upload_username
cornell_upload_password   = nanoCredentials.cornell_upload_password

cornell_download_hostname   = nanoCredentials.cornell_download_hostname
cornell_download_portnumber = nanoCredentials.cornell_download_portnumber
cornell_download_username   = nanoCredentials.cornell_download_username
cornell_download_password   = nanoCredentials.cornell_download_password

ubc_hostname = nanoCredentials.ubc_hostname

class CornellFTP(M2Crypto.ftpslib.FTP_TLS):
    """
    Connect to the Cornell FTP server using M2Crypto FTP_TLS.  Modified from
    PALFA Cornell FTP class by Patrick Lazarus.
    """
    
    def __init__(self, mode="upload", *args, **kwargs):
        
        M2Crypto.ftpslib.FTP_TLS.__init__(self, *args, **kwargs)

        if mode == "upload":
            host     = cornell_upload_hostname
            port     = cornell_upload_portnumber
            username = cornell_upload_username
            password = cornell_upload_password

        elif mode == "download":
            host     = cornell_download_hostname
            port     = cornell_download_portnumber
            username = cornell_download_username
            password = cornell_download_password

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


    def download(self, remote_path, local_path=os.path.curdir):
        """
        Download a file from the FTP server.

        Paramters
        ---------
        remote_path : str
            The full path and file name on the remote host
        remote_path : str
            The path to the local destination

        Returns
        -------
        None
        """
        # Get the file name, without the full path
        filenm = os.path.split(remote_path)[1]
        # Open a binary file for writing
        f      = open(os.path.join(local_path, filenm), "wb")

        # Initiate the download
        self.sendcmd("TYPE I")
        self.retrbinary("RETR %s"%remote_path, lambda block: f.write(block))
        f.close()

        print("Successfully downloaded %s from Cornell FTP server"%filenm)

        return None



class UBCSFTP(object):
    """
    Connect to the UBC data archive using a paramiko SSH client.
    """

    def __init__(self, host=ubc_hostname):

        username = raw_input("Enter UBC username: ")
        password = getpass.getpass(prompt="Enter UBC password: ")
        
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


class DataProduct(object):
    def __init__(self, profile_name):
        self.ProfileName = profile_name

    def add_archive(nsubs, wsub, ndumps, tdump, fmt, src_raw_profilenm,
                    src_profilenm, processing_type, processing_filenm):
        self.type                 = "archive"
        self.n_subbands           = nsubs
        self.subbands_width       = wsub
        self.n_dumps              = ndumps
        self.dump_length          = tdump
        self.format               = fmt
        self.sourceRawProfileName = src_raw_profilenm
        self.sourceProfileName    = src_profilenm
        self.processingType       = processing_type
        self.processingFileName   = processing_filenm
    
    
    def add_toa(psrnm, tmpltnm, toa, toa_err, mjd, scope_code,
                details_filenm, subidx, dumpidx, date_loaded,
                date_calculated):
        self.type                 = "TOA"
        self.pulsarName           = psrnm
        self.templateName         = tmpltnm
        self.TOA                  = toa
        self.TOAError             = toa_error
        self.MJD                  = mjd
        self.telescopeCode        = scope_code
        self.detailsFileName      = details_filenm
        self.subband_idx          = subidx
        self.dump_idx             = dumpidx
        self.date_loaded          = date_loaded
        self.date_calculated      = date_calculated
        

def parse_metafile(infilenm):
    infile  = open(infilenm, "r")
    lines   = infile.readlines()
    entries = []
    entry   = {}
    
    for line in lines:
        try:
            key, value = [s.strip() for s in line.split(":")]
            
            if key == "ProfileName":
                if len(entry.keys()) != 0: entries.append(entry.copy())
                entry = {}
        
            entry[key] = value
            
        except:
            pass
        
    for entry in entries:
        if "TOA" in entry.keys(): entry["type"] = "TOA"
        else: entry["type"] = "archive"
        
    return entries
