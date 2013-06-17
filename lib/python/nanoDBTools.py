import os, getpass, M2Crypto, paramiko, nanoCredentials

# For security reasons, credentials are stored in a separate file and
# are assigned here.
cornell_upload_hostname     = nanoCredentials.cornell_upload_hostname
cornell_upload_portnumber   = nanoCredentials.cornell_upload_portnumber
cornell_upload_username     = nanoCredentials.cornell_upload_username
cornell_upload_password     = nanoCredentials.cornell_upload_password

cornell_download_hostname   = nanoCredentials.cornell_download_hostname
cornell_download_portnumber = nanoCredentials.cornell_download_portnumber
cornell_download_username   = nanoCredentials.cornell_download_username
cornell_download_password   = nanoCredentials.cornell_download_password

ubc_hostname                = nanoCredentials.ubc_hostname



class CornellFTP(M2Crypto.ftpslib.FTP_TLS):
    """
    Connect to the Cornell FTP server using M2Crypto FTP_TLS.  Modified from
    PALFA Cornell FTP class by Patrick Lazarus.
    """
    
    def __init__(self, mode="upload", *args, **kwargs):
        
        M2Crypto.ftpslib.FTP_TLS.__init__(self, *args, **kwargs)
        
        if mode == "upload":
            hostname   = cornell_upload_hostname
            portnumber = cornell_upload_portnumber
            username   = cornell_upload_username
            password   = cornell_upload_password
        
        elif mode == "download":
            hostname   = cornell_download_hostname
            portnumber = cornell_download_portnumber
            username   = cornell_download_username
            password   = cornell_download_password
        
        self.connect(hostname, portnumber)
        self.auth_tls()
        self.set_pasv(1)
        self.login(username, password)
    
    
    def upload(self, srcpth, destdir):
        """
        Upload a file to the FTP server, recursively making the directory
        structure if necessary.

        Paramters
        ---------
        srcpth : str
            The full path to the source file.
        destdir : str
            The full path to the destination directory.

        Returns
        -------
        None
        """
        
        # Split the source path into the directory and file name
        srcdir, filenm = os.path.split(srcpth)
        # Try to make the destination directory path
        for directory in destdir.split("/"):
            # Get the list of directories in the current working directory
            dirlist = self.nlst() 
            if directory not in dirlist:
                self.mkd(directory) # Make the new directory if necessary
            self.cwd(directory) # Descend into the directory
        # The upload expects to start from the root directory, so go there
        self.cwd("/")
        
        destpth = os.path.join(destdir, filenm)
        # Now upload the file if it doesn't already exist
        if filenm not in self.nlst(destdir):
            f = open(srcpth, "r")
            self.sendcmd("TYPE I")
            # The actual upload command
            self.storbinary("STOR %s"%destpth, f) 
            f.close()

            # Now verify that the uploaded file has the correct size
            srcsize  = os.path.getsize(srcpth)
            destsize = self.size(destpth)
            
            if srcsize == destsize:
                pass
                
            else:
                print("\nWARNING: Verification of %s on "\
                      "Cornell FTP failed"%(filenm))
        
        # Do nothing if the file already exists on the remote server
        else:
            print("\nSkipping %s because it already exists on Cornell FTP" % \
                  filenm)
        
        return None
    
    
    def download(self, srcpth, destdir=os.path.curdir):
        """
        Download a file from the FTP server.

        Paramters
        ---------
        srcpth : str
            The full path to the source file.
        destdir : str
            The full path to the destination directory.

        Returns
        -------
        None
        """
        
        # Get the file name, without the full path
        filenm = os.path.split(srcpth)[1]
        # Open a binary file for writing
        f      = open(os.path.join(destdir, filenm), "wb")
        
        # Initiate the download
        self.sendcmd("TYPE I")
        self.retrbinary("RETR %s"%srcpth, lambda block: f.write(block))
        f.close()
        
        return None



class UBCSFTP(object):
    """
    Connect to the UBC data archive using a paramiko SSH client.
    """
    
    def __init__(self, hostname=ubc_hostname):
        
        username = getpass.getpass(prompt="Enter UBC username: ")
        password = getpass.getpass(prompt="Enter UBC password: ")
        
        self.client = paramiko.SSHClient()
        # This tells paramiko to automatically accept unknown host keys
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(hostname, username=username, password=password)
        self.sftp = self.client.open_sftp()
    
    
    def upload(self, srcpth, destdir):
        """
        Upload a file to the archive using sftp, recursively making the
        directory structure if necessary.

        Paramters
        ---------
        srcpth : str
            The full path to the source file.
        destdir : str
            The full path to the destination directory.
        
        Returns
        -------
        None
        """
        
        # Split the source path into the directory and file name
        srcdir, filenm = os.path.split(srcpth)
        # Try to make the remote directory path
        path = "/"
        for directory in destdir.split("/"):
            try:
                path = os.path.join(path, directory)
                self.sftp.mkdir(path)
            # This will be thrown if the directory already exists.  In that
            # case, just keep going
            except IOError:
                pass
        
        destpth = os.path.join(destdir, filenm)
        # Now upload the file if it doesn't already exist
        if filenm not in self.sftp.listdir(destdir):
            self.sftp.put(srcpth, destpth)
            
            # Now verify that the uploaded file has the correct size
            srcsize  = os.path.getsize(srcpth)
            destsize = self.sftp.stat(destpth).st_size
            
            if srcsize == destsize:
                pass
            
            else:
                print("\nWARNING: Verification of %s on "\
                      "UBC archive failed"%(filenm))
        
        # Do nothing if the file already exists on the remote server
        else:
            print("\nSkipping %s because it already exists on UBC archive" % \
                  filenm)
        
        return None
    
    
    def close(self):
        "Close the SSH client session."
        
        self.sftp.close()
        self.client.close()



def parse_metafile(filenm):
    """
    Parse a NANOGrav metadata file and turn each entry into a python
    dictionary.

    Parameters
    ----------
    filenm : str
      The input file name.

    Returns
    -------
    entris : list
      A list of python dictionaries corresponding to the entries in the
      input file.
    """
    
    # Open the input file and read each line
    
    infile  = open(filenm, "r")
    lines   = infile.readlines()
    infile.close()
    # Make an empty list and dictionary for storing the entries
    entries = []
    entry   = {}
    
    for line in lines:
        try:
            # For each line in the input file, get the dictionary key and
            # value by splitting on the first colon
            key   = line.split(":")[0].strip()
            value = ":".join(line.split(":")[1:]).strip()
            
            # If the key is ProfileName, assume the start of a new entry
            # and copy the old entry to the list
            if key == "ProfileName":
                if len(entry.keys()) != 0: entries.append(entry.copy())
                entry = {}
            
            # len(key) could be 0 for empty lines, so only update the
            # dictionary if that isn't the case
            if len(key) != 0: entry[key] = value
        
        except:
            pass
    
    # This appends the last entry
    if len(entry.keys()) != 0: entries.append(entry.copy())
    
    # Add a "type" key 
    for entry in entries:
        if "TOA" in entry.keys(): entry["type"] = "TOA"
        else: entry["type"] = "archive"
    
    return entries
