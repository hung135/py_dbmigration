import zipfile
import tarfile
import gzip
import os
import logging


class object:
    pass


def extract_file(source_file, writeable_path, skip=False,zip_type='zip'):
    namelist = object()

    """
    returns list of file names and path
    """
# print source_file
    #logging.info("Extracting Zip File:{0}".format(source_file))
    if zip_type=='zip':
        file = zipfile.ZipFile(source_file)
        if skip is False:
            file.extractall(writeable_path)
            file.close()
        namelist.list = file.namelist()
    #print(source_file)
    if zip_type=='tar':
        file = tarfile.open(source_file, "r:")
        namelist.list = file.getnames()
        if skip is False:
            file.extractall(writeable_path)
            file.close()
    if zip_type=='gzip':
        #file = tarfile.TarFile(source_file)
        file=tarfile.open(source_file,"r:gz")
        namelist.list = file.getnames()
        if skip is False:
            file.extractall(writeable_path)
            file.close()

        
        
        



# print writeable_path
# cleanup_file(file.namelist(),writeable_path)
    
    
    logging.debug("Files Extracted:{0}".format(namelist.list))
    namelist.path = writeable_path

    namelist.total_files = len(namelist.list)
    # namelist.cleanup_file=cleanup_file
    return namelist


def cleanup_file(files):
    for file in files.list:
        logging.debug("Deleting File:{0}".format(files.path + '/' + file))
        os.remove(files.path + '/' + file)
