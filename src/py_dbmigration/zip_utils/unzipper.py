import zipfile
import tarfile
import gzip
 
import logging
import shutil
import os, logging as lg

logging=lg.getLogger()


class object:
    pass


def extract_file(source_file, writeable_path, skip=False, zip_type='zip', skip_ifexists=False):
    namelist = []
    """
    returns list of file names and path
    """
    dir_exist = os.path.isdir(writeable_path)
    
    if skip_ifexists and dir_exist:
        logging.warning("Extract Target Directory Exists and Skip=True:\nSkipping to Save Time")
    else:
        zip_type = zip_type.lower()
        # shutil.rmtree(writeable_path)
        types=['zip']
        if (any(zip_type in sublist for sublist in types) ):
            file = zipfile.ZipFile(source_file)
            if skip is False:
                logging.info("\tExtracting Zip File:\n\t\t{}".format(source_file))

                file.extractall(writeable_path)
                file.close()
            #else:
            namelist = [str(f) for f in file.filelist]
        # print(source_file)
        types=['gzip','tar','gz']
        if (any(zip_type in sublist for sublist in types) ):
        #if zip_type in ['gzip','tar']:
            logging.info("\tExtracting TAR File:{}".format(source_file))
            file = tarfile.open(source_file, "r:gz")
            namelist = list(file.getnames())
            if skip is False:
                file.extractall(writeable_path)
                file.close()
 
        types=['bz2','b2z']
        if (any(zip_type in sublist for sublist in types) ):
            logging.info("\tExtracting BZ2 File:{}".format(source_file))
            file = tarfile.open(source_file, "r:bz2")
            namelist = list(file.getnames())
            if skip is False:
                file.extractall(writeable_path)
                file.close()
 

    return list(namelist)


def cleanup_file(files):
    for file in files.list:
        logging.info(f"\tDeleting Temp File:{os.path.join(files.path,file)}")
        os.remove(os.path.join(files.path,file))
