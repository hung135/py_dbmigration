import zipfile
import tarfile
import gzip
 
import logging
import shutil
import os, logging

#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')


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
        if zip_type == 'zip':

            file = zipfile.ZipFile(source_file)
            if skip is False:
                logging.info("\tExtracting Zip File:\n\t\t{}".format(source_file))

                file.extractall(writeable_path)
                file.close()
            #else:
            namelist = [str(f) for f in file.filelist]
        # print(source_file)
        if zip_type == 'tar':
            logging.info("\tExtracting TAR File:{}".format(source_file))
            file = tarfile.open(source_file, "r:")
            namelist = list(file.getnames())
            if skip is False:
                file.extractall(writeable_path)
                file.close()
        if zip_type == 'gzip':
            logging.info("\tExtracting GZip File:{}".format(source_file))
            #file = tarfile.TarFile(source_file)
            file = tarfile.open(source_file, "r:gz")
            namelist = list(file.getnames())
            if skip is False:
                file.extractall(writeable_path)
                file.close()

    return list(namelist)


def cleanup_file(files):
    for file in files.list:
        logging.info(f"\tDeleting Temp File:{os.path.join(files.path,file)}")
        os.remove(os.path.join(files.path,file))
