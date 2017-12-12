import zipfile
import tarfile
import gzip
import os
import logging


class object:
    pass


def extract_file(source_file, writeable_path, skip=False, zip_type='zip', skip_ifexists=True):
    namelist = []
    """
    returns list of file names and path
    """
    dir_exist = os.path.isdir(writeable_path)

    if skip_ifexists and dir_exist:
        logging.debug("Extract Target Directory Exists and Skip=True")
    else:
        zip_type = zip_type.lower()

        if zip_type == 'zip':
            logging.info("Extracting Zip File:{0}".format(source_file))
            file = zipfile.ZipFile(source_file)
            if skip is False:
                file.extractall(writeable_path)
                file.close()
            namelist = list(file.namelist())
        # print(source_file)
        if zip_type == 'tar':
            logging.info("Extracting TAR File:{0}".format(source_file))
            file = tarfile.open(source_file, "r:")
            namelist = list(file.getnames())
            if skip is False:
                file.extractall(writeable_path)
                file.close()
        if zip_type == 'gzip':
            logging.info("Extracting GZip File:{0}".format(source_file))
            # file = tarfile.TarFile(source_file)
            file = tarfile.open(source_file, "r:gz")
            namelist = list(file.getnames())
            if skip is False:
                file.extractall(writeable_path)
                file.close()


# print writeable_path
# cleanup_file(file.namelist(),writeable_path)

    logging.debug("Files Extracted:{0}".format(list(namelist)))

    return list(namelist)


def cleanup_file(files):
    for file in files.list:
        logging.debug("Deleting File:{0}".format(files.path + '/' + file))
        os.remove(files.path + '/' + file)
