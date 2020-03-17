import os
import re
import hashlib
from datetime import datetime

class Func():
    @classmethod
    def file_information(self, logger, f, reg=None):
        """
        Gathers MD5, size, date created, and extract (if reg passed in) of file

        Parameters
        ----------
        logger: logging.logger
        f: str
            File path
        reg: string
            Regex to match

        Returns
        -------
        tuple
            (md5, size, date, extract) if found else (None, None, None, None)
        """
        try:
            _md5 = hashlib.md5(open(f, "rb").read()).hexdigest()
            _size = os.path.getsize(f)
            _date = datetime.fromtimestamp(os.stat(f).st_mtime)
            _extract = self.file_path_extract(logger, f, reg) if reg else reg

            return (_md5, _size, _date, _extract)
        except Exception as e:
            logger.warning("Something went wrong trying to get file information for: {0}, e: {1}".format(f, e))
            return (None, None, None, None)

    @classmethod
    def file_path_extract(self, logger, f, reg,position=-1):
        """
        Extracts the the matching regext in the file path if there.

        If the file doesn't contain it but the next level in the directory does that will be taken.

        Parameters
        ----------
        logger: logging.logger
        f: str
            File path
        reg: string
            Regex to match

        Returns
        -------
        None or regex value
            If a value is found it will return that else none
        """
        try:
            reg = re.compile(reg)
            found = re.findall(reg, f)
            return None if len(found) == 0 else found[position]
        except re.error:
            logger.error(f"Invalid regex {reg}, skipping")
            return None
        except TypeError as te:
            logger.error(te)
            logger.error(f"TypeError on file path extract for {f}")
            return None
