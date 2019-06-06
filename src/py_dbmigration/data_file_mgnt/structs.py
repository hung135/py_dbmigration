from enum import Enum
import os
class import_status(Enum):
    RAW = 'RAW'
    FAILED = 'FAILED'
    PROCESSED = 'PROCESSED'
    PROCESSING = 'PROCESSING'
    OBSOLETE = 'OBSOLETE'
    DUPLICATE = 'DUPLICATE'
    UNK = 'UNKNOWN'


class Status:
    # object to carry status info for prossing and import
    status = None
    name = None
    rows_inserted = 0
    error_msg = None
    additional_info = None
    import_status = import_status.UNK
    rows_inserted = 0
    continue_processing=False
    def __init__(self, file):
        self.file_path=__file__
        self.name=os.path.basename(__file__)
        self.import_status=None
        self.status = 'Init'
        self.rows_inserted = 0
        self.error_msg = None
        self.additional_info = None
        self.rows_inserted = 0
        self.file_size =0
   
    def __str__(self):
        return_string="""LogicFile: {}\nStatus: {}\nError_msg: {}\nAdditiona_info: {}\n """
        return return_string.format(self.name,self.status,self.error_msg,self.additional_info)
