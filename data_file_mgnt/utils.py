def import_daa_file(foi, db):

    import_type = foi.import_method or import_type

    if import_type == self.IMPORT_VIA_PANDAS:
        limit = None
        if limit_rows is not None:

            limit = limit_rows
        elif foi.limit_rows is not None:
            limit = foi.limit_rows
        else:
            limit = None

        ####################################################################################
        status_dict = self.import_file_pandas(foi, db, limit_rows=limit,
                                              chunk_size=chunksize)
        ####################################################################################
    # only postgres support for now
    elif import_type == self.IMPORT_VIA_CLIENT_CLI:
        ####################################################################################
        status_dict = self.import_1file_client_side(foi, db)
        ####################################################################################
    elif import_type == self.IMPORT_VIA_CUSTOM_FUNC:
        status_dict == self.CUSTOM_FUNC()
    else:
        raise Exception(
            "No Import Method Provided: \n{}\n{}".format(
                self.IMPORT_VIA_CLIENT_CLI,
                self.IMPORT_VIA_PANDAS))
