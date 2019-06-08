# moving into separate file for later use

# object to run through series of rule to change the data
# only works when import using pandas at this time


class RedactionRules:

    def make_null(self, data_frame, column_name):

        data_frame.drop(column_name, axis=1, inplace=True)

    def make_hash(self, data_frame, column_name):
        import hashlib
        print("Hashing column", column_name)
        data_frame[column_name] = data_frame[column_name].apply(
            lambda x: hashlib.md5(str(x)).hexdigest())

    def make_increment(self, data_frame, column_name):
        import hashlib
        print("Incrementing column", column_name)
        data_frame[column_name] = data_frame[
            column_name].apply(lambda x: x + 1)

    def process_redaction(self, data_frame):

        df_rules = self.df_rules.loc[
            self.df_rules['data_set'] == self.dataset_name]

        redacted_data_frame = data_frame
        # print(redacted_data_frame.columns)
        # drop all columns not in list:

        # print(redacted_data_frame.index.tolist())
        print("Redact Rules:", df_rules)

        for col in redacted_data_frame.columns.tolist():

            if col not in df_rules['column_name'].tolist():
                print("\tDROPING COLUMN NOT IN Redact Rules: {}".format(col))

                self.make_null(redacted_data_frame, col)

        for idx, series in df_rules.iterrows():

            # rule 1 drop the column
            if series.rule == 'drop':  # checking for Nan
                print("Dropping Columns SPECIFIED by Rules", series.column_name)
                # print(redacted_data_frame.columns.tolist())

                self.make_null(redacted_data_frame, series.column_name)
            # rule 2 drop the column
            if series.rule == 'Hash':  # checking for Nan
                self.make_hash(redacted_data_frame, series.column_name)
            if series.rule == 'Increment':  # checking for Nan
                self.make_increment(redacted_data_frame, series.column_name)

        # print(redacted_data_frame.columns)
        return redacted_data_frame

    def __init__(self, rules_file_path, dataset_name, data_frame=None):
        print(rules_file_path)

        self.rules_file_path = rules_file_path
        self.df_rules = pd.read_excel(self.rules_file_path)
        self.dataset_name = dataset_name

        if data_frame is not None:
            self.process_redaction(data_frame)
        # print(x)