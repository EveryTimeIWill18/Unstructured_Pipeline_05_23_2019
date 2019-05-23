"""
metadata
~~~~~~~~
Load in the meta data file.
NOTE:
    Important Columns:
        - Object_id
        - Object_name
        - Prior_Version_Object_Name
        - Version
        - File_name
        - Format
        - claim_id


"""
import os
import xlrd
import pandas as pd
from datetime import datetime, timedelta
from unstructured_data_pipeline.interfaces.metadata_interface import MetaDataInterface


pd.set_option("display.max_rows", 101)
pd.set_option("display.max_columns", 50)

METADATA_TEST = 'Y:\\Shared\\USD\\Business Data and Analytics' \
                '\\Unstructured_Data_Pipeline\\metadata_test.xls'

# Concrete Implementations #
####################################################################################################
class MetaData(MetaDataInterface):
    """Load in the metadata file"""
    def __init__(self):
        self.START_DATE: str = '20190502'  # first delta file in the filesystem
        self.METADATA_LOG: str = 'metadata_files_log.csv'   # name of the meta data file log
        self.last_used_metadata_file: str = None
        self.current_metadata_file: pd.DataFrame = None

    def load_metadata(self, file_path: str):
        try:
            metadata_filename = str(datetime.today() - timedelta(days=1))[:10].replace('-', '')
            for _, _, files in os.walk(os.path.join(file_path, metadata_filename)):
                if metadata_filename + '.xls' in files:
                    self.current_metadata_file = pd.read_excel(
                        xlrd.open_workbook(
                            filename=os.path.join(
                                file_path, metadata_filename,
                                'Metadata', metadata_filename + '.xls'
                            ),
                            encoding_override="cp1252"
                        )
                    )
                    self.last_used_metadata_file = metadata_filename
        except:
            pass

    def load_metadata_file(self, full_file_path: str) -> pd.DataFrame:
        """
        NOTE:
            Method should be used only to check output.
            Should not be used within the data pipeline.
        """
        try:
            if os.path.isfile(full_file_path) and os.path.splitext(full_file_path)[-1] == '.xls':
                metadata_file = pd.read_excel(
                    xlrd.open_workbook(filename=full_file_path, encoding_override="cp1252")
                )
                return metadata_file
            else:
                raise OSError(f'OSError: File: {os.path.basename(full_file_path)} not found.')
        except OSError as e:
            print(e)

    def load_backlogged_metadata(self, file_path: str, days: int):
        try:
            current = os.listdir(os.path.join(file_path))
            backlog = [str(datetime.today() - timedelta(days=d))[:10].replace('-', '')
                            for d in reversed(range(1, days+1, 1))]
            return backlog
        except:
            pass

    def update_metadata_log(self, file_path: str):
        try:
            if os.path.isdir(file_path):
                if self.METADATA_LOG in os.listdir(file_path):
                    # append to the metadata log
                    with open(os.path.join(file_path, self.METADATA_LOG), 'a') as f:
                        f.write(self.last_used_metadata_file)
                else:
                    # create the metadata log for the first time
                    with open(os.path.join(file_path, self.METADATA_LOG), 'w') as f:
                        f.write(self.last_used_metadata_file)
            return self.last_used_metadata_file
        except Exception as e:
            print(e)

    def load_metadata_log(self, file_path: str):
        try:
            if os.path.isdir(file_path):
                if len(os.listdir(file_path)) > 0:
                    pass
                else:
                    print("No metadata files to ")
        except:
            pass
