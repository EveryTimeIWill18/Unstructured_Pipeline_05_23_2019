"""
config_bda_claims
~~~~~~~~~~~~~~~~~
"""
from enum import Enum
from configparser import ConfigParser


class DMSConfigFile(Enum):
    """BDA Unstructured Data Pipeline Configuration"""
    CONFIG_FILE = 'C:\\Users\\wmurphy\\PycharmProjects' \
                  '\\data_processing_pipeline_5_14_2019' \
                  '\\config_files\\config.ini'


class DMSConfiguration(object):
    """BDA pipeline configuration"""

    # create a ConfigParser instance
    config = ConfigParser()
    config.read(DMSConfigFile.CONFIG_FILE.value)
    sections = config.sections()

    @property
    def get_sftp(self)  -> dict:
        """get sftp configuration"""
        # sftp setup
        host = self.config.get(self.sections[0], 'HOST')
        port = self.config.get(self.sections[0], 'PORT')
        username = self.config.get(self.sections[0], 'USERNAME')
        password = self.config.get(self.sections[0], 'PASSWORD')
        # return a dictionary of sftp credentials
        return {'host': host, 'port': port, 'username': username, 'password': password}

    @property
    def get_pickle_path(self) -> str:
        pickle_path = self.config.get(self.sections[2], 'BDA_PICKLE')
        # return path to where bda pipeline pickle files are stored
        return pickle_path

    @property
    def get_mapping_path(self) -> str:
        mapping_path = self.config.get(self.sections[3], 'BDA_MAPPING_PATH')
        return mapping_path

    @property
    def get_log_path(self) -> str:
        logfile_path = self.config.get(self.sections[4], 'LOG_FILE_PATH')
        return logfile_path

    @property
    def get_write_paths(self) -> dict:
        eml = self.config.get(self.sections[5], 'DMS_EML_WRITE_PATH')
        rtf = self.config.get(self.sections[5], 'DMS_RTF_WRITE_PATH')
        doc = self.config.get(self.sections[5], 'DMS_DOC_WRITE_PATH')
        docx = self.config.get(self.sections[5], 'DMS_DOCX_WRITE_PATH')
        pdf = self.config.get(self.sections[5], 'DMS_PDF_WRITE_PATH')
        doc_to_csv = self.config.get(self.sections[5], 'DMS_DOC_TO_CSV')
        pdf_img = self.config.get(self.sections[5], 'DMS_PDF_IMG')

        # return a dictionary of file write paths
        return {'eml': eml, 'rtf': rtf, 'doc': doc,
                'docx': docx, 'pdf': pdf, 'doc_to_csv': doc_to_csv,
                'pdf_img': pdf_img}

    @property
    def get_raw_data(self) -> dict:
        prod = self.config.get(self.sections[1], 'PROD')
        historical = self.config.get(self.sections[1], 'HISTORICAL')
        delta = self.config.get(self.sections[1], 'DELTA')
        dev = self.config.get(self.sections[1], 'DEV')
        dev_historical = self.config.get(self.sections[1], 'DEV_HISTORICAL')
        dev_dev = self.config.get(self.sections[1], 'DEV_DEV')
        return {'prod': prod, 'historical': historical, 'delta': delta,
                'dev': dev, 'dev_historical': dev_historical, 'dev_dev': dev_dev}

    @property
    def get_r_files(self) -> dict:
        r_path = self.config.get(self.sections[6], 'R_PATH')
        r_executable = self.config.get(self.sections[5], 'R_EXECUTABLE')
        r_script_one = self.config.get(self.sections[6], 'R_SCRIPT_ONE')
        r_script_two = self.config.get(self.sections[6], 'R_SCRIPT_TWO')
        r_script_three = self.config.get(self.sections[6], 'R_SCRIPT_THREE')
        # return a dictionary of the required R scripts/executables
        return {'r_path': r_path, 'r_executable': r_executable,
                'r_script_one': r_script_one, 'r_script_two': r_script_two,
                'r_script_three': r_script_three}

    @property
    def get_remote_paths(self) -> dict:
        bda_pipeline = self.config.get(self.sections[7], 'BDA_PIPELINE')
        remote_pickle_path = self.config.get(self.sections[7], 'REMOTE_PICKLE_PATH')
        remote_mapping_path = self.config.get(self.sections[7], 'REMOTE_MAPPING_PATH')
        remote_zip_path = self.config.get(self.sections[7], 'REMOTE_ZIP_PATH')
        remote_log_path = self.config.get(self.sections[7], 'REMOTE_LOG_PATH')
        python_ngrams_script = self.config.get(self.sections[7], 'PYTHON_NGRAMS_SCRIPT')

        # return a dictionary of the remote directories
        return {'bda_pipeline': bda_pipeline, 'remote_pickle_path': remote_pickle_path,
                'remote_mapping_path': remote_mapping_path, 'remote_zip_path':remote_zip_path,
                'remote_log_path': remote_log_path, 'python_ngrams_script': python_ngrams_script}


# create an instance of the bda configuration file
dms_config = DMSConfiguration()


