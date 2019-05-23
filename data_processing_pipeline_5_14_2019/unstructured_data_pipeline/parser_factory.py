"""
parser_factory
~~~~~~~~~~~~~~
"""
import os
import pickle
import inspect
from pprint import pprint
from unstructured_data_pipeline.concrete_file_preprocessors import FileGenerator
from unstructured_data_pipeline.concrete_file_preprocessors import EmlParser
from unstructured_data_pipeline.concrete_file_preprocessors import DocParser
from unstructured_data_pipeline.concrete_file_preprocessors import PdfParser
from unstructured_data_pipeline.concrete_file_preprocessors import RtfParser
from unstructured_data_pipeline.concrete_file_preprocessors import DocxParser
from unstructured_data_pipeline.concrete_file_preprocessors import PdfParser
from unstructured_data_pipeline.hive_server import SftpData, SftpConnection
from unstructured_data_pipeline.concrete_file_preprocessors import d, logger
from unstructured_data_pipeline.configs.config_personal_umbrella import pu_config
from unstructured_data_pipeline.configs.config_dms_claims import dms_config
from unstructured_data_pipeline.configs.config_sa_claims import sa_config

# Class Decorators #
####################################################################################################
def decorate_dms_mapping_dict(cls):
    """Class decorator for adding the additional
    mapping requirements to the DMS_Claims project"""
    try:
        if cls.__dict__['project'] == 'personal_umbrella':
            pass

    except:
        pass


# Factory Design Pattern #
####################################################################################################
class BaseFactory(object):
    """Parent factory design pattern to establish file paths"""
    def __init__(self, project: str):
        # Personal Umbrella setup
        if project == 'personal_umbrella':
            self.__dict__['project'] = 'personal_umbrella'
            self.__dict__['raw_data'] = pu_config.get_raw_data
            self.__dict__['pickle_path'] = pu_config.get_pickle_path
            self.__dict__['mapping_path'] = pu_config.get_mapping_path
            self.__dict__['logfile_path'] = pu_config.get_log_path
            self.__dict__['write_paths'] = pu_config.get_write_paths
            self.__dict__['sftp'] = pu_config.get_sftp
            self.__dict__['r_files'] = pu_config.get_r_files
            self.__dict__['remote_paths'] = pu_config.get_remote_paths

        # DMS Claims setup
        elif project == 'dms_claims':
            self.__dict__['project'] = 'dms_claims'
            self.__dict__['raw_data'] = dms_config.get_raw_data
            self.__dict__['pickle_path'] = dms_config.get_pickle_path
            self.__dict__['mapping_path'] = dms_config.get_mapping_path
            self.__dict__['logfile_path'] = dms_config.get_log_path
            self.__dict__['write_paths'] = dms_config.get_write_paths
            self.__dict__['sftp'] = dms_config.get_sftp
            self.__dict__['r_files'] = dms_config.get_r_files
            self.__dict__['remote_paths'] = dms_config.get_remote_paths
        # SA Claims setup
        elif project == 'sa_claims':
            self.__dict__['project'] = 'sa_claims'
            self.__dict__['raw_data'] = sa_config.get_raw_data
            self.__dict__['pickle_path'] = sa_config.get_pickle_path
            self.__dict__['mapping_path'] = sa_config.get_mapping_path
            self.__dict__['logfile_path'] = sa_config.get_log_path
            self.__dict__['write_paths'] = sa_config.get_write_paths
            self.__dict__['sftp'] = sa_config.get_sftp
            self.__dict__['r_files'] = sa_config.get_r_files
            self.__dict__['remote_paths'] = sa_config.get_remote_paths
        else:
            raise ValueError(f'ValueError: {project} not a valid project name')


class ParserFactory(object):
    """Factory class for building the file parsers"""
    file_parser: dict = None
    file_extensions: list = ['doc', 'docx', 'eml', 'pdf', 'rtf']
    file_ext: str = None
    current_parser_obj = None
    base_factory_: BaseFactory = None
    project_: str = None

    def parse_file_ext(self, file_ext: str, project: str):
        """Parse the file ext type"""
        # pass the project name to the
        self.project_ = project

        # create an instance of the BaseFactory class
        # NOTE: base_factory contains all project path information
        base_factory = BaseFactory(project=project)
        self.base_factory_ = base_factory
        print(f"Base Factory: {base_factory}")

        if file_ext in self.file_extensions:
            self.file_ext = file_ext

            if file_ext == 'pdf':
                print(f"PDF PARSER INITIALIZED")
                # set the current file parser
                parser = globals()[f'{file_ext.title()}Parser'](project=project)
                self.current_parser_obj = parser
                parser_generator = FileGenerator(project='personal_umbrella', file_ext=file_ext)
                parser_iterator = parser_generator.__iter__()
                #pprint(f"Parser Vars: {vars(parser.pdf_ocr)}")
                #print(f"PDF IMAGE PATH: {vars(self.base_factory_)}")
                #pprint(f"Parser Generator: {vars(parser_generator)}")

                # NOTE: Temporary counter added for testing
                temp_counter = 0
                #
                # begin file iteration
                while temp_counter < 10:
                    print(f"TEMP COUNTER: {temp_counter}")
                    try:
                        current_file = next(parser_iterator)
                        parser.extract_text(current_file)
                        #parser.extract_images(current_file)
                        #parser.extract_text_from_img(parser.pdf_ocr.current_pdf_dir)
                        temp_counter += 1
                    except StopIteration:
                        print(f'Finished parsing {file_ext} files.')
                        break

                # load the file ext dictionary
                ParserFactory.file_parser = parser.mapping_dict
                pprint(ParserFactory.file_parser)

            if file_ext == 'doc':
                # set the current file parser
                parser = globals()[f'{file_ext.title()}Parser'](project=project)
                self.current_parser_obj = parser
                # execute the R script that extracts text from the doc files
                parser.run_doc_to_csv_rscript(base_factory.__dict__['raw_data'], str(20))


                parser_generator = FileGenerator(project='personal_umbrella', file_ext=file_ext)
                parser_iterator = parser_generator.__iter__()


            else:
                # set the current file parser
                parser = globals()[f'{file_ext.title()}Parser'](project=project)
                self.current_parser_obj = parser
                #print(f"Parser Name: {parser.__class__}")
                #print(f"Raw Data: { self.base_factory_.__dict__['raw_data']}")

                parser_generator = FileGenerator(project='personal_umbrella', file_ext=file_ext)
                parser_iterator = parser_generator.__iter__()

               # begin iteration
                while True:
                    try:
                        parser.extract_text(next(parser_iterator))
                    except StopIteration:
                        print(f'Finished parsing {file_ext} files.')
                        break

                # view the contents of the mapping dictionary
                # load the file ext dictionary
                self.file_parser = parser.mapping_dict


    def serialize_contents(self, write_path: str, file_ext: str, protocol: int):
        """Write the mapping dictionary to a pickle file."""
        self.file_ext = file_ext
        pkl_name = self.file_ext.title() + '_' + d + '.pickle'
        try:
            #os.chdir(self.base_factory_.__dict__['pickle_path'])
            os.chdir(write_path)
            pickle.dump(self.file_parser, open(pkl_name, 'wb'), protocol=protocol)
        except pickle.PickleError as e:
            print(f"PickleError: A pickling error has occurred while writing: {pkl_name}")
            print(e)

    def sftp_file(self, file_type: str):
        """Push the pickled mapping file[dict] to the remote server."""
        try:
            print(f"self.project_: {self.project_}")
            print(f"self.file_ext: {self.file_ext}")
            print(f"self.file_type: {file_type}")
            sftp = SftpData(project=self.project_, file_ext=self.file_ext, file_type=file_type)
            sftp.connect()
            #print(sftp.file_type, sftp.file_ext, sftp.username, sftp.password, sftp.host)
            #print(f"stfp.__dict__['pickle_path']: {sftp.__dict__['pickle_path']}")
            #print(f"sftp.__dict__['pickle_path']: {sftp.__dict__['remote_paths']['remote_pickle_path']}")
        except Exception as e:
            print(e)
