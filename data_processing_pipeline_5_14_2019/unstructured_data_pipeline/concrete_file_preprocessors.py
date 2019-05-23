"""
concrete_file_preprocessors
~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
import os
import stat
import string
import shutil
import zipfile
import subprocess
from email import policy
from pprint import pprint
from bs4 import BeautifulSoup
from datetime import datetime
from PyPDF2 import PdfFileReader
from collections import OrderedDict
from email.parser import BytesParser
from xml.etree.cElementTree import XML
from unstructured_data_pipeline import FileGeneratorInterface
from unstructured_data_pipeline import FileParserInterface
from unstructured_data_pipeline.logs.logging_config import BaseLogger
from unstructured_data_pipeline.configs.config_dms_claims import dms_config
from unstructured_data_pipeline.configs.config_personal_umbrella import pu_config
from unstructured_data_pipeline.configs.config_sa_claims import sa_config
from unstructured_data_pipeline.concrete_ocr import PdfOCR
from unstructured_data_pipeline.file_encoders import (SPACES, PUNCTUATION,
                                                      RTF_ENCODING,
                                                      specialchars, destinations,
                                                      NEWLINE, BARS, TABS)

# Global Variables #
####################################################################################################
# date configuration
d = str(datetime.today())[:10].replace("-","_")

# logs configuration
logfile = "data_pipeline_log_" + d
logger = BaseLogger(dms_config.get_log_path, logfile)


# Concrete Parsers #
####################################################################################################
class BaseParser(object):
    """Base Parser"""

    def __init__(self, project: str):
        if project == 'personal_umbrella':
            self.__dict__['project'] = 'personal_umbrella'
            self.__dict__['raw_data'] = pu_config.get_raw_data
        if project == 'sc_claims':
            self.__dict__['project'] = 'sa_claims'
            self.__dict__['raw_data'] = sa_config.get_raw_data
        if project == 'dms_claims':
            self.__dict__['project'] = 'dms_claims'
            self.__dict__['raw_data'] = dms_config.get_raw_data


class FileGenerator(FileGeneratorInterface, BaseParser):
    """Concrete Implementation"""

    def __init__(self, project: str, file_ext: str):
        BaseParser.__init__(self, project)
        self.project = project
        self.file_ext = file_ext

    def __iter__(self):
        try:
            if os.path.isdir(self.__dict__['raw_data']):
                for name in os.scandir(self.__dict__['raw_data']):
                    if os.path.splitext(os.path.basename(name))[-1] == '.'+self.file_ext:
                        yield os.path.join(self.__dict__['raw_data'], os.path.basename(name))
        except StopIteration:
            print("Finished processing files")


class EmlParser(FileParserInterface, BaseParser):
    """Eml File Parser"""

    def __init__(self, project):
        BaseParser.__init__(self, project=project)
        self.project = project
        self.mapping_dict: dict = {}        # {(object_id, claim_id, filename, previous_filename): raw_text}

        # file counters
        self.file_counter: int = 0          # count of the files successfully parsed
        self.error_file_counter: int = 0    # count of files that raised errors
        self.error_files: list = []

        # logs configuration
        #logger.info(info='starting eml parsing')

    def extract_text(self, current_file) -> dict:
        """Extract the current email's text"""
        try:
            with open(current_file, 'rb') as eml_f:
                msg = BytesParser(policy=policy.default).parse(eml_f)
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == 'text/html':
                            soup = BeautifulSoup(part.get_content(), 'html.parser')
                            body = soup.findAll(text=True)  # extract the text

                            # process the text list into a formatted string
                            body = ' '.join(body) \
                                .translate(str.maketrans('', '', string.punctuation)) \
                                .lower()
                            body = SPACES.sub(" ", body)
                            body = NEWLINE.sub("", body)
                            body = TABS.sub(" ", body)
                            body = ''.join([i if ord(i) < 128 else ' ' for i in body])
                            #NOTE: update for dms_claims project (5/17/19)
                            if self.project == 'dms_claims':
                                self.mapping_dict.update({})
                            #NOTE: END//

                            self.mapping_dict.update({os.path.basename(current_file): body})
                            self.file_counter += 1
                            return {os.path.basename(current_file): body}
        except OSError as e:
            if current_file in self.error_files:
                pass
            else:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
                #logger.error(error=f'OSError: Could not parse email: {os.path.basename(current_file)}')
                #logger.error(error=f"Python Exception: {e}") # added: 5/1/2019
        except Exception as e:  # added: 5/1/2019
            if current_file in self.error_files:
                pass
            else:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))
                #logger.error(error=f"Python Exception: {e}")


class RtfParser(FileParserInterface):
    """Rtf File Parser"""
    def __init__(self):
        self.mapping_dict: dict = {}

        # file counters
        self.file_counter: int = 0  # count of the files successfully parsed
        self.error_file_counter: int = 0  # count of files that raised errors
        self.error_files: list = []

    def extract_text(self, current_file) -> dict:
        """Extract the current rtf file's text"""
        try:
            with open(current_file, 'rb') as f:
                text = f.read().decode('utf-8')
                stack = []
                ignorable = False
                ucskip = 1
                curskip = 0
                out = []  # Output buffer.
                for match in RTF_ENCODING.finditer(text):
                    word, arg, hex, char, brace, tchar = match.groups()
                    if brace:
                        curskip = 0
                        if brace == '{':
                            # Push state
                            stack.append((ucskip, ignorable))
                        elif brace == '}':
                            # Pop state
                            ucskip, ignorable = stack.pop()
                    elif char:  # \x (not a letter)
                        curskip = 0
                        if char == '~':
                            if not ignorable:
                                out.append('\xA0')
                        elif char in '{}\\':
                            if not ignorable:
                                out.append(char)
                        elif char == '*':
                            ignorable = True
                    elif word:  # \foo
                        curskip = 0
                        if word in destinations:
                            ignorable = True
                        elif ignorable:
                            pass
                        elif word in specialchars:
                            out.append(specialchars[word])
                        elif word == 'uc':
                            ucskip = int(arg)
                        elif word == 'u':
                            c = int(arg)
                            if c < 0: c += 0x10000
                            if c > 127:
                                out.append(chr(c))  # NOQA
                            else:
                                out.append(chr(c))
                            curskip = ucskip
                    elif hex:  # \'xx
                        if curskip > 0:
                            curskip -= 1
                        elif not ignorable:
                            c = int(hex, 16)
                            if c > 127:
                                out.append(chr(c))  # NOQA
                            else:
                                out.append(chr(c))
                    elif tchar:
                        if curskip > 0:
                            curskip -= 1
                        elif not ignorable:
                            out.append(tchar)
                    result = ''.join(out)
                result = ''.join(ch for ch in result if ch not in PUNCTUATION)
                result = SPACES.sub(" ", result)
                result = ''.join(result)

                # update self.fileDict
                self.mapping_dict.update({os.path.basename(current_file): result.lower()})
                self.file_counter += 1
                return {os.path.basename(current_file): result.lower()}
        except OSError as e:
            self.file_counter += 1
            self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
            #logger.error(error=f"OSError: Could not parse email: {os.path.basename(current_file)}")
            #logger.error(error=f"Python Exception: {e}")


class DocxParser(FileParserInterface, BaseParser):
    """Docx File Parser"""

    def __init__(self, project: str):
        BaseParser.__init__(self, project=project)
        self.project = project
        self.mapping_dict: dict = {}

        # file counters
        self.file_counter: int = 0  # count of the files successfully parsed
        self.error_file_counter: int = 0  # count of files that raised errors
        self.error_files: list = []

        # logging configuration
        #logger.info(info='starting docx parsing')

    def extract_text(self, current_file):
        """Extract the current docx file's text"""
        try:
            WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
            PARA = WORD_NAMESPACE + 'p'
            TEXT = WORD_NAMESPACE + 't'

            # unzip the current document and read its contents
            if os.path.getsize(current_file) > 0:
                document = zipfile.ZipFile(current_file)
                for i, _ in enumerate(document.infolist()):
                    if document.infolist()[i].filename == 'word/document.xml':
                        xml_content = document.read('word/document.xml')
                        document.close()
                        tree = XML(xml_content)  # parse the xml document
                        paragraphs = []
                        for paragraph in tree.getiterator(PARA):
                            texts = [
                                node.text
                                for node in paragraph.getiterator(TEXT)
                                if node.text
                            ]
                            if texts:
                                # process the text list into a formatted string
                                texts = ' '.join(texts) \
                                    .translate(str.maketrans('', '', string.punctuation)) \
                                    .lower()
                                # NOTE: added (5/17/2019)
                                texts = SPACES.sub(" ", texts)
                                texts = texts[6:]
                                texts = BARS.sub("", texts)
                                texts = NEWLINE.sub(" ", texts)
                                texts = TABS.sub(" ", texts)
                                # attempt to remove special characters
                                texts = ''.join([i if ord(i) < 128 else ' ' for i in texts])
                                paragraphs.append(texts)
                                # NOTE: END//
                        #pprint(f"PARAGRAPHS TEXT: {''.join(paragraphs)}")
                        self.mapping_dict.update({os.path.basename(current_file): ''.join(paragraphs)})
                        # increment the file counter
                        self.file_counter += 1
                        return {os.path.basename(current_file): ''.join(paragraphs)}
                else:
                    pass
            else:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
                #logger.error(error=f'File: {current_file} is empty')
                #logger.error(error=f'File: {current_file} is empty')    # added: 5/1/2019
        except OSError as e:
            self.error_file_counter += 1
            self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
            #logger.error(error=f'OSError: Could not parse email: {os.path.basename(current_file)}')
            #logger.error(error=f"Python Exception: {e}")
        except Exception as e:
            self.error_file_counter += 1
            self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
            #logger.error(error=f"File: {current_file} raised an error")
            #logger.error(error=f"Python Exception: {e}")    # added: 5/1/2019


class DocParser(FileParserInterface):
    """Doc File Parser"""

    def __init__(self):
        self.mapping_dict: dict = {}

        # file counters
        self.file_counter: int = 0  # count of the files successfully parsed
        self.error_file_counter: int = 0  # count of files that raised errors
        self.error_files: list = []

        # logging configuration
        logger.info(info='starting txt(doc) parsing')


    def run_doc_to_csv_rscript(self, file_path, timeout):
        """run the R script that converts .doc to .csv"""
        filepath = '"' + file_path + '"'
        time_out = '"' + timeout + '"'

        # run the R script
        subprocess.call([dms_config.get_r_files['r_executable'],
                         os.path.join(dms_config.get_r_files['r_path'],
                                      dms_config.get_r_files['r_script_three']),
                         filepath, time_out], shell=True)

    def extract_text(self, current_file: str):
        """extract the contents from the converted .doc files"""
        try:
            with open(current_file, 'r', errors='replace', encoding='utf-8') as f:
                text = f.read()
                text = SPACES.sub(" ", text)
                text = text[6:]
                text = BARS.sub("", text)
                text = NEWLINE.sub(" ", text)
                text = TABS.sub(" ", text)
                text = text.translate(str.maketrans('', '', string.punctuation)).lower()

                # attempt to remove special characters
                text = ''.join([i if ord(i) < 128 else ' ' for i in text]) # added 5/9/2019

                logger.info(info=f'Current file basename: {os.path.basename(current_file)}')
                self.mapping_dict.update({os.path.basename(current_file): text})
                self.file_counter += 1
                return {os.path.basename(current_file): text}
        except OSError:
            if current_file not in self.error_files:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
                logger.error(error=f"OSError: Could not parse doc: {os.path.basename(current_file)}")

class PdfParser(FileParserInterface, BaseParser):
    """Parser for pdfs"""
    def __init__(self, project):
        BaseParser.__init__(self, project)
        self.project = project
        self.mapping_dict: dict = {}
        self.pdf_content_by_page: list[OrderedDict[any]] = []
        self.pdf_by_page_counter: int = 0

        self.pdf_ocr = PdfOCR(project_name=self.project)

        # file counters
        self.file_counter: int = 0  # count of the files successfully parsed
        self.error_file_counter: int = 0  # count of files that raised errors
        self.error_files: list = []

        # current pdf pointers
        self.page_counter: int = 0
        self.pdf_page_count: int = 0
        self.pdf_counter: int = 0
        self.current_pdf: str = None

    def extract_text(self, current_file):
        pprint(f'current pdf file: {current_file}')
        try:
            # update the current pdf
            self.current_pdf = current_file

            # update the temporary OrderedDict
            d = OrderedDict()
            d['filename'] = os.path.splitext(os.path.split(self.current_pdf)[1])[0]

            with open(current_file, 'rb') as f:
                pdf_reader = PdfFileReader(f)
                pages = []
                pg = 0
                while pg < pdf_reader.numPages:
                    try:
                        current_page = pdf_reader.getPage(pg)
                        # extract the contents of the pdf
                        text = list(str(current_page.extractText()).splitlines())
                        text = ''.join(text)

                       # NOTE: added (5/17/2019)
                        if len(text) > 0:
                            text = SPACES.sub(" ", text)
                            text = text[6:]
                            text = BARS.sub("", text)
                            text = NEWLINE.sub(" ", text)
                            text = TABS.sub(" ", text)
                            text = text.translate(str.maketrans('', '', string.punctuation)).lower()
                            # attempt to remove special characters
                            text = ''.join([i if ord(i) < 128 else ' ' for i in text])
                            pprint(f"Extracted Text: {text}")
                        d[pg] = text
                        pg += 1
                    except:
                        print(f'An error has occurred extracting text from page, {pg} of file {current_file}.\n\n')
                        pg += 1

                # increment the pdf counter
                self.pdf_content_by_page.append(d)
                self.pdf_counter += 1
        except:
            print(f'Pdf: {current_file}, could not be read.\n\n')

    def extract_images(self, file_path: str):
        """Extract pdf images into new directories"""
        # extract pdf image files
        self.pdf_ocr.extract_pdf_image(full_file_name=file_path)

    def extract_text_from_img(self, file_path: str):
        """Extract text from the pdf images"""
        try:
            # get a list of pdf directories that have extracted images
            img_dirs = os.listdir(self.pdf_ocr.__dict__['pdf_img_path'])
            print(f"Image Dirs: {img_dirs}")
            self.pdf_ocr.ocr_image_file(img_file_path=file_path)
        except Exception as e:
            print("PdfImageExtractionError: An error was raised during pdf text mining.")
            print(e)
        try:
            # BLOCK: Load extracted pdf image text into the mapping[dict]
            for od in self.pdf_content_by_page:
                for i, k in enumerate(od):
                    if len(self.pdf_ocr.pdf_img_content[i]) != 0:
                        # insert the extracted image text into the od
                        od[k] = self.pdf_ocr.pdf_img_content[i]

            # load the extracted text into the mapping_dict[dict]
            for od in self.pdf_content_by_page:
                self.mapping_dict[os.path.split(self.current_pdf)[1]] = ''.join(
                    list(od.values())[1:]
                )
            # BLOCK: Remove the temporary pdf image directory
            rm_attempts = 0
            while rm_attempts < 5:
                # check that you have delete access
                if os.access(file_path, os.W_OK) and os.access(file_path, stat.S_IWGRP):
                    # delete the image directory
                    shutil.rmtree(file_path)
                    break
                else:
                    # if write access is False
                    os.chmod(file_path, stat.S_IRWXU| stat.S_IRWXG| stat.S_IRWXO)
                    rm_attempts += 1 # increment rm_attempts
            else:
                # number of remove attempts exceeded
                dir_name = os.path.splitext(os.path.split(file_path)[1])[0]
                print(f"WARNING: Pdf Image Directory: {dir_name} could not be deleted!")
        except OSError as e:
            print("OSError: An error was raised during directory deletion.")
            print(e)
