"""
concrete_ocr
~~~~~~~~~~~~
A concrete implementation of the OCRInterface.
"""
import io
import os
import time
import string
import struct
import PyPDF2
import pytesseract
from PIL import Image
from pprint import pprint
from wand.image import Image as wand_img
from unstructured_data_pipeline.file_encoders import (SPACES, WHITESPACE,
                                                      PUNCTUATION, specialchars,
                                                      NEWLINE, TABS)
from unstructured_data_pipeline.interfaces.ocr_interface import OCRInterface
from unstructured_data_pipeline.configs.config_sa_claims import sa_config
from unstructured_data_pipeline.configs.config_dms_claims import dms_config
from unstructured_data_pipeline.configs.config_personal_umbrella import pu_config

# path to tesseract executable
pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files\\Tesseract-OCR\\tesseract'



class PdfOCR(OCRInterface):
    """OCR pdf documents"""

    def __init__(self, project_name):
        self.extracted_pdfs: list = []
        self.pdf_contents: list  = []
        self.CCITT_group = 4
        self.is_tiff_img: bool = False
        self.img_format: str = 'tiff'
        self.current_pdf_dir: str = None
        self.pdf_img_content: list = []

        if project_name == 'personal_umbrella':
            self.__dict__['project'] = project_name
            self.__dict__['pdf_img_path'] = pu_config.get_write_paths['pdf_img']
        elif project_name == 'sa_claims':
            self.__dict__['project'] = project_name
            self.__dict__['pdf_img_path'] = sa_config.get_write_paths['pdf_img']
        elif project_name == 'dms_claims':
            self.__dict__['project'] = project_name
            self.__dict__['pdf_img_path'] = dms_config.get_write_paths['pdf_img']
        else:
            raise ValueError(f'ValueError: Project, {project_name} is not a valid project name.')

    def tiff_header_CCITT(self, width, height, img_size, CCITT_group=4):
        """
        Creates a header for extracting images from .tiff pdf image file format.
        """
        tiff_header_struct = '<' + '2s' + 'h' + 'l' + 'h' + 'hhll' * 8 + 'h'
        self.CCITT_group = CCITT_group

        return struct.pack(tiff_header_struct,  b'II',  # Byte order indication: Little indian
                       42,  # Version number (always 42)
                       8,  # Offset to first IFD
                       8,  # Number of tags in IFD
                       256, 4, 1, width,  # ImageWidth, LONG, 1, width
                       257, 4, 1, height,  # ImageLength, LONG, 1, length
                       258, 3, 1, 1,  # BitsPerSample, SHORT, 1, 1
                       259, 3, 1, self.CCITT_group,  # Compression, SHORT, 1, 4 = CCITT Group 4 fax encoding
                       262, 3, 1, 0,  # Threshholding, SHORT, 1, 0 = WhiteIsZero
                       273, 4, 1, struct.calcsize(tiff_header_struct),  # StripOffsets, LONG, 1, len of header
                       278, 4, 1, height,  # RowsPerStrip, LONG, 1, length
                       279, 4, 1, img_size,  # StripByteCounts, LONG, 1, size of image
                       0  # last IFD
        )


    def extract_pdf_image(self, full_file_name: str):
        """Extract image files from the current pdf."""
        try:
            if os.path.isfile(full_file_name):
                # open the current pdf
                pdf_reader = PyPDF2.PdfFileReader(
                    open(full_file_name, 'rb')
                )
                print(f'Current Pdf: {full_file_name}')
                # get the number of pages
                num_pages = pdf_reader.getNumPages()
                # create a dictionary for the current pdf
                current_pdf = {}
                # iterate through each page and extract the pdf's contents
                n = 0
                while n < num_pages:
                    try:
                        # get the current page
                        page = pdf_reader.getPage(n)
                        # get the xObject
                        xObject = page['/Resources']['/XObject'].getObject()
                        #text = page.extractText()
                        #print(f'Text size: {len(text)}')
                        # sub page counter
                        m = 0
                        for obj in xObject:
                            # if current object is an image
                            if xObject[obj]['/Subtype'] == '/Image':
                                size = (xObject[obj]['/Width'], xObject[obj]['/Height'])
                                data = xObject[obj]._data
                                if xObject[obj]['/ColorSpace'] == '/DeviceRGB':
                                    mode = "RGB"
                                else:
                                    mode = "P"

                            # NOTE: extract .tiff images
                            if xObject[obj]['/Filter'] == '/CCITTFaxDecode':
                                # .tiff
                                # create a directory for the image

                                # set the image format
                                self.img_format = 'tiff'

                                pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                                if not os.path.exists(os.path.join(self.__dict__['pdf_img_path'], pdf_name)):
                                    # create a directory for the current pdf
                                    new_dir = os.path.join(self.__dict__['pdf_img_path'], pdf_name)
                                    self.current_pdf_dir = new_dir
                                    os.mkdir(new_dir)
                                    time.sleep(4)

                                # NOTE: using the tiff struct method
                                if xObject[obj]['/DecodeParms']['/K'] == -1:
                                    self.CCITT_group = 4
                                else:
                                    self.CCITT_group = 3

                                width = xObject[obj]['/Width']
                                height = xObject[obj]['/Height']
                                data = xObject[obj]._data
                                img_size = len(data)
                                tiff_header = self.tiff_header_CCITT(
                                    width=width, height=height, img_size=img_size, CCITT_group=self.CCITT_group
                                )
                                # save the image file
                                img_name = f'ImgFilePage{n}_{m}.tiff'
                                with open(os.path.join(new_dir, img_name), 'wb') as img_file:
                                    img_file.write(tiff_header + data)
                                m += 1


                            # NOTE: extract .png images
                            elif xObject[obj]['/Filter'] == '/FlateDecode':
                                # .png

                                # set the image format
                                self.img_format = 'png'

                                # create a directory for the image
                                pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                                if not os.path.exists(os.path.join(self.__dict__['pdf_img_path'], pdf_name)):
                                    # create a directory for the current pdf
                                    new_dir = os.path.join(self.__dict__['pdf_img_path'], pdf_name)
                                    self.current_pdf_dir = new_dir
                                    os.mkdir(new_dir)
                                    time.sleep(4)
                                # save the image file
                                img = Image.frombytes(mode, size, data)
                                img.save(os.path.join(new_dir, f'ImgFilePage{n}_{m}.png'))
                                m += 1

                            # NOTE: extract .jpg images
                            elif xObject[obj]['/Filter'] == '/DCTDecode':
                                # .jpg

                                # set the image format
                                self.img_format = 'jpg'

                                # create a directory for the image
                                pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                                if not os.path.exists(os.path.join(self.__dict__['pdf_img_path'], pdf_name)):
                                    # create a directory for the current pdf
                                    new_dir = os.path.join(self.__dict__['pdf_img_path'], pdf_name)
                                    self.current_pdf_dir = new_dir
                                    os.mkdir(new_dir)
                                    time.sleep(4)

                                # save the image file
                                img = open(os.path.join(new_dir, f'ImgFilePage{n}_{m}.jpg'), "wb")
                                img.write(data)
                                img.close()
                                m += 1

                            # NOTE: extract .jp2 images
                            elif xObject[obj]['/Filter'] == '/JPXDecode':
                                # .jp2

                                # set the image format
                                self.img_format = 'jp2'

                                # create a directory for the image
                                pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                                if not os.path.exists(os.path.join(self.__dict__['pdf_img_path'], pdf_name)):
                                    # create a directory for the current pdf
                                    new_dir = os.path.join(self.__dict__['pdf_img_path'], pdf_name)
                                    self.current_pdf_dir = new_dir
                                    os.mkdir(new_dir)
                                    time.sleep(4)

                                # save the image file
                                img = open(os.path.join(new_dir, f'ImgFilePage{n}_{m}.jp2'), "wb")
                                img.write(data)
                                img.close()
                                m += 1

                            # NOTE: extract image from bytes
                            else:
                                # image from bytes
                                print(f'Pdf: {full_file_name} has no images on page: {n}')
                                m += 1
                        # increment the page counter
                        n += 1
                    except Exception as e:
                        print(f'An error occurred extracting text from page: {n}')
                        print(e)
                        n += 1
        except OSError as e:
            print(f'OSError: An error occurred while trying to extract images from pdf: {full_file_name}')


    def ocr_image_file(self, img_file_path: str):
        try:
            if os.path.isdir(img_file_path):
                for f in os.listdir(img_file_path):
                    self.pdf_img_content.append(
                        os.path.splitext(os.path.split(img_file_path)[1])[0]
                    )
                    pdf_file_number = os.path.splitext(f)[0][-3]
                    print(f"PDF FILE NUMBER: {pdf_file_number}")
                    try:
                        if self.img_format is 'tiff':
                            tiff_img = os.path.join(img_file_path, f)
                            img_ = Image.open(tiff_img).convert("RGBA")

                            # extract and clean the text
                            text = pytesseract.image_to_string(img_, lang='eng')
                            text = ''.join(text) \
                                .translate(str.maketrans('', '', string.punctuation)) \
                                .lower()
                            text = SPACES.sub(" ", text)
                            text = NEWLINE.sub("", text)
                            text = TABS.sub("", text)

                            # attempt to remove special characters
                            text = ''.join([i if ord(i) < 128 else ' ' for i in text])

                            # update pdf dictionaries
                            self.extracted_pdfs.append(text)
                            self.pdf_img_content.append(text)

                        else:
                            img_ = wand_img(filename=os.path.join(img_file_path, f), resolution=300)
                            wnd_img = wand_img(image=img_).make_blob(
                                os.path.splitext(f)[-1].lstrip('.')
                            )
                            im = Image.open(io.BytesIO(wnd_img))

                            # extract and clean the text
                            text = pytesseract.image_to_string(im, lang='eng')
                            text = ' '.join(text) \
                                .translate(str.maketrans('', '', string.punctuation)) \
                                .lower()
                            text = SPACES.sub("", text)
                            text = NEWLINE.sub("", text)
                            text = TABS.sub("", text)

                            # attempt to remove special characters
                            text = ''.join([i if ord(i) < 128 else ' ' for i in text])

                            # update pdf dictionaries
                            self.extracted_pdfs.append(text)
                            self.pdf_img_content.append(text)
                    except:
                        print(f'An error occurred while ocring image: {f}')
            else:
                raise OSError(f'OSError: Path, {img_file_path} not found.')
        except OSError as e:
            print(e)
