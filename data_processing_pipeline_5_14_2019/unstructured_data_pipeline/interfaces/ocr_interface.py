"""
ocr_interface
~~~~~~~~~~~~~
"""
import abc


class OCRInterface(metaclass=abc.ABCMeta):
    """An abstract interface for optical character recognition"""

    @abc.abstractmethod
    def extract_pdf_image(self, file_path: str):
        """load in the current file"""
        raise NotImplementedError

    @abc.abstractmethod
    def ocr_image_file(self, img_file_path: str):
        """ocr an image file"""
        raise NotImplementedError
