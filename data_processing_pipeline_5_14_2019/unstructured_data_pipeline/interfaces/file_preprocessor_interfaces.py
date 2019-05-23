"""
file_preprocessor_interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
import abc
from pprint import pprint

# Interfaces #
####################################################################################################
class FileParserInterface(object, metaclass=abc.ABCMeta):
    """File Parser Interface"""

    @abc.abstractmethod
    def extract_text(self, current_file):
        """Extract text from the current file."""
        raise NotImplementedError



class FileGeneratorInterface(object, metaclass=type):
    """File Generator Interface"""

    @abc.abstractmethod
    def __iter__(self):
        """iterator dunder method must be overridden"""
        raise NotImplementedError



