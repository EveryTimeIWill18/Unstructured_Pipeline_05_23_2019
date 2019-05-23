"""
logging_interface
~~~~~~~~~~~~~~~~~
"""
import abc

# Logger Interface #
####################################################################################################
class PipelineLoggerInterface(metaclass=abc.ABCMeta):
    """A logs interface for the data pipeline logs classes"""

    @abc.abstractmethod
    def config(self, *args, **kwargs):
        """logger configuration"""
        pass

    @abc.abstractmethod
    def debug(self, **kwargs):
        """logs.debug method"""
        pass

    @abc.abstractmethod
    def info(self, **kwargs):
        """logs.info method"""
        pass

    @abc.abstractmethod
    def warning(self, **kwargs):
        """logs.warning method"""
        pass

    @abc.abstractmethod
    def error(self, **kwargs):
        """logs.error method"""
        pass

    @abc.abstractmethod
    def critical(self, **kwargs):
        """logs.critical method"""
        pass
