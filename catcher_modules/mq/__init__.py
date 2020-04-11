import logging
from os.path import join

from catcher.utils.file_utils import read_file
from catcher.utils.misc import fill_template_str

logging.getLogger("pykafka").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)


class MqStepMixin:

    @staticmethod
    def form_body(message, file, variables):
        data = message
        if data is None:
            if file is None:
                raise ValueError('Either data or data_from_file must be set.')
            data = read_file(join(variables['RESOURCES_DIR'], fill_template_str(file, variables)))
        return fill_template_str(data, variables)
