# -*- coding: utf-8 -*-
from python_service_framework.frameworks.processor import Processor
import logging
logger = logging.getLogger(__name__)


class TestProcessor(Processor):
    def __init__(self, name, **kwargs):
        super(TestProcessor, self).__init__(name, **kwargs)
        for item in kwargs:
            logger.info("{} : {}".format(item, kwargs[item]))

    def process(self, inputs, context=None):
        outputs = []
        for item in inputs:
            output = item + context.get_context('udf_string', 'service')
            outputs.append(output)

        return outputs, self.name
