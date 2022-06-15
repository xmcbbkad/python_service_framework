# -*- coding: utf-8 -*-


class Config:
    CONFIG_FILE_SOURCE_OSS = 'oss'
    CONFIG_FILE_SOURCE_LOCAL = 'local'
    CONFIG_FILE_SOURCE_CENTER = 'center'

    OSS_KEY = ''
    OSS_SECRET = ''
    OSS_ENDPOINT = ''
    OSS_BUCKET = ''

    IO_TYPE_SINGLE = 'single'
    IO_TYPE_LIST = 'list'

    DIRECT_RETURN_CLASS_NAME = 'r'


from python_service_framework.frameworks.route_manager import RouteManager
from python_service_framework.frameworks.processor import Processor
from python_service_framework.frameworks.context_manager import ContextManager
from python_service_framework.frameworks.processor_chains import ProcessorChains
