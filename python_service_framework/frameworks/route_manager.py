# -*- coding: utf-8 -*-
import os
import sys
from python_service_framework.frameworks.processor_chains import ProcessorChains
import logging
logger = logging.getLogger(__name__)


class RouteManager(object):
    def __init__(self):
        pass

    @staticmethod
    def select_router(config_path, file_type='local', project_root='.'):
        if project_root not in list(sys.path):
            sys.path.append(project_root)
            logger.info("WORK DIRCTORY：{}".format(sys.path))
        route = ProcessorChains(config_path, file_type=file_type)
        return route


if __name__ == '__main__':
    route = RouteManager.select_router('../test.json')
    obj = route.process(['00001000001'])
    print(obj)
