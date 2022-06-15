# -*- coding: utf-8 -*-
from python_service_framework.frameworks.route_manager import RouteManager
from python_service_framework.frameworks import ContextManager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Runner(SimplexBaseModel):
    def __init__(self, *args, **kwargs):
        super(Runner, self).__init__(*args, **kwargs)
        self.route = RouteManager.select_router('test.json', file_type='oss', project_root='.')

    def predict(self, data, **kwargs):
        logger.info('### runner.predicting ###')

        print("{}, {}, {}".format("### CHECK-DATA ###", data, type(data)))
        context = ContextManager()
        context.add_context('udf_string', ' - 自定义的字符串')
        if not data:
            return
        outputs = self.route.process(data, context)
        return outputs


if __name__ == '__main__':
    model = Runner()
    result = model.predict(['1010111', 'simple'])
    print(result)
