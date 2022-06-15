# -*- coding: utf-8 -*-
from python_service_framework.frameworks.processor import Processor


class QueryProcessor(Processor):
    def __init__(self, name, **kwargs):
        super(QueryProcessor, self).__init__(name, **kwargs)

    def process(self, inputs, context=None):
        # 对query不做什么事情，要修改则继承该类重写process方法
        outputs = inputs
        return outputs, self.name
