# -*- coding: utf-8 -*-
from python_service_framework.frameworks.context_manager import ContextManager
import logging
logger = logging.getLogger(__name__)


class Processor(object):
    """服务处理逻辑的基本单元

    本服务框架中所有的基本逻辑都要继承此类
    """

    def __init__(self, name, **kwargs):
        """初始化类变量"""
        self.name = name
        logger.info("构建base:{}插件...".format(self.__class__.__name__))

    def preprocess(self, inputs: list) -> list:
        '''
            在process之前要调用的方法
            :param inputs 输入的数据
            :return: inputs 返回的数据
        '''
        return inputs

    def process(self, inputs: list, context: ContextManager = None) -> (list, str):
        '''
        逻辑处理的基本单位
        :param inputs 输入的数据
        :param context 执行的上下文
        :return: outputs 返回的数据
        :return: self.name 方法对应类的名称
        '''
        inputs = self.preprocess(inputs)
        outputs = inputs
        return outputs, self.name

