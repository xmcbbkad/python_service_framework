# -*- coding: utf-8 -*-
from python_service_framework.frameworks.processor import Processor


class BaseRankerProcessor(Processor):
    def __init__(self, name, **kwargs):
        super(BaseRankerProcessor, self).__init__(name, **kwargs)

    def process(self, inputs, context=None):
        raise NotImplementedError(
            'Subclasses should implement the predict func')


class PreRankProcessor(BaseRankerProcessor):
    def __init__(self, name, **kwargs):
        super(PreRankProcessor, self).__init__(name, **kwargs)

    def process(self, inputs: list, context=None) -> (list, str):
        return_list = []
        for data in inputs:
            data = self.datapreprocess(data)
            data = self.roughrank(data)
            data = self.dedup(data)
            return_list.append(data)

        return return_list, self.name

    def datapreprocess(self, data):
        """ 对召回结果进行数据的预处理，包括填值，预去重，过滤，字段生成等

        Arguments:
            data: es查询返回的字典类型.

        Returns:
            data: 经过预处理的es查询返回的字典.

        Raises:
            InvalidArgumentError
        """
        raise NotImplementedError(
            'Subclasses should implement the predict func')

    def roughrank(self, data):
        """ 对召回结果进行粗排序

        Arguments:
            data: es查询返回的字典类型.

        Returns:
            data: 经过粗排序的es查询返回的字典.

        Raises:
            InvalidArgumentError
        """
        raise NotImplementedError(
            'Subclasses should implement the predict func')

    def dedup(self, data):
        """ 对召回结果进行去重

        Arguments:
            data: es查询返回的字典类型.

        Returns:
            data: 经过去重的es查询返回的字典.

        Raises:
            InvalidArgumentError
        """
        raise NotImplementedError(
            'Subclasses should implement the predict func')


class RankProcessor(BaseRankerProcessor):
    def __init__(self, name, **kwargs):
        super(RankProcessor, self).__init__(name, **kwargs)
        self.model_path = kwargs.get('model_path', {})
        self.model_rank_num = kwargs.get('model_rank_num', {})

        self.model_rank_num = int(self.model_rank_num) if isinstance(self.model_rank_num, str) else self.model_rank_num
        assert self.model_rank_num != {}, "无法获取配置文件数据"
        assert self.model_path != {}, "无法获取配置文件数据"

    def process(self, inputs, context=None):
        self.load_model(self.model_path)
        return_list = []
        for data in inputs:
            data = self.rank(data)
            return_list.append(data)
        return return_list, self.name

    def load_model(self, model_path):
        """ 载入排序模型

        Arguments:
            model_path: 排序模型的存储地址.

        Returns:
            model: 返回载入的模型.

        Raises:
            InvalidArgumentError
        """
        raise NotImplementedError(
            'Subclasses should implement the predict func')

    def generate_rank_list(self):
        pass

    def rank(self, data):
        """ 从query生成符合es的查询体

        Arguments:
            data: es查询返回的字典类型.

        Returns:
            data: 经过预处理的es查询返回的字典.

        Raises:
            InvalidArgumentError
        """
        raise NotImplementedError(
            'Subclasses should implement the predict func')


class PostRankProcessor(BaseRankerProcessor):
    def __init__(self, name, **kwargs):
        super(PostRankProcessor, self).__init__(name, **kwargs)


    def process(self, inputs: list, context=None) -> (list, str):
        return_list = []
        for data in inputs:
            data = self.postrank(data)
            return_list.append(data)
        return return_list, self.name

    def postrank(self, data):
        """ 对排序后的结果进行最后处理，抽取、转化成自己需要的结果

        Arguments:
            data: 排序后的结果集合字典.

        Returns:
            data: 自己需要的数据类型.

        Raises:
            InvalidArgumentError
        """
        raise NotImplementedError(
            'Subclasses should implement the predict func')
