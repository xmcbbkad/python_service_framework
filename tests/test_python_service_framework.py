# -*- coding: utf-8 -*-
from python_service_framework.run import Runner
import unittest


class TestProcessor(unittest.TestCase):

    def test1(self):
        model = Runner()
        result = model.predict(['1010111'])
        self.assertEqual(['1010111 - 自定义的字符串 - 自定义的字符串 - 自定义的字符串'], result)

