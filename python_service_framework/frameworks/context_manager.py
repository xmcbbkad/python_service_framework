# -*- coding: utf-8 -*-


class ContextManager(object):
    def __init__(self):
        self.context = {}

    def context_is_exists(self, key):
        return key in self.context.keys()

    def get_context(self, key, default=None):
        return self.context.get(key, default)

    def add_context(self, key, value):
        if key not in self.context:
            self.context[key] = value
            return True
        else:
            self.context[key] = value
            return False
