# -*- coding: utf-8 -*-
import json
import oss2
import importlib
from queue import Queue
import copy
import logging
logger = logging.getLogger(__name__)


class TreeNode:
    def __init__(self, name=None, info=None, children=None):
        if children is None:
            children = []
        self.name = name
        self.info = info
        if info and not name:
            self.name = info["name"]
        self.children = children
        self.isouter = False

    def node_print(self):
        if self.name:
            return self.name
        else:
            return self.__str__()

    def add_child(self, node):
        self.children.append(node)

    def get_children(self):
        return self.children

    def set_outer(self, outer):
        self.isouter = outer


def parse_tree(root, level=1):
    print("{}{}".format("-" * 2 * level, root.node_print()))
    for child in root.get_children():
        parse_tree(child, level + 1)


class ProcessorDAG(object):
    def __init__(self, conf_path, **kwargs):
        # 初始化要用到的插件
        file_type = kwargs.get('file_type', 'local')
        self.conf = None
        if file_type == 'oss':
            # load_oss
            auth = oss2.Auth('', '')
            endpoint = ''
            bucket = oss2.Bucket(auth, endpoint, '')
            object_stream = bucket.get_object(conf_path)
            self.conf = json.loads(object_stream.read().decode(encoding='utf-8'))
            if object_stream.client_crc != object_stream.server_crc:
                logger.error("The CRC checksum between client and server is inconsistent!")
        else:
            self.conf = json.load(open(conf_path, 'r'))
        self.__check_conf()
        self.plugins = {}
        self.plugin_configs = {}
        for plugin in self.conf.get('plugins', []):
            self.plugin_configs[plugin['name']] = plugin
            Plugin = self.__get_plugins_cls(plugin['path'])
            self.plugins[plugin['name']] = Plugin(**plugin['args'])
        logger.info("PLUGINS = {}".format(self.plugins))
        self.exetree = self.__build_exec_tree()
        self.paths = self.__parse_to_path(self.exetree)

    def __check_conf(self):
        find_input = False
        name_set = ['input', 'output']
        for item in self.conf.get('plugins', []):
            name = item.get('name')
            if not name or '-' in name or '.' in name:
                raise Exception("【配置文件异常】Processor名称不合法！")
            name_set.append(name)

        for item in self.conf.get('plugins', []):
            input = item.get('input')
            if input == 'input' and find_input is False:
                find_input = True
            elif input == 'input' and find_input is True:
                raise Exception("【配置文件异常】检测到多个系统输入！")
            if input:
                if isinstance(input, str):
                    if input not in name_set:
                        raise Exception("【配置文件异常】检测到模块输入端格式不正确！输出模块d必须是input或模块名")
                else:
                    raise Exception("【配置文件异常】检测到模块输入端格式不正确！必须为字符串类型")

            outputs = item.get('output')
            if outputs:
                if isinstance(outputs, list):
                    for op in outputs:
                        if op not in name_set:
                            raise Exception("【配置文件异常】检测到模块输出端格式不正确！输出模块必须是output或模块名")
                else:
                    raise Exception("【配置文件异常】检测到模块输出端格式不正确！必须为列表类型")

    def __build_exec_tree(self):
        # 建立树结构
        root = None
        que = Queue(maxsize=0)

        for key in self.plugin_configs.keys():
            if self.plugin_configs[key]['input'] == "input":
                root = TreeNode(key, self.plugin_configs[key])
                que.put(root)
                break

        while not que.empty():
            node = que.get()
            child_list = node.info['output']
            for item in child_list:
                if item == 'output':
                    node.set_outer(True)
                    continue
                for key in self.plugin_configs.keys():
                    if self.plugin_configs[key]['name'] == item:
                        sub_node = TreeNode(key, self.plugin_configs[key])
                        node.add_child(sub_node)
                        que.put(sub_node)
                        break

        return root

    def __parse_to_path(self, root):
        # 树 -> 路径，中间结果保存，利用输出节点的名称作为变量名分别保存
        paths = []
        if root is None:
            return paths
        self.__get_path(root, paths, root.name + "")
        return paths

    def __get_path(self, root, result, str):
        if len(root.get_children()) == 0:
            if root.isouter:
                str = str + '->output'
            result.append(str)
            return
        else:
            if root.isouter:
                result.append(str + '->output')
        for child in root.get_children():
            self.__get_path(child, result, str + '->' + child.name)

    @staticmethod
    def __get_plugins_cls(module_str):
        paths = module_str.split('.')
        class_name = paths[-1]
        module_path = '.'.join(paths[:-1])
        plugin_mod = importlib.import_module(module_path)
        PluginCls = getattr(plugin_mod, class_name)
        if not PluginCls:
            raise Exception('User defined plugin %s not exists' % module_str)
        return PluginCls

    def judge_type_copy(self, input):
        '''
        判断输入是什么类型，如果是df则不需要复制，如果是dict，list类型则需要进行深复制
        '''
        if isinstance(input, dict) or isinstance(input, list):
            return copy.deepcopy(input)
        return input

    def parse_to_run(self, inputs):
        # 配置文件到执行逻辑
        outputs = inputs
        middles = {}
        context = {}
        # 配置文件 -> 树
        # __build_exec_tree
        # 树 -> 路径
        # __parse_to_path，最终生成self.paths
        # 路径 -> 执行
        for path in self.paths:
            last = 'input'
            modules = path.split('->')
            for i in range(0, len(modules)):
                module = modules[i]
                key = '-'.join(modules[:i+1])
                if module == 'output':
                    if last in middles:
                        outputs = self.judge_type_copy(middles[last])
                    else:
                        raise Exception("key {} not in fields".format(last))
                else:
                    if last == 'input':
                        if key not in middles:
                            assert self.plugin_configs[module]['input'] == 'input'
                            middle_input = self.judge_type_copy(inputs)
                            middles[key] = self.plugins[module].process(middle_input, context)
                            middle_input = None
                        last = key
                    else:
                        if key not in middles:
                            if last in middles:
                                if len(self.plugin_configs[module]['input'].split('.')) == 2:
                                    input_index = int(self.plugin_configs[module]['input'].split('.')[1])
                                    middle_input = middles[last][input_index - 1]
                                else:
                                    middle_input = middles[last]
                                middle_input = self.judge_type_copy(middle_input)
                                middles[key] = self.plugins[module].process(middle_input, context)
                                middle_input = None
                            else:
                                raise Exception("key {} not in fields".format(last))
                        last = key

        middles.clear()
        return outputs

    def run(self, inputs):
        # 先写成代码自定义流程，然后改写成配置文件形式
        # 输入为上游pony传进来的dataframe，输出为要传给下游的dataframe，如果没有下游数据则返回false（或者空数据）即可
        # 中间数据格式是自定义，要和自定义插件格式保持一致
        logger.info("data")
        outputs = self.parse_to_run(inputs)
        return outputs


if __name__ == '__main__':
    plugins = ProcessorDAG('plugin_configs/streaming/config.json')  # configs/batch/example.json
    parse_tree(plugins.exetree)
    print(plugins.paths)
    # obj = plugins.run('00001000001')
    # print(obj)
