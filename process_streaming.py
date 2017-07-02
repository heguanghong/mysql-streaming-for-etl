# -*- coding: utf-8 -*-

import attr
import json
import sys, os
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)

import ConfigParser
import re

class  Tables():
        def __init__(self, db):
                self.__db = db

        def __get_mapped_tables(self,schema):
                map_file = schema + "_mapped_tables.conf"
                if os.path.exists(map_file):
                        cf = ConfigParser.ConfigParser()
                        cf.read(map_file)
                        tables_mapped = {}
                        for table in cf.sections():
                                tables_mapped[table] = cf.items(table)
                return tables_mapped

        def extract_num_from_string(self,string_num, tag='#' ):
                num = 0
                if string_num != None:
                        value = re.findall(r'#(\d+)#', string_num)
                        if len(value) > 0:
                                num = int(value[0])
                return num

        def extract_multi_num_from_string(self,string_num, tag='#' ):
                num = ''
                if string_num != None:
                        value = re.findall(r'#([\d+,]+)#', string_num)
                        if len(value) > 0:
                                num = value[0]
                return num

        def extract_xml(self,xml,attribute):
                """
                        re.findall(r'<JPG-1000X1000-ALBUM>([\S*\s*\n]*.)</JPG-1000X1000-ALBUM>',a)
                :param xml:
                :param attribute:
                :return:
                """
                attr = ''
                if xml != None:
                        value = re.findall(r'<%s>([\S*\s*\n]*.)</%s>'%(attribute,attribute),xml)
                        if len(value )>0:
                                attr = value[0]
                                attr = attr.replace('<![CDATA[]]>','').replace('<![CDATA[','').replace(']]>','')
                return attr

        def filer_birthday(self,date_value):
                birthday = ''
                if date_value != None:
                        value = date_value.replace(u'年-', '-').replace(u'年', '-').replace(u'月', '-')\
                                .replace('日', '').replace(u'/', '-')
                        value = re.findall(r'\d{2,4}-\d{1,2}-\d{1,2}', value)
                        if len(value)>0:
                                birthday = value[0]
                return birthday

        def filter_height(self,height_string):
                height = 0
                if height_string != None:
                        height_string = height_string.lower()
                        value = re.findall(r'(\d{3})cm', height_string)
                        if len(value) > 0:
                                height = int(value[0])
                        else:
                                value = re.findall(r'(\d.\d+)m', height_string)
                                if len(value) > 0:
                                        height = int(float(value[0]) * 100)
                if height>250:
                        height = 0
                return height

        def filter_weight(self,weight_string):
                weight = 0
                if weight_string != None:
                        weight_string = weight_string.lower()
                        value = re.findall(r'(\d+)kg', weight_string)
                        if len(value)>0:
                                weight = int(value[0])
                        if weight >150:
                                weight = 0
                return weight













