#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os

def main():
    if len(sys.argv) != 2:
        print 'This file is used to gen thrift struct from mysql table creation sql'
        print 'And gen domain class'
        print 'Usage: {} <ddl_file>'.format(sys.argv[0])
        sys.exit(1)

    thirft_ret = 'struct {\n'
    domain_ret = 'Class {\n'
    table_name = ''
    col_list = []

    file_name = sys.argv[1]
    field_idx = 1
    create_prefix = 'CREATE TABLE `'
    with open(file_name, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith(create_prefix):
                table_name = line[len(create_prefix):-3]
                continue
            elif not line.startswith('`'):
                continue

            tokens = line.split()
            col_name = tokens[0][1:-1] #remove backticks
            col_type = tokens[1]
            thrift_type, java_type = db_type_to_thrift_java_type(col_type)
            comment = ''
            comment_idx = line.find("COMMENT '")
            if comment_idx > -1:
                comment_idx += len("COMMENT '")
                end_idx = line.rfind("',")
                comment = line[comment_idx:end_idx]
            #if(tokens[-2] == 'COMMENT'):
            #    comment = tokens[-1]
            camel_cased = underscore_to_camelcase(col_name)
            thirft_ret += '    {}: optional {} {}; // {}\n'.format(field_idx, thrift_type, camel_cased, comment)
            domain_ret += 'private {} {}; // {}\n'.format(java_type, col_name, comment)
            col_list.append(col_name)
            field_idx += 1

    thirft_ret += '}\n'
    domain_ret += '}\n'
    print thirft_ret
    print 'table name:', table_name, underscore_to_classname(table_name)
    print
    print domain_ret
    print
    print ', '.join(col_list)
    print
    print gen_foreach_list(col_list)
    print

def gen_foreach_list(col_list):
    ret = '('
    for col in col_list:
        s = '#{{item.{0}}}, '.format(col)
        ret += s
    ret = ret[:-2]
    ret += ')'
    return ret


def db_type_to_thrift_java_type(db_type):
    if db_type.startswith('int') or db_type.startswith('mediumint'):
        return 'i32', 'int'
    elif db_type.startswith('varchar'):
        return 'string', 'String'
    elif db_type.startswith('bigint'):
        return 'i64', 'long'
    elif db_type.startswith('decimal') or db_type.startswith('double'):
        return 'double', 'double'
    elif db_type.startswith('tinyint'):
        return 'byte', 'byte'
    elif db_type.startswith('smallint'):
        return 'i16', 'short'
    else:
        print 'ERROR, unknown type {}'.format(db_type)
        sys.exit(1)

def underscore_to_camelcase(underscore):
    items = underscore.split('_')
    camelcase = items[0]
    for i in range(1, len(items)):
        camelcase += items[i].capitalize()
    return camelcase

def underscore_to_classname(underscore):
    items = underscore.split('_')
    camelcase = ''
    for i in range(0, len(items)):
        camelcase += items[i].capitalize()
    return camelcase

if __name__ == "__main__":
    main()
