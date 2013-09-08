#!/usr/bin/env python
'''
Directly dump PBF file into a postgres database for later processing
Created on Sep 4, 2013

@author: Kotaimen
'''
from pprint import pprint

import os
import sys
import io
import ujson as json
import argparse
import gzip
import pbfdump


class Mapper(pbfdump.Mapper):

    def setup(self):
        self.buf = io.BytesIO()
        self.node_count = 0
        self.way_count = 0
        self.relation_count = 0

    def map_nodes(self, nodes):
        for node in nodes:
            if not node[1]:
                return
            self.node_count += 1
            self.buf.write('node\t')
            json.dump(node, self.buf)
            self.buf.write('\n')

    def map_ways(self, ways):
        for way in ways:
            self.way_count += 1
            self.buf.write('way\t')
            json.dump(way, self.buf)
            self.buf.write('\n')

    def map_relations(self, relations):
        for relation in relations:
            self.relation_count += 1
            self.buf.write('relation\t')
            json.dump(relation, self.buf)
            self.buf.write('\n')

    def result(self):
        buf = self.buf.getvalue()
        self.buf = None
        return ((self.node_count, self.way_count, self.relation_count), buf)


class Reducer(pbfdump.Reducer):

    def __init__(self, dumpfile):
        self.total_nodes = 0
        self.total_ways = 0
        self.total_relations = 0

        self.dumpfile = open(dumpfile, 'w')
#        self.dumpfile = gzip.GzipFile(dumpfile, 'w')

    def reduce(self, map_result):
        (node_count, way_count, relation_count), buf = map_result
        self.dumpfile.write(buf)
        self.total_nodes += node_count
        self.total_ways += way_count
        self.total_relations += relation_count

    def progress(self):
        return '%0dk nodes, %dk ways, %dk relations' %  \
            (self.total_nodes / 1000, 
             self.total_ways / 1000, 
             self.total_relations / 1000)

    def report(self):
        pass


#===============================================================================
# Main
#===============================================================================

# import logging
# import multiprocessing
# multiprocessing.log_to_stderr().setLevel(logging.DEBUG)


def main():

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('input_file')
    arg_parser.add_argument('output_file')
    arg_parser.add_argument('--expected-size', '-s', dest='expected_size',
                            type=int, default=0)
    arg_parser.add_argument('--workers', '-w', dest='workers',
                            type=int, default=1)

    args = arg_parser.parse_args()

    mapper = Mapper()

    reducer = Reducer(args.output_file)

    parser = pbfdump.PBFParser(args.input_file,
                               mapper=mapper,
                               reducer=reducer,
                               worker=args.workers,
                               debug=False)

    result = parser.process()

if __name__ == '__main__':
    main()
