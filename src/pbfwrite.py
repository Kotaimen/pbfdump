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
                continue
            self.node_count += 1
            self.buf.write('node\t%d\t' % node[0])
            json.dump(dict(osm_id=node[0],
                           tags=node[1],
                           geometry=node[2]),
                      self.buf)
            self.buf.write('\n')

    def map_ways(self, ways):
        for way in ways:
            self.way_count += 1
            self.buf.write('way\t%d\t' % way[0])
            json.dump(dict(osm_id=way[0],
                           tags=way[1],
                           nodes=way[2]),
                      self.buf)
            self.buf.write('\n')

    def map_relations(self, relations):
        for relation in relations:
            self.relation_count += 1
            self.buf.write('relation\t%d\t' % relation[0])
            json.dump(dict(osm_id=relation[0],
                           tags=relation[1],
                           relation=relation[2]),
                      self.buf)
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

        self.dump = open(dumpfile, 'w')

    def reduce(self, map_result):

        (node_count, way_count, relation_count), buf = map_result
        self.total_nodes += node_count
        self.total_ways += way_count
        self.total_relations += relation_count
        self.dump.write(buf)

    def progress(self):
        return '%dk nodes, %dk ways, %dk relations' % \
            (self.total_nodes / 1000,
             self.total_ways / 1000,
             self.total_relations / 1000)

    def report(self):
        return '%d nodes, %d ways, %d relations' % \
            (self.total_nodes,
             self.total_ways,
             self.total_relations)


#===============================================================================
# Main
#===============================================================================

# import logging
# import multiprocessing
# multiprocessing.log_to_stderr().setLevel(logging.DEBUG)


def main():

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('input_file')
#     arg_parser.add_argument('cache_file')
    arg_parser.add_argument('output_file')
#     arg_parser.add_argument('--expected-size', '-s', dest='expected_size',
#                             type=int, default=0)
#     arg_parser.add_argument('--workers', '-w', dest='workers',
#                             type=int, default=1)

    args = arg_parser.parse_args()

    mapper = Mapper()

    reducer = Reducer(args.output_file)

    parser = pbfdump.PBFParser(args.input_file,
                               mapper=mapper,
                               reducer=reducer,
                               worker=None,
                               debug=False)

    result = parser.process()
    print result

if __name__ == '__main__':
    main()
