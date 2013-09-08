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

    def __init__(self, dumpfile, cachefile):
        self.total_nodes = 0
        self.total_ways = 0
        self.total_relations = 0

        self.dump = open(dumpfile, 'w')
        self.cache = None
        
    def reduce(self, map_result):
        if self.cache is None:
            self.cache = pbfdump.nodecache.NodeCache()
            print 'Loading node cache...'
            self.cache.load(cachefile)
            print '%d coordinates loaded' % self.cache.size()
            
        (node_count, way_count, relation_count), buf = map_result
        self.total_nodes += node_count
        self.total_ways += way_count
        self.total_relations += relation_count

        for line in buf.splitlines():
            feature_type, json_body = tuple(line.split('\t', 1))
            feature_data = json.loads(json_body)
            self.dump.write(feature_type) # type
            self.dump.write('\t')
            self.dump.write(str(feature_data[0])) #osmid
            self.dump.write('\t')
            
            if feature_type == 'way':
                coords = self.cache.findall(feature_data[2])
                assert None not in coords
                json.dump(dict(osm_id=feature_data[0],
                               tags=feature_data[1],
                               geometry=coords),
                          self.dump)
            if feature_type == 'node':
                json.dump(dict(osm_id=feature_data[0],
                               tags=feature_data[1],
                               geometry=feature_data[2]),
                          self.dump)                
            elif feature_type == 'relation':
                json.dump(dict(osm_id=feature_data[0],
                               tags=feature_data[1],
                               relation=feature_data[2]),
                          self.dump)                                
            self.dump.write('\n')

#         self.dump.write(buf)

    def progress(self):
        return '%dk nodes, %dk ways, %dk relations' %  \
            (self.total_nodes / 1000,
             self.total_ways / 1000,
             self.total_relations / 1000)

    def report(self):
        return '%d nodes, %d ways, %d relations' %  \
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
    arg_parser.add_argument('cache_file')
    arg_parser.add_argument('output_file')
    arg_parser.add_argument('--expected-size', '-s', dest='expected_size',
                            type=int, default=0)
    arg_parser.add_argument('--workers', '-w', dest='workers',
                            type=int, default=1)

    args = arg_parser.parse_args()

    mapper = Mapper()

    reducer = Reducer(args.output_file, args.cache_file)

    parser = pbfdump.PBFParser(args.input_file,
                               mapper=mapper,
                               reducer=reducer,
                               worker=args.workers,
                               debug=False)

    result = parser.process()
    print result

if __name__ == '__main__':
    main()
