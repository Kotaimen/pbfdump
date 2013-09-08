#!/usr/bin/env python
'''
Directly dump PBF file into a postgres database for later processing
Created on Sep 4, 2013

@author: Kotaimen
'''
from pprint import pprint

import os
import sys
import gzip
import argparse

import pbfdump


class Mapper(pbfdump.Mapper):

    def map_nodes(self, nodes):
        self.nodes = list((n[0], n[2]) for n in nodes)

    def result(self):
        return self.nodes


class Reducer(pbfdump.Reducer):

    def __init__(self, dumpfile, expected_size=0):
        self.dumpfile = dumpfile
        self.nodecache = pbfdump.NodeCache(expected_size)

    def reduce(self, map_result):
        for osmid, coord in map_result:
            self.nodecache.insert(osmid, coord)

    def progress(self):
        return '%dk coordinates loaded.' % (self.nodecache.size() / 1000)

    def report(self):
        print 'Dumping coordinate cache to:', self.dumpfile
        self.nodecache.dump(self.dumpfile)
        print '...done.'

#===============================================================================
# Main
#===============================================================================

# import logging
# import multiprocessing
# multiprocessing.log_to_stderr().setLevel(logging.DEBUG)


def main():

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('pbf_file')
    arg_parser.add_argument('coordinate_cache')
    arg_parser.add_argument('--expected-size', '-s', dest='expected_size',
                            type=int, default=0)
    arg_parser.add_argument('--workers', '-w', dest='workers',
                            type=int, default=1)

    args = arg_parser.parse_args()

    mapper = Mapper()

    reducer = Reducer(args.coordinate_cache,
                      args.expected_size)

    parser = pbfdump.PBFParser(args.pbf_file,
                               mapper=mapper,
                               reducer=reducer,
                               worker=args.workers,
                               debug=False)

    result = parser.process()

if __name__ == '__main__':
    main()
