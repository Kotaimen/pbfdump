#!/usr/bin/env python
'''
Created on Sep 4, 2013

@author: Kotaimen
'''

import argparse
from pprint import pprint

import pbfdump


class Mapper(pbfdump.Mapper):

    def map_nodes(self, nodes):
        self.num_nodes = len(list(nodes))

    def map_ways(self, ways):
        self.num_ways = len(list(ways))

    def map_relations(self, relations):
        self.num_relations = len(list(relations))

    def result(self):
        return self.num_nodes, self.num_ways, self.num_relations


class Reducer(pbfdump.Reducer):

    def __init__(self):
        self.total_nodes = 0
        self.total_ways = 0
        self.total_relations = 0

    def reduce(self, map_result):
        num_nodes, num_ways, num_relations = map_result
        self.total_nodes += num_nodes
        self.total_ways += num_ways
        self.total_relations += num_relations

    def progress(self):
        return '%0dk nodes, %dk ways, %dk relations' % (self.total_nodes / 1000,
                                                        self.total_ways / 1000,
                                                        self.total_relations / 1000)

    def report(self):
        return '%0d nodes, %d ways, %d relations' % (self.total_nodes,
                                                     self.total_ways,
                                                     self.total_relations)

#===============================================================================
# Main
#===============================================================================


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('pbf_file')
    args = arg_parser.parse_args()

    mapper = Mapper()
    reducer = Reducer()

    parser = pbfdump.PBFParser(args.pbf_file,
                               mapper=mapper,
                               reducer=reducer,
                               worker=None,
                               debug=False)

    result = parser.process()

    print result


if __name__ == '__main__':
    main()
