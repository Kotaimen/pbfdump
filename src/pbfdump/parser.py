'''
OSM PBF format parser, current based on imposm.parser's pbf parser but dumped
its slightly wierd multiprocessing framework

Created on Sep 2, 2013

@author: Kotaimen
'''

import logging
import time
import itertools
import multiprocessing
import threading

from imposm.parser.pbf.parser import PBFFile, PrimitiveBlockParser


class Mapper(object):

    def __call__(self, args):
        filename, block_offset, block_size = args
        block = PrimitiveBlockParser(filename, block_offset, block_size)
        self.setup()
        self.map_nodes(block.nodes())
        self.map_ways(block.ways())
        self.map_relations(block.relations())
        return self.result()

    def setup(self):
        pass

    def map_nodes(self, nodes):
        pass

    def map_ways(self, ways):
        pass

    def map_relations(self, relations):
        pass

    def result(self):
        raise NotImplementedError


class Reducer(object):

    def reduce(self, map_result):
        raise NotImplementedError

    def progress(self,):
        return ''

    def report(self):
        raise NotImplementedError


class PBFParser(object):

    def __init__(self,
                 filename=None,
                 mapper=None,
                 reducer=None,
                 worker=None,
                 debug=False):

        self._filename = filename
        self._mapper = mapper
        self._reducer = reducer
        self._worker = worker
        self._debug = debug
        print 'Scanning PBF file "%s"' % self._filename,
        self._blocks = self._load_pbf()
        print '...%d blocks total' % len(self._blocks)
        self._tic = 0

    def _load_pbf(self):
        pbf_file = PBFFile(self._filename)
        return list((self._filename, pos['blob_pos'], pos['blob_size']) \
                    for pos in pbf_file.blob_offsets())

    def _tac(self, current_block, total_blocks, prompt):
        tac = time.time()
        if tac - self._tic > 1:
            self._tic = tac
            precent = 100. * current_block / total_blocks
            print 'Block #%d/#%d (%.1f%%): %s' % (current_block,
                                                  total_blocks,
                                                  precent,
                                                  prompt)

    def process(self):
        total_blocks = len(self._blocks)
        current_block = 0

        def reducer(current_block, map_result):
            self._reducer.reduce(map_result)
            self._tac(current_block, total_blocks, self._reducer.progress())

        if self._debug:
            for map_result in itertools.imap(self._mapper, self._blocks):
                current_block += 1
                reducer(current_block, map_result)
                del map_result
        else:
            pool = multiprocessing.Pool(processes=self._worker)

            for map_result in pool.imap_unordered(self._mapper, self._blocks):
                current_block += 1
                reducer(current_block, map_result)
                del map_result

        return self._reducer.report()
