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
    args = arg_parser.parse_args()

    cache = pbfdump.nodecache.NodeCache()
    print 'Loading node cache...'
    cache.load(args.cache_file)
    print '%d coordinates loaded' % cache.size()

    with open(args.input_file, 'r') as ifp, open(args.output_file, 'wb') as ofp:
        for n, line in enumerate(ifp):
            if n % 100000 == 0:
                print 'feature %dk' % (n / 1000)
            if line.startswith('way\t'):
                line = line.split('\t', 2)
                data = json.loads(line[2])
                ofp.write('way\t%d\t' % data['osm_id'])
                coords = cache.findall(data['nodes'])
                assert None not in coords
                json.dump(dict(osm_id=data['osm_id'],
                               tags=data['tags'],
                               coords=coords,
                               ),
                          ofp)
                ofp.write('\n')
            else:
                ofp.write(line)



if __name__ == '__main__':
    main()
