import sys
sys.path.insert(0, './build/lib.macosx-10.8-x86_64-2.7/')
import unittest
import random

from pbfdump import nodecache

class TestNodeCache(unittest.TestCase):

    def setUp(self):
        self.n = nodecache.NodeCache()

    def tearDown(self):
        del self.n

    def testNodeCacheInit(self):
        self.assertEqual(self.n.size(), 0)

    def testNodeCacheStr(self):
        self.assertEqual(repr(self.n), 'SparseNodeCache()')

    def testInsert(self):
        self.n.insert(1, (1.1, 2.1))
        self.n.insert(2, (3.1, 4.1))
        coord = self.n.find(1)
        self.assertAlmostEqual(1.1, coord[0], delta=5)
        self.assertAlmostEqual(2.1, coord[1], delta=5)
        self.assertEqual(self.n.find(3), None)
#         print self.n.findall([1, 2])
        self.assertEqual(self.n.findall([4, 3]), [None, None])
        self.assertEqual(self.n.findall([1, 2]), [(1.1, 2.1), (3.1, 4.1)])
        self.assertEqual(self.n.findall([1, 3]), [(1.1, 2.1), None])


    def testDumpLoad(self):
        self.n.clear()
        num = 1 * 10000
        dump = './self.dump'
        print 'inserting...'
        for i in xrange(num):
            self.n.insert(i, (random.uniform(-180., 180.), random.uniform(-180., 180.)))
        print 'dumping...'
        self.n.dump(dump)
        self.assertEqual(self.n.size(), num)
        self.n.clear()
        self.assertEqual(self.n.size(), 0)
        print 'loading...'
        self.n.load(dump)
        self.assertEqual(self.n.size(), num)
        self.n.clear()



if __name__ == '__main__':
    unittest.main()
