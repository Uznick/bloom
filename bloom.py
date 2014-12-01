#!/usr/bin/env python
import tornado.ioloop
import tornado.web
from tornado import gen
from concurrent.futures import ThreadPoolExecutor
from bitarray import bitarray
import hashlib
import sys

# Constants

hashpart = 30
m = 2 ** hashpart   # Bloom m-parameter
k = 7       # Bloom k-parameter
listen_port = 8888
workers = 4

def getHashes(element):
    H = hashlib.sha224()
    H.update(element)

    h = bitarray()
    h.frombytes(H.digest())

    return [ reduce(lambda A, v: A*2+1 if v else A*2, h[i*hashpart:(i+1)*hashpart], 0) for i in xrange(k) ]

class CmdAddHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        element = self.get_argument("e", strip=False)
        hashes = yield thread_pool.submit(getHashes,element)

        for i in hashes:
            Bloom[i] = True

        self.set_header('Content-Type', 'text/plain; charset="utf-8"')
        self.write("ADDED\n")

class CmdCheckHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        element = self.get_argument("e", strip=False)
        hashes = yield thread_pool.submit(getHashes,element)
        
        self.set_header('Content-Type', 'text/plain; charset="utf-8"')
        for i in hashes:
            if not Bloom[i]:
                self.write("MISSING\n")
                return

        self.write("PRESENT\n")

# Init

print >> sys.stderr, "Initializing %.2f MBytes bitvector ..." % (m / float(2**20) / 8)
Bloom = bitarray(m)
Bloom.setall(False)
print >> sys.stderr, "OK"
print >> sys.stderr, "Initializing %d hash calculation workers ..." % workers
thread_pool = ThreadPoolExecutor(workers)
print >> sys.stderr, "OK"
print >> sys.stderr, "Initialization complete"

application = tornado.web.Application([
    (r"/add", CmdAddHandler),
    (r"/check", CmdCheckHandler),
], debug=True)

if __name__ == "__main__":
    application.listen(listen_port)
    tornado.ioloop.IOLoop.instance().start()
