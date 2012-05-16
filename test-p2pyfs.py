#!/usr/bin/python
import os, threading, sys

class Test(object):
    def __init__(self):
        self.test_result = True
    # name is absolute path

    def create(self, name, prefix):
        n = "%s/%s"
        FILE = open(n, "w")
        FILE.close()

    def write(self, n):
        pass
    def createn(self, name, prefix, nf):
        for i in range(nf):
            n = "%s/%s-%d" % (name, prefix, i)
            FILE = open(n, "w")
            FILE.close()

    def checkn(self, name, prefix, nf):
        print "Checking"
        for i in range(nf):
            n = "%s/%s-%d" % (name, prefix, i)
            print n
            if not os.path.exists(name):
                self.test_result &= False
        self.test_result &= True

    def test1(self, n_clients, files):
        print "Test 1: 1 client writes files, a lot of other clients read them"
        self.createn(files[0], "aa", 20)
        threads = []
        for i in range(n_clients):
            t = threading.Thread(target = self.checkn, args=(files[i], "aa", 20))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        if self.test_result:
            print "Test 1: OK!"
        else:
            print "Test 1: Failure!"
            self.test_result = True


    def test2(self, n_clients, files):
        print "Test 2: All clients make files and read them"
        threads1 = []
        threads2 = []
        for i in range(n_clients):
            t = threading.Thread(target = self.createn, args=(files[i], "aa" + str(i), 20))
            t.start()
            threads1.append(t)
        for t in threads1:
            t.join()
        for i in range(n_clients):
            for j in range(n_clients):
                t = threading.Thread(target = self.checkn, args=(files[i], "aa" + str(j), 20))
                t.start()
                threads2.append(t)
        for t in threads2:
            t.join()
        if self.test_result:
            print "Test 2: OK!"
        else:
            print "Test 2: Failure!"
            self.test_result = True


    def test3(self, n_clients, files):
        print "Test 3: One file, all clients write to it"
        #self.creatn(files)
        pass


n_clients = 2
if len(sys.argv) == 2:
    n_clients = int(sys.argv[1])

files = []

for i in range(n_clients):
    files.append(os.path.abspath("yfs%d/" % i))

tst = Test()
#tst.test1(n_clients, files)
tst.test2(n_clients, files)

    
