#!/usr/bin/python
import os, threading, sys

def create(name, prefix):
    n = "%s/%s"
    FILE = open(n, "w")
    FILE.close()

def write(n):
    pass
def createn(name, prefix, nf):
    for i in range(nf):
        n = "%s/%s-%d" % (name, prefix, i)
        FILE = open(n, "w")
        FILE.close()

def checkn(name, prefix, nf):
    for i in range(nf):
        n = "%s/%s-%d" % (name, prefix, i)
        if not os.path.exists(n):
            return False
    return True

def checkn_wrapper(name, prefix, nf, result, index):
  result[index] = checkn(name, prefix, nf)

def test1(n_clients, files):
    print "Test 1: 1 client writes files, a lot of other clients read them"
    createn(files[0], "aa", 50)
    results = range(n_clients)
    threads = []
    for i in range(n_clients):
        t = threading.Thread(target = checkn_wrapper, args=(files[i], "aa", 50, results, i))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    if sum(results) == len(results):
        print "Test 1: OK!"
    else:
        print "Test 1: Failure!"


def test2(n_clients, files):
    print "Test 2: All clients make files and read them"
    threads = []
    for i in range(n_clients):
        t = threading.Thread(target = createn, args=(files[i], "aa" + str(i), 50))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    results = range(n_clients * n_clients)
    for i in range(n_clients):
        for j in range(n_clients):
            t = threading.Thread(target = checkn_wrapper, args=(files[i], "aa" + str(j), 50, results, i * n_clients + j))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()
    if sum(results) == len(results):
        print "Test 2: OK!"
    else:
        print "Test 2: Failure!"


def test3(n_clients, files):
    print "Test 3: One file, all clients write to it"
    #creatn(files)
    pass


n_clients = 2
if len(sys.argv) == 2:
    n_clients = int(sys.argv[1])

files = []

for i in range(n_clients):
    files.append(os.path.abspath("yfs%d/" % i))

test1(n_clients, files)
test2(n_clients, files)

