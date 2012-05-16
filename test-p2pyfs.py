#!/usr/bin/python
import os, threading, sys, time

def create(name, prefix):
    n = "%s/%s"
    FILE = open(n, "w")
    FILE.close()

def write(name, prefix, nf):
    for i in range(nf):
        n = "%s/%s-%d" % (name, prefix, i)
        FILE = open(n, "w")
        FILE.write("x")
        FILE.close()

def write_wrapper(name, prefix, nf, result, index):
    write(name, prefix, nf)
    result[index] = 1

def checksize(name, prefix, i, size):
    n = "%s/%s-%d" % (name, prefix, i)
    if os.path.getsize(n) == size:
        return True
    return False

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
    t_initial = time.time()
    createn(files[0], "aa", 50)
    results = range(n_clients)
    threads = []
    for i in range(n_clients):
        t = threading.Thread(target = checkn_wrapper, args=(files[i], "aa", 50, results, i))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    t_final = time.time()
    t_diff = t_final - t_initial
    if sum(results) == len(results):
        print "Test 1: OK! Took: %f secs" % float(t_diff)
    else:
        print "Test 1: Failure!"

def test2(n_clients, files):
    print "Test 2: All clients make files and read them"
    threads = []
    t_initial = time.time()
    n_clients %= 3
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
    t_final = time.time()
    t_diff = t_final - t_initial
    if sum(results) == len(results):
        print "Test 2: OK! Took: %f secs" % float(t_diff)
    else:
        print "Test 2: Failure!"

def test3(n_clients, files):
    print "Test 3: One file, all clients write to it"
    t_initial = time.time()
    createn(files[0], "zz", 50)
    results = range(n_clients)
    threads = []
    for i in range(n_clients):
        t = threading.Thread(target = write_wrapper, args=(files[i], "zz", 50, results, i))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    t_final = time.time()
    t_diff = t_final - t_initial
    if sum(results) == len(results) and checksize(files[0], "zz", 0, 1):
        print "Test 3: OK! Took: %f secs" % float(t_diff)
    else:
        print "Test 3: Failure!"

n_clients = 2
if len(sys.argv) == 2:
    n_clients = int(sys.argv[1])

files = []

for i in range(n_clients):
    files.append(os.path.abspath("yfs%d/" % i))

test1(n_clients, files)
print ""
test2(n_clients, files)
print ""
test3(n_clients, files)
print ""
print "All tests completed!"
