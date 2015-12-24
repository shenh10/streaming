from collections import deque
from threading import *
import socket, select
import sys
import asyncore
import signal
import pdb
class Master(object):
    def __init__(self, addr , port , task, url):
        self.task_queue = deque(task)
        self.N = len(task)
        self.done = 0
        self.K = 10
        self.lookup = {}
        self.address = addr
        self.port = port
        self.index = url
        self.index_dict = {}
        self.event = Event()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.conn_list = deque([])
        self.recv_buffer = 2048
        self.server_thread = Thread(target = self.server_executor)
        self.event_thread = Thread(target = self.event_listener)
        self.lock = Lock()
        self.sock_lock = {} 
        self.func_lock = Lock()
        self.response_queue = deque([])
        self.stop = 0

    def event_listener(self):
        while not self.stop:
            while not self.stop and not self.event.wait(.1):
                pass
            self.func_lock.acquire()
            self.handle_response()
            self.event.clear()
            self.func_lock.release()

    def handle_response(self):
        while not self.stop and self.response_queue:
            (connd, url, status) = self.response_queue.popleft()
            if url in self.lookup[connd]:
                print 'Response come back of %s'%url
                self.lookup[connd].remove(url)
                if status == 'SUCCESS':
                    print 'SUCCESS Downlowded:%s'%url
                    self.lock.acquire()
                    self.done += 1
                    self.lock.release()
                else:
                    print 'FAILURE Downlowded:%s'%url
                    self.lock.acquire()
                    self.task_queue.append(url)
        print "Finish %d pieces..."%self.done
 

    def server_executor(self):
        server_addr =(self.address, self.port)
        self.sock.bind(server_addr)
        print >>sys.stderr, 'starting up on %s port %s' % self.sock.getsockname()
        self.sock.listen(10)
        while not self.stop:
            l = list(self.conn_list) 
            read_sockets,write_sockets,error_sockets = select.select(l + [ self.sock ],[],[], 0)
            for sock in read_sockets:
                if sock == self.sock:
                    sockfd, addr = self.sock.accept()
                    self.conn_list.append(sockfd)
                    self.sock_lock[sockfd] = Lock()
                    print "Client (%s, %s) connected" % addr
                else:
                    try:
                        self.sock_lock[sock].acquire()
                        data = sock.recv(self.recv_buffer)
                        self.sock_lock[sock].release()
                        if data:
                            self.event.set()
                            if data.startswith('SUCCESS') or data.startswith('FAILURE'):
                                (status, url) = data.split('!')
                                self.response_queue.append((sock,url,status))
                    except:
                        print "Client (%s, %s) is offline" % addr
                        sock.close()
                        self.conn_list.remove(sock)
                        continue
        for conn in self.conn_list:
            conn.send('CLOSE')
            conn.close()
        self.sock.close()


    def get_smallest_length(self, x):
        return [(k, len(x.get(k))) for k in x.keys() if len(x.get(k))==min([len(n) for n in x.values()])] 

    def run(self):
        try:
            self.server_thread.start()
            self.event_thread.start()
            while not self.stop:
                self.lock.acquire()
                done = self.done
                self.lock.release()
                if done == self.N:
                    break
                if not self.task_queue:
                    continue
                for slave in self.conn_list:
                    if  slave not in self.lookup:
                       self.lookup[slave] = deque([])
                       read_sockets,write_sockets,error_sockets = select.select([], [slave],[], 0)
                       for sock in write_sockets:
                           print 'Ask client to download index file'
                           self.sock_lock[sock].acquire()
                           sock.send('INDEX!%s'%self.index)
                           data = sock.recv(self.recv_buffer) 
                           self.sock_lock[sock].release()
                           if data.startswith('ACC!SUCCESS'):
                                continue
                           else:
                                print "Error to push index file"
                                self.stop = 1
                if len(self.lookup.keys()) == 0:
                    continue
                (min_slave, min_val) = self.get_smallest_length(self.lookup)[0] 
                if min_val < self.K:
                    url = self.task_queue.popleft()
                    read_sockets,write_sockets,error_sockets = select.select([], [min_slave],[], 0)
                    for sock in write_sockets:
                        if sock == min_slave: 
                            if url in self.lookup[sock]:
                                continue
                            self.sock_lock[sock].acquire()
                            sock.send('DOWNLOAD!%s'%url)
                            data = sock.recv(self.recv_buffer) 
                            if data.startswith('ACC!SUCCESS'):
                                self.lookup[sock].append(url)
                            else:    
                                self.task_queue.append(url)
                            self.sock_lock[sock].release()
                else:
                    print 'Too many task assigned , waiting...'
                    while not self.stop and not self.event.wait(.1):
                        pass
                    self.func_lock.acquire()
                    self.handle_response()
                    self.event.clear()
                    self.func_lock.release()
            self.stop = 1
            print 'All task done... Exit from Downloader'
            while self.server_thread.is_alive():
                self.server_thread.join(.1)
            while self.event_thread.is_alive():
                self.event_thread.join(.1)

        except :
            self.stop = 1
            while self.server_thread.is_alive():
                self.server_thread.join(.1)
            while self.event_thread.is_alive():
                self.event_thread.join(.1)
            sys.exit()
