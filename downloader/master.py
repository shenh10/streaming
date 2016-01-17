from collections import deque
from threading import *
import socket, select
import sys
import asyncore
import signal
import pdb
class Master(object):
    def __init__(self, addr , port , task, url):
        """
        The master takes all segments and assign the downloading job to clients.
        Multithreading program with synchronous/asychronous communications to clients.
        Queues:
            task_queue: tasks waiting for assign
            conn_list: handles of sockets from clients
            response_queue: download finish event from clients
        Locks:
            sock_lock: lock for each client handle, in avoid of read/write conflict in different threads.
            conn_list_lock: lock for conn_list queue
            lock: lock for "done"
            func_lock: lock for handle_response function.
        Threads:
            server_executor(server_thread): server listening for incoming connection and first stage handshake/download repsonse reception 
            run(main thread): Dynamically assign tasks if task_queue is not empty and waiting threshold "K" is not exceeded.
            event_listener(event_thread): Handle downloaded response(Increase counting, remove waiting job from dict, support failure tolerance)
        Events:
            event: New download response recieved
            nc_event: New client joined in
        Others:
            N: number of task piece
            done: current downloaded pieces among local P2P network
            K: Upper limit for waiting task per client 
            lookup: <client, [wating tasks]> dictionary maintains tasks assigned to client which haven't been done.
            address/port: master IP address and port to run server
            sock: server socket handle
            recv_buffer: recieve buffer size
            index: m3u8 url (push to all clients for local video playback)
            stop: stop sign for error occurance or task complete
            
        """
        self.task_queue = deque(task)
        self.N = len(task)
        self.done = 0
        self.K = 10
        self.lookup = {}
        self.address = addr
        self.port = port
        self.index = url
        self.event = Event()
        self.nc_event = Event()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.conn_list = deque([])
        self.conn_list_lock = Lock()
        self.recv_buffer = 2048
        self.server_thread = Thread(target = self.server_executor)
        self.event_thread = Thread(target = self.event_listener)
        self.lock = Lock()
        self.sock_lock = {} 
        self.func_lock = Lock()
        self.response_queue = deque([])
        self.stop = 0

    def event_listener(self):
        """
        Thread for downloaded response
        """
        while not self.stop:
            while not self.stop and not self.event.wait(.1):
                pass
            self.func_lock.acquire()
            self.handle_response()
            self.event.clear()
            self.func_lock.release()

    def handle_response(self):
        """
        Poll response_queue, if not empty, deal with it
        """
        while not self.stop and self.response_queue:
            (connd, url, status) = self.response_queue.popleft()
            if url in self.lookup[connd]:
                print 'Response come back of %s'%url
                # If success, remove from lookup table and increase the counting 
                self.lookup[connd].remove(url)
                if status == 'SUCCESS':
                    print 'SUCCESS Downlowded:%s'%url
                    self.lock.acquire()
                    self.done += 1
                    self.lock.release()
                else:
                # If failed, push task to task_queue to be reassigned
                    print 'FAILURE Downlowded:%s'%url
                    self.lock.acquire()
                    self.task_queue.append(url)
        print "Finish %d pieces..."%self.done
 

    def server_executor(self):
        """
        Server thread
        """
        server_addr = (self.address, self.port)
        self.sock.bind(server_addr)
        print >>sys.stderr, 'starting up on %s port %s' % self.sock.getsockname()
        self.sock.listen(10)
        while not self.stop:
            self.conn_list_lock.acquire()
            l = list(self.conn_list) 
            self.conn_list_lock.release()
            # Polling the clients socket, check if there is socket ready to read
            read_sockets,write_sockets,error_sockets = select.select(l + [ self.sock ],[],[], 0)
            for sock in read_sockets:
                # server ready to read
                if sock == self.sock:
                    # Accept connection request
                    sockfd, addr = self.sock.accept()
                    self.conn_list_lock.acquire()
                    self.conn_list.append(sockfd)
                    self.conn_list_lock.release()
                    self.sock_lock[sockfd] = Lock()
                    print "Client (%s, %s) connected" % addr
                    if not self.nc_event.is_set(): self.nc_event.set()
                # some client ready to read
                else:
                    try:
                        self.sock_lock[sock].acquire()
                        # Read message to buffer
                        data = sock.recv(self.recv_buffer)
                        self.sock_lock[sock].release()
                        if data:
                            self.event.set()
                            # If its a "downloaded" message, push to response_queue
                            if data.startswith('SUCCESS') or data.startswith('FAILURE'):
                                (status, url) = data.split('!')
                                self.response_queue.append((sock,url,status))
                    except:
                        print "Client (%s, %s) is offline" % addr
                        sock.close()
                        self.conn_list.remove(sock)
                        continue
        for conn in self.conn_list:
            #conn.send('CLOSE')
            conn.close()
        self.sock.close()


    def get_smallest_length(self, x):
        return [(k, len(x.get(k))) for k in x.keys() if len(x.get(k))==min([len(n) for n in x.values()])] 

    def run(self):
        """
        Main thread: Assigning tasks to clients 
        """
        try:
            self.server_thread.start()
            self.event_thread.start()
            while not self.stop:
                self.lock.acquire()
                done = self.done
                self.lock.release()
                # If all pieces are downloaded, exit
                if done == self.N:
                    break
                # if task_queue is empty, continue
                if not self.task_queue:
                    continue
                self.conn_list_lock.acquire()
                for slave in self.conn_list:
                    # If slave is a new client
                    if  slave not in self.lookup:
                        # Add new client to lookup table
                       self.lookup[slave] = deque([])
                       read_sockets,write_sockets,error_sockets = select.select([], [slave],[], 0)
                       for sock in write_sockets:
                           # Push index file url to client
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
                self.conn_list_lock.release()
                # If no client is connected, continue
                if len(self.lookup.keys()) == 0:
                    continue
                # Find client with minimum load
                (min_slave, min_val) = self.get_smallest_length(self.lookup)[0] 
                print "Start assigning jobs..."
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
                # If all clients are busy(at least K  tasks waiting)
                else:
                    if self.nc_event.is_set():
                        self.nc_event.clear()
                    print 'Too many task assigned , waiting...'
                    # Only when new response or new client coming in then main thread exit from hanging status
                    while not self.stop and not self.event.wait(.1) and not self.nc_event.wait(.1):
                        pass
                    if self.event.is_set():
                        self.func_lock.acquire()
                        self.handle_response()
                        self.event.clear()
                        self.func_lock.release()
                    if self.nc_event.is_set():
                        self.nc_event.clear()
                        continue
            self.stop = 1
            print 'All task done... Exit from Downloader'
            while self.server_thread.is_alive():
                self.server_thread.join(.1)
            while self.event_thread.is_alive():
                self.event_thread.join(.1)

        except :
            print "Exception in run()"
            self.stop = 1
            while self.server_thread.is_alive():
                self.server_thread.join(.1)
            while self.event_thread.is_alive():
                self.event_thread.join(.1)
            sys.exit()
