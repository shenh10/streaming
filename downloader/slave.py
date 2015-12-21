from collections import deque
import select, socket
from threading import *
import urllib
import sys
import time
class Slave(object):
    def __init__(self, server_address, server_port, repo):
        self.task_list = deque([])
        self.server_address = server_address
        self.server_port = server_port
        self.sock = socket.create_connection((server_address, server_port))
        #self.message = message
        self.buffer_size = 2048
        self.repo = repo
        self.download_thread = Thread(target = self.downloader)
        self.socket_lock = Lock()
        self.stop = 0
        self.count = 0

    def run(self):
        self.download_thread.start()
        while not self.stop:
            try:
                read_sockets,write_sockets,error_sockets = select.select([self.sock],[],[])
                for sock in read_sockets:
                    if sock == self.sock:
                        self.socket_lock.acquire()
                        data = sock.recv(self.buffer_size)
                        if data.startswith('CLOSE'):
                            self.stop = 1
                        if data.startswith('DOWNLOAD!'):
                            (command, url) = data.split('!')
                            print 'DOWNLOAD command: to download %s'%url
                            sock.send('ACC!SUCCESS')
                            self.task_list.append(url)
                        self.socket_lock.release()
            except:
                print 'Exception on slave side. exit'
                self.sock.close()
                self.stop = 1
                while self.download_thread.is_alive():
                    self.download_thread.join(.1)
                sys.exit()

    def downloader(self):
        while not self.stop:
            if len(self.task_list) != 0:
                url = self.task_list.popleft()
                try:
                    print "Downloading: %s"%url                     
                    repo = self.repo+'/'+url.split('/')[-1]
                    time.sleep(1)
                    print repo
                    urllib.urlretrieve (url, repo)
                    self.socket_lock.acquire()
                    self.sock.send('SUCCESS!'+url)
                    self.socket_lock.release()
                    self.count += 1
                    print "Success: %s"%url 
                    print self.count
                except:
                    self.socket_lock.acquire()
                    self.sock.send('FAILURE!'+url)
                    self.socket_lock.release()
                    print "Failure: %s"%url 
