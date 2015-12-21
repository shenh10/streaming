from collections import deque
import select, socket
from threading import *
import urllib

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

    def run(self):
        self.download_thread.start()
        while not self.stop:
            try:
                read_sockets,write_sockets,error_sockets = select.select([self.sock],[],[])
                for sock in read_sockets:
                    if sock == self.sock:
                        print 'Getting something from server'
                        self.socket_lock.acquire()
                        data = sock.recv(self.buffer_size)
                        print data
                        self.socket_lock.release()
                        if data.startswith('DOWNLOAD:'):
                            (command, url) = data.split(':')
                            print 'DOWNLOAD command: to download %s'%url
                            self.task_list.append(url)
            except :
                print 'Exception on slave side. exit'
                self.sock.close()
                self.stop = 1
                while self.download_thread.is_alive():
                    self.download_thread.join(.1)

    def downloader(self):
        while not self.stop:
            if len(self.task_list) != 0:
                url = self.task_list.popleft()
                try:
                    print "Downloading: %s"%url 
                    urllib.urlretrieve (url, self.repo+url.split('/')[-1])
                    self.socket_lock.acquire()
                    self.sock.send('SUCCESS:'+url)
                    self.socket_lock.release()
                    print "Success: %s"%url 
                except:
                    self.socket_lock.acquire()
                    self.sock.send('FAILURE:'+url)
                    self.socket_lock.release()
                    print "Failure: %s"%url 
