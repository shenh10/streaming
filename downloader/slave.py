from collections import deque
import select, socket
from threading import *
import urllib
import sys
import time
import subprocess
class Slave(object):
    def __init__(self, server_address, server_port, repo, sibling_ip):
        self.task_list = deque([])
        self.server_address = server_address
        self.server_port = server_port
        print server_address, server_port
        self.sock = socket.create_connection((server_address, server_port))
        #self.message = message
        self.buffer_size = 2048
        self.repo = repo
        self.download_thread = Thread(target = self.downloader)
        self.sibling_ip = sibling_ip
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
                        if data.startswith('INDEX'):
                            (command, url) = data.split('!')
                            print 'To download index file %s'%url
                            repo = self.repo+'/'+url.split('/')[-1]
                            urllib.urlretrieve (url, repo)
                            sock.send('ACC!SUCCESS')
                        if data.startswith('DOWNLOAD!'):
                            (command, url) = data.split('!')
                            print 'Task : To Download %s'%url
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
       #     try:
            if len(self.task_list) != 0:
                url = self.task_list.popleft()
                #try:
                print "Downloading: %s"%url                     
                repo = self.repo+'/'+url.split('/')[-1]
                urllib.urlretrieve (url, repo)
                self.socket_lock.acquire()
                self.sock.send('SUCCESS!'+url)
                self.socket_lock.release()
                self.count += 1
                print "Success: %s"%url 
                print "Sending to peers......"
                if self.sibling_ip is not None:
                    print self.sibling_ip
                    for ip in self.sibling_ip:
                        print ip, repo, repo
                        self.call_subprocess(ip, repo, repo)
                print "Sended to peers......"
                print self.count
        #    except:
         #       self.socket_lock.acquire()
          #      self.sock.send('FAILURE!'+url)
           #     self.socket_lock.release()
            #    print "Failure: %s"%url 

    def call_subprocess(self, ip, source_repo, dest_repo):
        bashCommand = "../../workplace/client %s put %s %s"%(ip, source_repo, dest_repo)
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output = process.communicate()[0]
