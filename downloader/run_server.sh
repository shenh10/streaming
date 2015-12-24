#/bin/bash
source_ip=192.168.1.104
python downloader.py -a 192.168.1.102 -p 5000 -m -i "http://$source_ip:8080/all.m3u8" 
