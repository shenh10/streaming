#/bin/bash
rm -rf ../data/download/*
python downloader.py -a 192.168.1.102 -p 5000 -s -r /home/pi/video_streaming/data/download -c '192.168.1.103'
