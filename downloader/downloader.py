import m3u8
from urlparse import urljoin
import pdb
import sys
import argparse
from collections import deque
from master import Master
from slave import Slave

def load_m3u8(url):
    m3u8_obj = m3u8.load(url)
    file_list = []
    if m3u8_obj.is_variant:
        for playlist in m3u8_obj.playlists:
            sub_m3u8_obj = None
            if playlist.uri.startswith('http://'):
                file_list = load_m3u8(playlist.uri)
            else:
                sub_url = urljoin(url, playlist.uri)
                url, file_list = load_m3u8(sub_url)

    else:
        segments =  m3u8_obj.segments
        for segment in segments:
            duration = segment.duration
            if segment.uri.startswith('http://'):
                file_list.append(segment.uri)
            else:
                seg_url = urljoin(url, segment.uri)
                file_list.append(seg_url)
    return  url, file_list

def get_opts():
    parser = argparse.ArgumentParser(description="Thanks for using MircoDownloader")
    parser.add_argument("-p", "--port", help="port on listening", required = True)
    parser.add_argument("-a", "--addr", help="address on listening", required=True)
    _input = parser.add_argument("-i", "--input", help="m3u8 file source")
    repo = parser.add_argument("-r", "--repo", help="repo to store the downloaded file")
    parser.add_argument("-c", "--clients", help="points out all sibling slaves to syncup.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-m", "--master",help="run the script as master", action="store_true")
    group.add_argument("-s", "--slave",help="run the script as slave", action='store_true')
    args = parser.parse_args()
    if args.slave and args.repo is None:
         parser.error('If run in slave mode, repo is neccessary to be given.')
    if args.master and ( args.input is None ):
         parser.error('If run in master mode, stream source(-i) is neccessary to be given.')
    return args

if __name__ == '__main__':
    args = get_opts()
    if args.master == True:
        url, file_list = load_m3u8(args.input)
        print len(file_list)
        master = Master(args.addr, int(args.port), file_list, url)
        master.run()
    else:
        slave = Slave(args.addr, int(args.port), args.repo, [args.clients])
        slave.run()
    
