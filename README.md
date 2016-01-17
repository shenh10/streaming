Cooperative Video Streaming on Raspberry Pi
===================================
It implements a cooperative HTTP video streaming architecture based on Paper [MicroCast: cooperative video streaming on smartphones](http://dl.acm.org/citation.cfm?id=2307643) but deployed on Raspberry Pi. Local users use cellular network to download video pieces cooperatively and share to each other with WIFI network to acheive asynchronous video playing among local machines with high avaliability and few download costs


### MircoDownloader
Implement a cooperative downloader which utilizes bandwidth inside a local p2p network. Tested with 3G for downloading and WIFI for local transmission. Working much faster than 3G along for each participants. 

- downloader.py: wrapper for both master and slave
- master.py: download master for scheduling
- slave.py : actually worker for downloading


### Run Example
- Run Master
``` bash
python downloader.py -a [ip] -p [port] -m -i "http://$source_ip:$port/all.m3u8"

```

- Run slave
``` bash
python downloader.py -a [ip] -p [port] -s -r [path/to/repo] -c ['ip1,ip2']
```
