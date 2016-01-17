import subprocess, os, time
import m3u8
from urlparse import urljoin
"""
The file is to play video segments continously through pipe on Raspberry PI
"""
def load_m3u8(url):
    m3u8_obj = m3u8.load(url)
    file_list = []
    segments =  m3u8_obj.segments
    for segment in segments:
        duration = segment.duration
        if segment.uri.startswith('http://'):
            file_list.append(segment.uri)
        else:
            seg_url = urljoin(url, segment.uri)
            file_list.append(seg_url)
    return file_list


file_list = load_m3u8('../data/download/prog_index.m3u8')
num = len(file_list)
cmd = 'omxplayer -o hdmi /tmp/livevideo'
process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
for i in range(num):
    while (True):
        if os.path.exists(file_list[i]):
            break
        else:
            time.sleep(7)

    with open('/tmp/livevideo', 'w') as out:
        cmd = 'cat %s'%(file_list[i])
        process = subprocess.Popen(cmd.split(), stdout=out)
        output = process.communicate()[0]
        print output

#bashCommand = "../../workplace/client %s put %s %s"%(ip, source_repo, dest_repo)
#process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
#output = process.communicate()[0]
