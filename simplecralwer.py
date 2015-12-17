#!/usr/bin/python
import re, urllib
import smtplib
import json

textfile = file('depth_1','wt')
print "Enter the URL you wish to crawl.."
print 'Usage  - "http://phocks.org/stumble/creepy/" <-- With the double quotes'
#myurl = input("@> ")
myurl = "http://10.10.10.169:8081/topology.html?id=sdk_collect-39-1450268927"
crawlUrl =  "http://10.10.10.169:8081"
api = crawlUrl + "/api/v1/topology/sdk_collect-39-1450268927?sys=false"
read = urllib.urlopen(api).read()
print read
j = json.loads(read)
print type(j)
print j['topologyStats'][2]
print "\n\n"
print j['spouts']

print "\n\n"
print j['bolts']

tablerow = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
tablerow[0] = "g_coupon_bind_siting"
tablerow[1] = "0"
tablerow[2] = "0"
tablerow[3] = "0"
tablerow[4] = "0"
tablerow[5] = "0"
tablerow[6] = "0"
tablerow[7] = j['topologyStats'][2]['acked']
tablerow[8] = j['topologyStats'][2]['failed']
tablerow[9] = j['spouts'][0]['acked']
tablerow[10] = j['spouts'][0]['failed']
tablerow[11] = j['bolts'][0]['acked']
tablerow[12] = j['bolts'][0]['failed']
tablerow[13] = j['bolts'][0]['processLatency']
tablerow[14] = j['bolts'][1]['acked']
tablerow[15] = j['bolts'][1]['failed']
tablerow[16] = j['bolts'][1]['processLatency']
content = ""
f = open("resource/template.html")
line = f.readline()
while line:
		content = content + line
	        line = f.readline()
f.close()
content = content + "<tr>"
for i in tablerow:
	print i
	content = content + "<td>" + str(i) + "</td>"
content = content + "</tr>"
print content
sender = 'coco@zhangwenchao177.jx.diditaxi.com'
receivers = ['695611691@qq.com']
message = """From: From coco <coco@zhangwenchao177.jx.diditaxi.com>
To: To chenyangzhi <695611691@qq.com>
MIME-Version: 1.0
Content-type: text/html
Subject: SMTP HTML e-mail test

This is an e-mail message to be sent in HTML format
"""
htmlEnd = """
</table>
</body>
</html>
"""

message = message + content + htmlEnd
try:
	smtpObj = smtplib.SMTP('localhost')
      	smtpObj.sendmail(sender, receivers, message)
	print "Successfully sent email"
except SMTPException:
	print "Error: unable to send email"
#print json.dumps(read,separators=(',',':'))	


#for i in re.findall('''href=["'](.[^"']+)["']''', urllib.urlopen(myurl).read(), re.I):
#	print i  
#	for ee in re.findall('''href=["'](.[^"']+)["']''', urllib.urlopen(crawlUrl + i).read(), re.I):
#		print ee
#		textfile.write(ee+'\n')
#		textfile.close()
