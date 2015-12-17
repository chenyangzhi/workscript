#!/usr/bin/python

import smtplib
sender = 'coco@zhangwenchao177.jx.diditaxi.com'
receivers = ['695611691@qq.com']
message = """From: From coco <coco@zhangwenchao177.jx.diditaxi.com>
To: To chenyangzhi <695611691@qq.com>
MIME-Version: 1.0
Content-type: text/html
Subject: SMTP HTML e-mail test

This is an e-mail message to be sent in HTML format
</table>
</body>
</html>
"""
content = ""
f = open("resource/template.html")           
line = f.readline()         
while line:
	line = f.readline()
	content = content + line
	
f.close()
print content
message = message + content
try:
   smtpObj = smtplib.SMTP('localhost')
   smtpObj.sendmail(sender, receivers, message)
   print "Successfully sent email"
except SMTPException:
   print "Error: unable to send email"
