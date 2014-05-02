
import settings
import sys

def log(s,args=None):
    if args:
        msg=(s % args)
    else:
        msg=s
    msg=msg.encode(settings.outputencoding,'ignore')
    sys.stderr.write(msg+'\n')
    sys.stderr.flush()
