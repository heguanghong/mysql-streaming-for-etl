__author__ = 'hgh'

#!/usr/local/bin/python
# -*- coding: UTF-8 -*-

from daemon import Daemon
import sys
from mysql_streaming_core import MySQLStreaming

class StartSqlStreaming(Daemon):
        def run(self):
                #self.task = MySQLStreaming(self.daemon_code )

                self.task.producer()


if __name__ == '__main__':
        runType = 'normal'
        daemon_code = 'default'
        if len(sys.argv) == 3 or len(sys.argv) == 4:
                daemon_code = sys.argv[2]
                if len(sys.argv) == 4:
                        if sys.argv[3] == 'fast':
                            runType = 'fast'
        else:
                print "Usage: python mysql-streaming start|stop|restart  daemon_code [normal|fast]"
                print "        daemon_code must be unique !"
                exit(1)

        agent = StartSqlStreaming(daemon_code = daemon_code, pid_file = 'sql-straming.pid', runType = runType)
        if sys.argv[1]=='start':
                agent.start()
        elif sys.argv[1]=='stop':
                print "0" , agent.task
                print  agent.task.stream
                agent.stop()

        elif sys.argv[1]=='restart':
                agent.restart()
        elif sys.argv[1]=='status':
                agent.status()
        else:
            print 'python mysql-streaming start|stop|restart|status  daemon_code [normal|fast]'

