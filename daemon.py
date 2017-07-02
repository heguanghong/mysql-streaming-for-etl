# -*- coding: UTF-8 -*-

## linux daemon
# Core modules
#

import atexit
import os
import sys
import time
import signal
from mysql_streaming_core import MySQLStreaming

class Daemon(object):

        def __init__(self, daemon_code, pid_file, runType='release', stdin=os.devnull,stdout=os.devnull,
                     stderr=os.devnull,home_dir='.', umask=0, verbose=1):
                self.stdin = stdin
                self.daemon_code = daemon_code

                if runType=='fast':
                        self.stdout = stdout
                        self.stderr = stderr
                else:
                        self.stdout = './' + daemon_code + '-daemon-out.log'    #stdout
                        self.stderr = './' + daemon_code + '-daemon-err.log'    #stderr

                self.pidfile = daemon_code + "-" + pid_file
                self.home_dir = home_dir
                self.verbose = verbose
                self.umask = umask
                self.daemon_alive = True
                self.task = MySQLStreaming(self.daemon_code )

        def daemonize(self):
                try:
                        pid = os.fork()
                        if pid > 0:
                            sys.exit(0)
                except OSError, e:
                        sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                        sys.exit(1)
                # Decouple from parent environment
                os.chdir(self.home_dir)
                os.setsid()
                os.umask(self.umask)

                # Do second fork
                try:
                        pid = os.fork()
                        if pid > 0:
                            sys.exit(0)
                except OSError, e:
                        sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
                        sys.exit(1)

                # Redirect standard file descriptors
                sys.stdout.flush()
                sys.stderr.flush()
                si = file(self.stdin, 'r')
                so = file(self.stdout, 'a+')
                if self.stderr:
                        se = file(self.stderr, 'a+', 0)
                else:
                        se = so

                os.dup2(si.fileno(), sys.stdin.fileno())
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())
                # Write pidfile
                atexit.register(self.delpid)  # Make sure pid file is removed if we quit
                pid = str(os.getpid())
                file(self.pidfile, 'w+').write("%s\n" % pid)

        def delpid(self):
                os.remove(self.pidfile)

        def start(self, *args, **kwargs):
                # Check for a pidfile to see if the daemon already runs
                pid = self.get_pid()

                if pid:
                        message = "pidfile %s already exists. Is it already running?\n"
                        sys.stderr.write(message % self.pidfile)
                        sys.exit(1)

                # Start the daemon
                self.daemonize()
                self.run(*args, **kwargs)

        def get_pid(self):
                try:
                        pf = file(self.pidfile, 'r')
                        pid = int(pf.read().strip())
                        pf.close()
                except IOError:
                        pid = None
                except SystemExit:
                        pid = None
                return pid

        def stop(self):
                print  "1",self.task
                print self.task.stream
                if self.task.stream != None:
                        print "close streaming..."
                        self.task.stream.close()
                # Get the pid from the pidfile
                pid = self.get_pid()

                if not pid:
                        message = "pidfile %s does not exist. Not running?\n"
                        sys.stderr.write(message % self.pidfile)

                        # Just to be sure. A ValueError might occur if the PID file is
                        # empty but does actually exist
                        if os.path.exists(self.pidfile):
                                os.remove(self.pidfile)

                        return  # Not an error in a restart

                # Try killing the daemon process
                try:
                        i = 0
                        while 1:
                                os.kill(pid, signal.SIGTERM)
                                time.sleep(0.5)
                                i = i + 1
                                if i % 5 == 0:
                                        os.kill(pid, signal.SIGHUP)

                except OSError, err:
                        err = str(err)
                        if err.find("No such process") > 0:
                                if os.path.exists(self.pidfile):
                                    os.remove(self.pidfile)
                        else:
                                print str(err)
                                sys.exit(1)

        def restart(self):
                """
                Restart the daemon
                """
                self.stop()
                self.start()

        def status(self):
                pid = self.get_pid()
                if pid and os.path.exists('/proc/%d' % pid):
                        print 'umagent (', pid,') is running!'
                else:
                        print 'umagent is not running!'
                return pid and os.path.exists('/proc/%d' % pid)

        def run(self):
                """
                You should override this method when you subclass Daemon.
                It will be called after the process has been
                daemonized by start() or restart().
                """
