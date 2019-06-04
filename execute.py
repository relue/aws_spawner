import subprocess
import aws_conf
import threading
import subprocess
import Queue
import time

class AsyncLineReader(threading.Thread):
    def __init__(self, fd, outputQueue):
        threading.Thread.__init__(self)

        assert isinstance(outputQueue, Queue.Queue)
        assert callable(fd.readline)

        self.fd = fd
        self.outputQueue = outputQueue

    def run(self):
        map(self.outputQueue.put, iter(self.fd.readline, ''))

    def eof(self):
        return not self.is_alive() and self.outputQueue.empty()

    @classmethod
    def getForFd(cls, fd, start=True):
        queue = Queue.Queue()
        reader = cls(fd, queue)

        if start:
            reader.start()

        return reader, queue


def callProcess(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdoutReader, stdoutQueue) = AsyncLineReader.getForFd(process.stdout)
    (stderrReader, stderrQueue) = AsyncLineReader.getForFd(process.stderr)

    # Keep checking queues until there is no more output.
    while not stdoutReader.eof() or not stderrReader.eof():
        # Process all available lines from the stdout Queue.
        while not stdoutQueue.empty():
            line = stdoutQueue.get()
            print '' + repr(line)

            # Do stuff with stdout line.

        # Process all available lines from the stderr Queue.
        while not stderrQueue.empty():
            line = stderrQueue.get()
            print '' + repr(line)

            # Do stuff with stderr line.

        # Sleep for a short time to avoid excessive CPU use while waiting for data.
        time.sleep(0.05)

    print "Waiting for async readers to finish..."
    stdoutReader.join()
    stderrReader.join()

    # Close subprocess' file descriptors.
    process.stdout.close()
    process.stderr.close()

    print "Waiting for process to exit..."
    returnCode = process.wait()

    if returnCode != 0:
        raise subprocess.CalledProcessError(returnCode, command)

def run_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    for line in iter(p.stdout.readline, b''):
        print '>>> {}'.format(line.rstrip())

projectFolder = "/home/simon/projects/spiegel_clf"

updateRemote = "rsync --exclude-from='"+projectFolder+"/text_classification/aws/rsync_exclude.txt' -r -a -v --progress -e ssh "+projectFolder +" "+ aws_conf.node["address"]+":"+aws_conf.node["remotefolder"]
command = updateRemote+" && ssh "+ aws_conf.node["address"]+" 'cd /home/ubuntu/projects/spiegel_clf/text_classification && python SingleExecutor.py'"
print command
callProcess(command)




