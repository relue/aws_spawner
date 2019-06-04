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

class BootstrapInstance:
    instanceData = {}
    def __init__(self, inst):
        self.instanceData["id"] = inst.id
        self.instanceData["ssh_address"] = "ubuntu@"+inst.public_ip_address
        self.instanceData["project_folder_remote"] = aws_conf.general["project_folder_remote"]
        self.instanceData["project_folder_remote_parent"] = aws_conf.general["project_folder_remote_parent"]
        self.instanceData["project_folder_local"] = aws_conf.general["project_folder_local"]


    def setupInstance(self):
        setId = "echo "+self.instanceData["id"]+" >> aws/statelog/instanceId"
        gotoProject = "cd "+self.instanceData["project_folder_remote"]
        dirCreateCommand = self.createFolders()
        syncInstance = self.syncInstance()
        preInstall = self.getPreInstall()

        run = "python SingleExecutor.py "+self.instanceData["id"]
        if len(aws_conf.general["expName"]) > 0:
            run += " " + aws_conf.general["expName"]
        runCommand = "ssh -o \"StrictHostKeyChecking no\" "+ self.instanceData["ssh_address"]+\
                      " '"+gotoProject+" && "+dirCreateCommand+" ; "+setId+" &&  "+preInstall+" &&  "+run+"'"

        preInitSSH = "ssh -o \"StrictHostKeyChecking no\" "+ self.instanceData["ssh_address"]+ " ' cd ~'"



        self.run_command(preInitSSH, isCall=True)

        self.run_command(syncInstance, isCall=True)
        print "finish sync"

        self.run_command(runCommand, isCall=False)
        print "start slave"


    def run_command(self, command, isCall=False):
        f = open('logs/log'+self.instanceData["id"]+'.txt', 'a+')

        print command
        if isCall:
            p = subprocess.call(command, stdout=f, stderr=subprocess.STDOUT, shell=True)
        else:
            p = subprocess.Popen([command], stdin=None, stdout=None, stderr=None, close_fds=True,shell=True)

    def createFolders(self):
        f = open('empty_folders.txt', 'a+')
        command = "mkdir "
        for line in f:
            line = line.replace('\n', ' ').replace('\r', '')
            command += "" + line
        return command

    def syncInstance(self):
        updateRemote = "rsync --exclude-from='" + self.instanceData["project_folder_local"]  + "/aws/rsync_exclude.txt'" \
                       " -r -a -v --progress -e \"ssh -o StrictHostKeyChecking=no\" " + self.instanceData["project_folder_local"]  + " " + \
                       self.instanceData["ssh_address"] + ":" + self.instanceData["project_folder_remote_parent"]
        #print updateRemote
        return updateRemote

    def callProcess(self, command):
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

    def getPreInstall(self):
        listC = [
            "yes | pip install boto3",
            "pip install spacy",
            "python -m spacy download de"
        ]
        return " && ".join(listC)

    def getInstanceLog(self):
        remote_logname = " aws/logs/slaveExec.log"
        local_logname = "logs/executor_" + self.instanceData["id"] + ".log"
        command = "ssh -o \"StrictHostKeyChecking no\" " + self.instanceData["ssh_address"] + " ' "\
                  +"cd "+self.instanceData["project_folder_remote"]+" && cat " +remote_logname+"' > "+local_logname+""
        #print command
        self.run_command(command, isCall=True)
        content = open(local_logname, 'r').read()
        return content




