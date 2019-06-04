import boto3
import time
import bootstrap_instance
import aws_conf
import os

class AwsManager:
    def __init__(self):
        self.session = boto3.Session(region_name='eu-central-1',
                               aws_access_key_id="BKIAI6SXJ4UVZTHWY7CQ",
                               aws_secret_access_key="wAs1H0yljv2AGMWf1PMpgEpvHuu/m3r/dKmCj6hh")

        self.client = self.session.client('autoscaling', region_name='eu-central-1')
        '''
        self.client = boto3.client('autoscaling',
                     aws_access_key_id="vAs1H0yljv2AGMWf1PMpgEpvHuu/m3r/dKmCj6hh",
                     aws_secret_access_key="AKIAI6SXJ4UVZTHWY7CQ",
                     region_name="eu-central-1"
                     )
        '''

    def changeWorkerCount(self, count):
        self.workerCount = count
        response = self.client.set_desired_capacity(
            AutoScalingGroupName=aws_conf.general["asg_name"],
            DesiredCapacity=self.workerCount,
            HonorCooldown=False
        )

    def getInstanceObject(self,id):
        ec2 = self.session.resource('ec2')
        instance = ec2.Instance(id)

        #describe_instance_status
        #status_detailed = ec2.Instancestatus(id)
        return instance

    def check_health(self,instance):

        if instance.state["Name"] == "running":
            print instance.state["Name"]
            return True
        else:
            return False

    def get_instances(self,asg):
        response = self.client.describe_auto_scaling_instances()
        found = []
        for instance in response["AutoScalingInstances"]:
            instObj = self.getInstanceObject(instance["InstanceId"])
            if instance["AutoScalingGroupName"] == aws_conf.general["asg_name"]:
                if self.check_health(instObj):
                    found.append(instObj)
        return found

    def getIps(self,instances):
        ips = []
        for inst in instances:
            ips.append(inst.public_ip_address)
        return ips

    def getGroupInstances(self):
        asg = aws_conf.general["asg_name"]
        groupInstances = self.get_instances(asg)
        return groupInstances

    def startBootstrap(self):
        workerCount = aws_conf.general["worker_count"]
        self.changeWorkerCount(workerCount)

        ids = []
        while True:
            groupInstances = self.getGroupInstances()
            upLen = len(groupInstances)
            ids = [inst.id for inst in groupInstances]
            if workerCount == upLen:
                print "all instances up first"
                waiter = self.session.client('ec2').get_waiter('system_status_ok')
                if len(ids) > 0:
                    waiter.wait(InstanceIds=ids, Filters=[{"Name": "system-status.reachability", "Values":["passed"]}])
                    print("The instances are now reachable!")
                break
            else:
                print str(upLen)+ " of "+str(workerCount)+" instances up"
            #print self.session.client('ec2').describe_instance_status()
            time.sleep(1)
        commands = []
        for inst in groupInstances:
            #print getIps(groupInstances)
            bs = bootstrap_instance.BootstrapInstance(inst)
            command = bs.setupInstance()
            commands.append(command)

        #bs.run_command(commands, isCall=True)
        return groupInstances

    def monitorInstances(self):
        groupInstances = self.getGroupInstances()
        contentList = []
        for inst in groupInstances:
            bs = bootstrap_instance.BootstrapInstance(inst)
            contentList.append(bs.getInstanceLog())
        return contentList

    def getFileFromBucket(self, filenameBucket,filenameLocal):
        client = self.session.client('s3')
        obj = client.get_object(Bucket='infomotion-dataset-cache', Key=filenameBucket)
        newFile = open(filenameLocal, "wb")
        # write to file
        newFile.write(obj['Body'].read())

    def getBucket(self):
        self.getFileFromBucket('tokenized_texts_10000_spiegel.pkl','data/pickle/test123')
        self.syncBucketWithLocal()
        s3 = self.session.resource('s3')
        file = 'data/pickle/tokenized_texts_10001_spiegel.pkl'
        data = open(file, 'rb')
        object = s3.Object('infomotion-dataset-cache', 'tokenized_texts_10001_spiegel.pkl')
        return object.put(Body=data)

    def get_s3_list(self):
        s3 = self.session.resource('s3')
        your_bucket = s3.Bucket('infomotion-dataset-cache')
        fileList = []
        for s3_file in your_bucket.objects.all():
            fileList.append(s3_file.key)
        return fileList

    def pushFileToS3(self, filenameLocal, filenameBucket):
        s3 = self.session.resource('s3')
        data = open(filenameLocal, 'rb')
        object = s3.Object('infomotion-dataset-cache', filenameBucket)
        return object.put(Body=data)

    def syncCacheFiles(self, folder, cachekey, filePrefixes, download):
        for filePrefix in filePrefixes:
            filename = filePrefix+cachekey
            listS3 = self.get_s3_list()
            listLocal = os.listdir(folder)
            isInLocal = filename in listLocal
            isInS3 = filename in listS3
            if not download:
                if isInLocal and not isInS3:
                    print filename+" pushed"
                    success = self.pushFileToS3(folder+filename, filename)
            else:
                if isInS3 and not isInLocal:
                    print filename + " pulled"
                    success = self.getFileFromBucket(filename, folder+filename)

    def terminateInstances(self, ids):
        groupInstances = self.getGroupInstances()
        desired = len(groupInstances)
        self.changeWorkerCount(desired - len(ids))
        gps = self.session.client('ec2').terminate_instances(InstanceIds=ids)


