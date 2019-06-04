import aws_manager
aws = aws_manager.AwsManager()
import time
while True:
    lc=  aws.monitorInstances()
    for c in lc:
        print c
    time.sleep(5)