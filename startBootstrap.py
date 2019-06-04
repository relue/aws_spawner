import aws_manager
import os
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import parallel.ParallelHelper as ph
pho = ph.ParallelHelper()
pho.clearSlaves()
for filename in os.listdir("logs"):
    os.remove("logs/"+filename)

aws = aws_manager.AwsManager()
instances = aws.startBootstrap()

while True:
    lc=  aws.monitorInstances()
    for c in lc:
        print c
    time.sleep(5)

