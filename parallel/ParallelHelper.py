import pymongo
import datetime
import socket
import experimentConf

import sys

class ParallelHelper:
    mongoConnection = "mongodb://simon:asdf1asdf@ds155473.mlab.com:55473/jobs"
    def __init__(self):
        self.client = pymongo.MongoClient(self.mongoConnection)
        self.db = self.client.jobs
        collectionName = experimentConf.experimentParams["projectName"]
        self.collectionObj = self.db[collectionName]

    def getNewParameters(self, expName = ""):
        dictSearch = {'status': "new"}
        if len(expName) > 0:
            dictSearch["experimentName"] = expName
        one = self.collectionObj.find(dictSearch)
        data = False
        if one.count():
            data = one.sort("priority",pymongo.DESCENDING).limit(1)[0]
        params = False
        if data:
            id = data["_id"]
            params = data["paramsFull"]
            self.collectionObj.update({'_id':id},{"$set":{'status':'running', 'host':socket.gethostname()}},upsert=False)
        return data, params

    def getJobByState(self, expname):
        objs = self.collectionObj.find({'experimentName': expname})
        jobs = {}
        jobs["new"] = []
        jobs["running"] = []
        jobs["finished"] = []
        jobs["failed"] = []
        for obj in objs:
            jobs[obj["status"]].append(obj)

        return jobs

    def writeNewParameters(self, params, fullP, experimentName, commentText="", priority = 1):
        now = datetime.datetime.now()
        parameter = {
            "experimentName": experimentName,
            "paramsDiff": params,
            "paramsFull": fullP,
            "status": "new",
            "date": now.strftime("%Y_%m_%d_%H_%M"),
            "dateFeedback": "",
            "results": "",
            "resultsTemp": "",
            "comment":commentText,
            "priority": priority,
            "host": "",
            "errors": ""
        }
        id = self.collectionObj.insert_one(parameter).inserted_id
        return id

    def registerSlave(self, slaveIp, awsId):
        collection = experimentConf.experimentParams["projectName"]+"_slaves"
        now = datetime.datetime.now()
        parameter = {
            "slaveIP": slaveIp,
            "awsId": awsId,
            "lastChecked":now,
            "birthTime":now,
            "status": "created",
            "date_finished": "",
            "finished_jobs": 0
        }
        id = self.db[collection].insert_one(parameter).inserted_id
        return id

    def updateSlave(self, id, jobC, lastParam="", comment=""):
        collection = experimentConf.experimentParams["projectName"] + "_slaves"
        now = datetime.datetime.now()
        parameter = {
            "lastChecked": now,
            "status": "pending",
            "finished_jobs":jobC,
            "last_param": lastParam,
            "comment": comment
        }
        self.db[collection].update({'_id': id}, {"$set": parameter}, upsert=False)

    def unregisterSlave(self, id, lastParam ="", error = ""):
        collection = experimentConf.experimentParams["projectName"] + "_slaves"
        now = datetime.datetime.now()
        parameter = {
            "lastChecked": now,
            "status": "finished",
            "date_finished": now,
            "last_param": lastParam,
            "comment": error,
            "error": error
        }
        self.db[collection].update({'_id':id},{"$set":parameter}, upsert=False)

    def clearSlaves(self):
        collection = experimentConf.experimentParams["projectName"] + "_slaves"
        self.db[collection].drop()

    def checkSlaves(self):
        collection = experimentConf.experimentParams["projectName"] + "_slaves"
        one = collection.find({'status': "new"})

        return one

    def writeError(self, parameterJob, msg, elapsed):
        id = parameterJob["_id"]
        now = datetime.datetime.now()
        self.collectionObj.update({'_id':id},{"$set":{"status": "failed", "elapsed": elapsed,  "errors": msg, "dateFeedback": now.strftime("%Y_%m_%d_%H_%M")}}, upsert=False)

    def writeResult(self, parameterJob, results, elapsed):
        id = parameterJob["_id"]
        now = datetime.datetime.now()
        self.collectionObj.update({'_id':id},{"$set":{'results':results, "elapsed": elapsed, "status": "finished",  "dateFeedback": now.strftime("%Y_%m_%d_%H_%M")}},upsert=False)

    def writeTempResult(self, parameterJob, results, estTimeRemain):
        id = parameterJob["_id"]
        now = datetime.datetime.now()
        self.collectionObj.update({'_id': id}, {
            "$set": {'resultsTemp': results, 'estTimeRemain': estTimeRemain, "status": "updated",
                     "dateFeedback": now.strftime("%Y_%m_%d_%H_%M")}}, upsert=False)
