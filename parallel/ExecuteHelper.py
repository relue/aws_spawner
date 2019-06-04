from ParallelHelper import ParallelHelper
import collections
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import TextClassification

import config.models.rnn as rnnConf
import config.models.cnn as cnnConf
import config.models.fasttext as fasttextConf
import config.models.bayes as bayesConf

class ExecuteHelper:
    def updateDict(self, d, u):
        for k, v in u.iteritems():
            if isinstance(v, collections.Mapping):
                d[k] = self.updateDict(d.get(k, {}), v)
            else:
                d[k] = v
        return d

    def executeMain(self, config):
        m = TextClassification.TextClassification(config)
        result = m.result
        return result

    def batchDefine(self, prepParams, paramsDiff = None, experimentName="not defined", comment = "Batch", priority = 10):
        fullP = self.getConfig(prepParams, paramsDiff)
        ph = ParallelHelper()
        id = ph.writeNewParameters(paramsDiff, fullP, experimentName, commentText=comment, priority=priority)
        return id

    def getConfig(self, datasetConf, paramsDiff = None):
        modelName = paramsDiff["model_name"]

        if modelName == "conv":
            model_conf = cnnConf.Config.standardConf
        elif modelName == "rnn":
            model_conf = rnnConf.Config.standardConf
        elif modelName == "bayes":
            model_conf = bayesConf.Config.standardConf
        elif modelName == "fasttext":
            model_conf = fasttextConf.Config.standardConf
        elif modelName == "scikit":
            model_conf = scikitConf.Config.standardConf
        else:
            print modelName + "does not exist"
            model_conf = {}

        fullConf = self.updateDict(datasetConf, model_conf)

        if paramsDiff is not None:
            fullC = self.updateDict(fullConf, paramsDiff)
        return fullC