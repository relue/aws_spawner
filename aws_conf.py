
node = {}
general = {}
#node["address"] = "ubuntu@ec2-18-197-72-212.eu-central-1.compute.amazonaws.com"
node["address"] = "ubuntu@ec2-18-197-129-16.eu-central-1.compute.amazonaws.com"

general["project_folder_remote"] = "/home/ubuntu/projects/spiegel_clf/text_classification"
general["project_folder_remote_parent"] = "/home/ubuntu/projects/spiegel_clf"
general["project_folder_local"] = "/home/simon/projects/spiegel_clf/text_classification"

#Instanztyp: x1e.xlarge #group_name:textmining_with_x1e_xlarge
#textmining_gpu

#asg_gpu_v2
general["worker_count"] = 0

#general["expName"] = "ExperimentPlan-RNN-18-06-13-18-02-1528905746"
#viele Ergebnisse
general["expName"] = "Bahn_Dataset"
general["asg_name"] = "textmining_with_x1e_xlarge"