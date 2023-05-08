#!/usr/bin/python3
import time
from dateutil import parser as dateparser
import requests
import json
import subprocess


class CCApi:
    config = {}
    apiurl = ''
    apikey = ''
    headers = {}
    debug = False

    def __init__(self, config, debug=False):
        self.config = config
        self.apiurl = "%s/api/" % config['cc-backend']['host']
        self.apikey = config['cc-backend']['apikey']
        self.headers = {'accept': 'application/ld+json',
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer %s' % self.config['cc-backend']['apikey']}
        self.debug = debug

    def stopJob(self, id, data):
        url = self.apiurl+"jobs/stop_job/%d" % id
        r = requests.post(url, headers=self.headers, json=data)
        if r.status_code == 200:
            return r.json()
        else:
            print(data)
            print(r.status_code, r.content)
            return False
        
    def getJobs(self, filter_running=True):
        url = self.apiurl+"jobs/"
        if filter_running:
            url = url+"?state=running&items-per-page=100"
        jobs = []
        page = 1
        while True:
            r = requests.get(url + "&page=%d" % page, headers=self.headers)
            page += 1
            if r.status_code == 200:
                new_jobs = r.json()['jobs']
                if len(new_jobs) < 100:
                    jobs.extend(new_jobs)
                    return jobs
                else:
                    jobs.extend(new_jobs)
            else:
                return []

    def _getSubmitNodeId(self, globalJobId):
        job_id_parts = globalJobId.split('#')
        return self.config['htcondor']['submitnodes'][job_id_parts[0].split('.')[0]]

    def _jobIdToInt(self, globalJobId):
        job_id_parts = globalJobId.split('#')
        cluster_id, proc_id = [int(id) for id in job_id_parts[1].split('.')]
        return cluster_id << 32 | ((proc_id & 0x3FFFFFFF) << 2) | self._getSubmitNodeId(globalJobId)


    def compare_jobs(self, job, cc_job):
        print(self._jobIdToInt(job['GlobalJobId']), cc_job['jobId'])
        return self._jobIdToInt(job['GlobalJobId']) == cc_job['jobId'] and \
            abs(cc_job['startTime'] - job['EnteredCurrentStatus']) < 5
            
def get_entered_current_status(arrayId, jobId, startTime):
    conduit = "conduit" if arrayId % 10 == 0 else "conduit2"
    condor_job_id = str(jobId >> 32) + "." + str((jobId >> 2) & 0x3FFFFFFF)
    historys = subprocess.run(
        ["ssh", conduit + ".cs.uni-saarland.de", "condor_history", "-json", condor_job_id, "-limit", "1"], capture_output=True, text=True).stdout
    history = json.loads(historys)
    if len(history) > 0:
        return history[0]['EnteredCurrentStatus']
    querys = subprocess.run(
        ["ssh", conduit + ".cs.uni-saarland.de", "condor_q", "-json", condor_job_id], capture_output=True, text=True).stdout
    query = json.loads(querys)
    if len(query) > 0:
        return query[0]['EnteredCurrentStatus']
    return startTime + 1

if __name__ == "__main__":
    import argparse
    about = """This script syncs the slurm jobs with the cluster cockpit backend. It uses 
        the slurm command line tools to gather the relevant slurm infos and reads
        the corresponding info from cluster cockpit via its api. 

        After reading the data, it stops all jobs in cluster cockpit which are 
        not running any more according to slurm and afterwards it creates all new 
        running jobs in cluster cockpit. 
        """
    parser = argparse.ArgumentParser(description=about)
    parser.add_argument(
        "-c", "--config", help="Read config file. Default: config.json", default="config.json")
    parser.add_argument(
        "-d", "--debug", help="Enable debug output", action="store_true")
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as f:
        config = json.load(f)

    cc = CCApi(config, args.debug)

    condor_jobs = subprocess.run(
        ["ssh", "conduit2.cs.uni-saarland.de", "condor_q", "-json", "-all", "-glob", "-constraint", "\"JobStatus == 2\""], capture_output=True, text=True).stdout
    running_jobs = json.loads(condor_jobs)
    
    cc_jobs = cc.getJobs()
    running_job_dict = {cc._jobIdToInt(job['GlobalJobId']): (job['GlobalJobId'], job['JobCurrentStartDate']) for job in running_jobs}
    print("CC jobs:", len(cc_jobs), " Condor jobs:", len(running_job_dict))
    for cc_job in cc_jobs:
        startTime = cc_job['startTime']
        if not cc_job['jobId'] in running_job_dict or abs(startTime - running_job_dict[cc_job['jobId']][1]) > 5:
            data = {
                "jobState": "cancelled",
                "stopTime": get_entered_current_status(cc_job['arrayJobId'], cc_job['jobId'], startTime),
                "cluster": cc_job['cluster'],
                "jobId": cc_job['jobId'],
                "startTime": startTime
            }
            print(data)
            cc.stopJob(cc_job['id'], data)

