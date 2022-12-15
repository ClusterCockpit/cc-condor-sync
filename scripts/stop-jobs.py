#!/usr/bin/python3
import time
from dateutil import parser as dateparser
import requests
import json


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
        "-j", "--jobs", help="Read job file file. Default: tobestopped.json", default="tobestopped.json")
    parser.add_argument(
        "-d", "--debug", help="Enable debug output", action="store_true")
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as f:
        config = json.load(f)

    cc = CCApi(config, args.debug)
    with open("tobestopped.json") as f:
        jobs = json.load(f)['data']['jobs']['items']
        
        for job in jobs:
            startTime = int(time.mktime(dateparser.parse(job['startTime']).timetuple()))
            data = {
                "jobState": "cancelled",
                "stopTime": startTime+1,
                "cluster": job['cluster'],
                "jobId": job['jobId'],
                "startTime": startTime
            }
            cc.stopJob(job['id'], data)

