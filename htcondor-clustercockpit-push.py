#!/usr/bin/python3
# This script syncs the slurm jobs with the cluster cockpit backend. It uses
# the slurm command line tools to gather the relevant slurm infos and reads
# the corresponding info from cluster cockpit via its api.
#
# After reading the data, it stops all jobs in cluster cockpit which are
# not running any more according to slurm and afterwards it creates all new
# running jobs in cluster cockpit.
#
# -- Michael Schwarz <schwarz@uni-paderborn.de>

from dateutil import parser as dateparser
import platform
import subprocess
import json
import time
import requests
import re
import tailf


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

    def startJob(self, data):
        url = self.apiurl+"jobs/start_job/"
        r = requests.post(url, headers=self.headers, json=data)
        if r.status_code == 201:
            return r.json()
        elif r.status_code == 422:
            if self.debug:
                print(data)
                print(r.status_code, r.content)
            return False
        else:
            print(data)
            print(r)
            print(r.json())
            return False

    def stopJob(self, data):
        url = self.apiurl+"jobs/stop_job/"
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
            url = url+"?state=running"
        r = requests.get(url, headers=self.headers)
        if r.status_code == 200:
            return r.json()
        else:
            return {'jobs': []}


class CondorSync:
    condorJobData = {}
    ccData = {}
    config = {}
    debug = False
    ccapi = None
    submit_node = ''

    def __init__(self, config, debug=False):
        self.config = config
        self.debug = debug
        if 'submitnode' in config['htcondor']:
            self.submit_node = config['htcondor']['submitnode']
        else:
            self.submit_node = platform.node()

        # validate config TODO
        if "htcondor" not in config:
            raise KeyError
        if "eventlog" not in config['htcondor']:
            raise KeyError
        if "cc-backend" not in config:
            raise KeyError
        if "host" not in config['cc-backend']:
            raise KeyError
        if "apikey" not in config['cc-backend']:
            raise KeyError

        self.ccapi = CCApi(self.config, debug)

    def _exec(self, command):
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        if process.returncode == 0:
            return output.decode('utf-8')
        else:
            print("Error: ", error)
        return ""

    def _readCondorData(self):
        if self.debug:
            print("DEBUG: _readCondorData called")
        with open(self.config['htcondor']['eventlog']) as f:
            self.condorJobData = json.load(f)

    def _readCCData(self):
        if self.debug:
            print("DEBUG: _readCCBackendData called")
        self.ccData = self.ccapi.getJobs()
        if self.debug:
            print("DEBUG: ccData:", self.ccData)

    def _getAccDataForJob(self, jobid):
        raise NotImplementedError
        command = "%s -j %s --json" % (self.config['slurm']['sacct'], jobid)
        return json.loads(self._exec(command))

    def _getSubmitNodeId(self, globalJobId):
        job_id_parts = globalJobId.split('#')
        return self.config['htcondor']['submitnodes'][job_id_parts[0].split('.')[0]]

    def _jobIdInCC(self, jobid):
        for job in self.ccData['jobs']:
            if jobid == job['jobId']:
                return True
        return False

    def _jobIdToInt(self, globalJobId):
        job_id_parts = globalJobId.split('#')
        cluster_id, proc_id = [int(id) for id in job_id_parts[1].split('.')]
        return cluster_id << 32 | ((proc_id & 0x3FFFFFFF) << 2) | self._getSubmitNodeId(globalJobId)

    def _jobRunning(self, jobid):
        for job in self.slurmJobData['jobs']:
            if int(job['job_id']) == int(jobid):
                if job['job_state'] == 'RUNNING':
                    return True
        return False

    def _getACCIDsFromGRES(self, gres, nodename):
        ids = self.config['accelerators']

        nodetype = None
        for k, v in ids.items():
            if nodename.startswith(k):
                nodetype = k

        if not nodetype:
            print("WARNING: Can't find accelerator definition for node %s" %
                  nodename.strip())
            return []

        # the gres definition might be different on other clusters!
        m = re.match(r"(fpga|gpu):(\w+):(\d)\(IDX:([\d,\-]+)\)", gres)
        if m:
            family = m.group(1)
            type = m.group(2)
            amount = m.group(3)
            indexes = m.group(4)
            acc_id_list = []

            # IDX might be: IDX:0,2-3
            # first split at , then expand the range to individual items
            if len(indexes) > 1:
                idx_list = indexes.split(',')
                idx = []
                for i in idx_list:
                    if len(i) == 1:
                        idx.append(i)
                    else:
                        start = i.split('-')[0]
                        end = i.split('-')[1]
                        idx = idx + list(range(int(start), int(end)+1))
                indexes = idx

            for i in indexes:
                acc_id_list.append(ids[nodetype][str(i)])

            print(acc_id_list)
            return acc_id_list

        return []

    def _ccStartJob(self, job):
        print("INFO: Create job %s, user %s, name %s" %
              (job['GlobalJobId'], job['Owner'], job['JobBatchName'] if 'JobBatchName' in job else ''))
        jobId = self._jobIdToInt(job['GlobalJobId'])
        # for j in self.ccData['jobs']:
        #     if j['jobId'] == jobId:
        #         return

        nodelist = [job['RemoteHost'].split('@')[1]]

        # # Exclusive job?
        # if job['shared'] == "none":
        #     exclusive = 1
        # # exclusive to user
        # elif job['shared'] == "user":
        #     exclusive = 2
        # # exclusive to mcs
        # elif job['shared'] == "mcs":
        #     exclusive = 3
        # # default is shared node
        # else:
        exclusive = 0

        # read job script and environment
        # hashdir = "hash.%s" % str(job['GlobalJobId'])[-1]
        # jobscript_filename = "%s/%s/job.%s/script" % (
        #     self.config['slurm']['state_save_location'], hashdir, job['GlobalJobId'])
        # jobscript = ''
        # try:
        #     with open(jobscript_filename, 'r', encoding='utf-8') as f:
        #         jobscript = f.read()
        # except FileNotFoundError:
        #     jobscript = 'NO JOBSCRIPT'

        environment = ''
        # FIXME sometimes produces utf-8 conversion errors
        # environment_filename = "%s/%s/job.%s/environment" % (
        #     self.config['slurm']['state_save_location'], hashdir, job['GlobalJobId'])
        # try:
        #     with open(environment_filename, 'r', encoding='utf-8') as f:
        #         environment = f.read()
        # except FileNotFoundError:
        #     environment = 'NO ENV'
        # except UnicodeDecodeError:
        #     environment = 'UNICODE_DECODE_ERROR'

        # truncate environment to 50.000 chars. Otherwise it might not fit into the
        # Database field together with the job script etc.
        # environment = environment[:50000]

        # # get additional info from slurm and add environment
        # command = "scontrol show job %s" % job['GlobalJobId']
        # slurminfo = self._exec(command)
        # slurminfo = slurminfo + "ENV:\n====\n" + environment

        if job['Subproc'] > 0:
            print("WARNING: did not expect to see Subproc != 0")

        # build payload
        data = {'jobId': jobId,
                'user': job['Owner'],
                'cluster': self.config['cluster'],
                'numNodes': job['CurrentHosts'],
                'numHwthreads': job['CpusProvisioned'],
                'startTime': job['JobCurrentStartDate'],
                # 'walltime': int(job['time_limit']) * 60,
                'project': job['AccountingGroup'],
                'partition': 'main',  # job['partition'],
                'exclusive': exclusive,
                'resources': [],
                'metadata': {
                    'jobName': job['JobBatchName'] if 'JobBatchName' in job else ''
                }
                }

        # is this part of an array job?
        if job['Cluster'] > 0:
            data.update(
                {"arrayJobId": job['Cluster'] * 10 + self._getSubmitNodeId(job['GlobalJobId'])})

        num_acc = 0
        for node in nodelist:
            # begin dict
            resources = {'hostname': node.split('.')[0].strip()}

            # if a job uses a node exclusive, there are some assigned cpus (used by this job)
            # and some unassigned cpus. In this case, the assigned_cpus are those which have
            # to be monitored, otherwise use the unassigned cpus.
            # hwthreads = job['job_resources']['allocated_nodes'][str(
            #     i)]['cores']
            # cores_assigned = []
            # cores_unassigned = []
            # for k, v in hwthreads.items():
            #     if v == "assigned":
            #         cores_assigned.append(int(k))
            #     else:
            #         cores_unassigned.append(int(k))

            # if len(cores_assigned) > 0:
            #     cores = cores_assigned
            # else:
            #     cores = cores_unassigned
            # resources.update({"hwthreads": cores})

            # Get allocated GPUs if some are requested
            if 'AssignedGPUs' in job:

                acc_ids = job['AssignedGPUs'].split(',')
                if len(acc_ids) > 0:
                    num_acc = num_acc + len(acc_ids)
                    resources.update(
                        {"accelerators": [self.config['accelerators'][node][id] for id in acc_ids]})

            data['resources'].append(resources)

        # if the number of accelerators has changed in the meantime, upate this field
        data.update({"numAcc": num_acc})

        if self.debug:
            print(data)

        ccjob = self.ccapi.startJob(data)

    def _ccStopJob(self, job):
        if 'GlobalJobID' in job:
            globalJobId = job['GlobalJobId']
        else:
            globalJobId = "%s#%d.%d#%d" % (
                self.submit_node, job['Cluster'], job['Proc'], int(time.time()))
        print("INFO: Stop job %s" % globalJobId)
        jobId = self._jobIdToInt(globalJobId)

        # get search for the jobdata stored in CC
        # ccjob = {}
        # for j in self.ccData['jobs']:
        #     if j['jobId'] == jobId:
        #         ccjob = j

        # # check if job is still in squeue data
        # for job in self.slurmJobData['jobs']:
        #     if job['job_id'] == jobId:
        #         jobstate = job['job_state'].lower()
        #         endtime = job['end_time']
        #         if jobstate == 'requeued':
        #             print("Requeued job")
        #             jobstate = 'failed'

        #             if int(ccjob['startTime']) >= int(job['end_time']):
        #                 print("squeue correction")
        #                 # For some reason (needs to get investigated), failed jobs sometimes report
        #                 # an earlier end time in squee than the starting time in CC. If this is the
        #                 # case, take the starting time from CC and add ten seconds to the starting
        #                 # time as new end time. Otherwise CC refuses to end the job.
        #                 endtime = int(ccjob['startTime']) + 1

        # else:
        #     jobsAcctData = self._getAccDataForJob(jobid)['jobs']
        #     for j in jobsAcctData:
        #         if len(j['steps']) > 0 and j['steps'][0]['time']['start'] == ccjob['startTime']:
        #             jobAcctData = j
        #     jobstate = jobAcctData['state']['current'].lower()
        #     endtime = jobAcctData['time']['end']

        #     if jobstate == "node_fail":
        #         jobstate = "failed"
        #     if jobstate == "requeued":
        #         print("Requeued job")
        #         jobstate = "failed"

        #         if int(ccjob['startTime']) >= int(jobAcctData['time']['end']):
        #             print("sacct correction")
        #             # For some reason (needs to get investigated), failed jobs sometimes report
        #             # an earlier end time in squee than the starting time in CC. If this is the
        #             # case, take the starting time from CC and add ten seconds to the starting
        #             # time as new end time. Otherwise CC refuses to end the job.
        #             endtime = int(ccjob['startTime']) + 1

        jobstate_map = {4: "cancelled", 5: "completed", 9: "cancelled",
                        10: "stopped", 12: "stopped", 24: "failed"}
        jobstate = jobstate_map[job['TriggerEventTypeNumber']]

        data = {
            'jobId': jobId,
            'cluster': self.config['cluster'],
            'jobState': jobstate
        }
        if 'ToE' in job:
            if isinstance(job['ToE']['When'], int):
                data['stopTime'] = job['ToE']['When']
            else:
                data['stopTime'] = int(time.mktime(
                    dateparser.parse(job['ToE']['When']).timetuple()))
        else:
            data['stopTime'] = int(time.mktime(
                dateparser.parse(job['EventTime']).timetuple()))

        if 'JobCurrentStartDate' in job:
            data['startTime'] = job['JobCurrentStartDate']

        if self.debug:
            print(data)

        self.ccapi.stopJob(data)

    def _convertNodelist(self, nodelist):
        # Use slurm to convert a nodelist with ranges into a comma separated list of unique nodes
        if re.search(self.config['node_regex'], nodelist):
            command = "scontrol show hostname %s | paste -d, -s" % nodelist
            retval = self._exec(command).split(',')
            return retval
        else:
            return []

    def _handleEvent(self, event):
        # event codes: https://htcondor.readthedocs.io/en/latest/codes-other-values/job-event-log-codes.html
        if event['EventTypeNumber'] == 28:  # JobAdInformationEvent
            if event['TriggerEventTypeNumber'] == 1:  # Execute
                self._ccStartJob(event)
            elif event['TriggerEventTypeNumber'] == 4 or event['TriggerEventTypeNumber'] == 5 or \
                    event['TriggerEventTypeNumber'] == 9 or event['TriggerEventTypeNumber'] == 10 or \
                    event['TriggerEventTypeNumber'] == 12 or event['TriggerEventTypeNumber'] == 24:
                self._ccStopJob(event)

    def sync(self, limit=200, jobid=None, direction='both'):
        if self.debug:
            print("DEBUG: sync called")
            print("DEBUG: jobid %s" % jobid)

        # self._readCCData()

        with tailf.Tail(self.config['htcondor']['eventlog']) as tail:
            remaining = ""
            while True:
                for event in tail:
                    if isinstance(event, bytes):
                        eventlog = remaining + event.decode("utf-8")
                        decoder = json.JSONDecoder()
                        pos = 0
                        while True:
                            try:
                                event, pos = decoder.raw_decode(eventlog, pos)
                                remaining = ""
                                pos += 1
                                self._handleEvent(event)
                            except json.JSONDecodeError:
                                remaining = eventlog[pos:]
                                break
                    elif event is tailf.Truncated:
                        print("File was truncated")
                    else:
                        assert False, "unreachable"  # currently. more events may be introduced later
                time.sleep(5)  # save CPU cycles

        with open(self.config['htcondor']['eventlog'], 'r', encoding='utf-8') as f:
            eventlog = f.read()

            decoder = json.JSONDecoder()
            pos = 0
            while True:
                try:
                    event, pos = decoder.raw_decode(eventlog, pos)
                    pos += 1
                    self._handleEvent(event)
                except json.JSONDecodeError:
                    break
        # self._readCondorData()
        return

        # Abort after a defined count of sync actions. The intend is, to restart this script after the
        # limit is reached. Otherwise, if many many jobs get stopped, the script might miss some new jobs.
        sync_count = 0

        # iterate over cc jobs and stop them if they have already ended
        if direction in ['both', 'stop']:
            for job in self.ccData['jobs']:
                if jobid:
                    if int(job['jobId']) == int(jobid) and not self._jobRunning(job['jobId']):
                        self._ccStopJob(job['jobId'])
                        sync_count = sync_count + 1
                else:
                    if not self._jobRunning(job['jobId']):
                        self._ccStopJob(job['jobId'])
                        sync_count = sync_count + 1
                if sync_count >= limit:
                    print("INFO: sync limit (%s) reached" % limit)
                    break

        sync_count = 0
        # iterate over running jobs and add them to cc if they are still missing there
        if direction in ['both', 'start']:
            for job in self.slurmJobData['jobs']:
                # Skip this job if the user does not want the metadata of this job to be submitted to ClusterCockpit
                # The text field admin_comment is used for this. We assume that this field contains a comma seperated
                # list of flags.
                if "disable_cc_submission" in job['admin_comment'].split(','):
                    print(
                        "INFO: Job %s: disable_cc_sumbission is set. Continue with next job" % job['job_id'])
                    continue
                # consider only running jobs
                if job['job_state'] == "RUNNING":
                    if jobid:
                        if int(job['job_id']) == int(jobid) and not self._jobIdInCC(job['job_id']):
                            self._ccStartJob(job)
                            sync_count = sync_count + 1
                    else:
                        if not self._jobIdInCC(job['job_id']):
                            self._ccStartJob(job)
                            sync_count = sync_count + 1
                if sync_count >= limit:
                    print("INFO: sync limit (%s) reached" % limit)
                    break


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
    parser.add_argument("-j", "--jobid", help="Sync this jobid")
    parser.add_argument(
        "-l", "--limit", help="Stop after n sync actions in each direction. Default: 200", default="200", type=int)
    parser.add_argument("--direction", help="Only sync in this direction",
                        default="both", choices=['both', 'start', 'stop'])
    args = parser.parse_args()

    # open config file
    if args.debug:
        print("DEBUG: load config file: %s" % args.config)
    with open(args.config, 'r', encoding='utf-8') as f:
        config = json.load(f)
    if args.debug:
        print("DEBUG: config file contents:")
        print(config)

    s = CondorSync(config, args.debug)
    s.sync(args.limit, args.jobid, args.direction)
