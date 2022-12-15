#!/usr/bin/python3

from io import StringIO
import pandas as pd
import json
import subprocess

def fetch_condor_machines():
    compact_result = subprocess.run(
        ["ssh", "conduit", "condor_status", "-compact"], capture_output=True, text=True)
    data = pd.read_csv(StringIO(compact_result.stdout),
                           sep='\s+', skipfooter=5, engine="python")
    return data["Machine"]

def mapping_for_machine(host):
    machineAds = subprocess.run(
        ["ssh", "conduit", "condor_status", "-json", host], capture_output=True, text=True)
    info = json.loads(machineAds.stdout)
    mapping = {}
    for slot in info:
        if 'DetectedGPUs' in slot and not 'ParentSlotId' in slot:
            detected = [name.strip() for name in slot['DetectedGPUs'].split(',')]
            for name in detected:
                snake = name.replace('-', '_').strip()
                if 'GPUs_' + snake in slot:
                    mapping[name] = slot['GPUs_' + snake]['DevicePciBusId']
                elif snake + 'DevicePciBusId' in slot:
                    mapping[name] = slot[snake + 'DevicePciBusId']
    return mapping

if __name__ == "__main__":
    import argparse
    about = """This script reads a map from "AssignedGPUs" names to the PCIe bus ids.
        """
    parser = argparse.ArgumentParser(description=about)
    parser.add_argument("--host", help="The host to map for.", default="")
    parser.add_argument("-d", "--debug", help="Enable debug output", action="store_true")
    args = parser.parse_args()
    
    if len(args.host) > 0:
        print(json.dumps({args.host: mapping_for_machine(args.host)}))
    else:
        machines = fetch_condor_machines()
        mappings = {}
        for machine in machines:
            mappings[machine] = mapping_for_machine(machine)
        print(json.dumps(mappings))
