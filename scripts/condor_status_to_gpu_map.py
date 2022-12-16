#!/usr/bin/python3

import json

if __name__ == "__main__":
    import argparse
    about = """This script parses a `condor_status -json` dump file and spits out a GPU hash ID to PCIe address map.
        """
    parser = argparse.ArgumentParser(description=about)
    parser.add_argument("condor_status_file",
                        help="`condor_status -json` dump", default="condor_status.json")
    args = parser.parse_args()

    # open dump file
    with open(args.condor_status_file, 'r', encoding='utf-8') as f:
        condor_status = json.load(f)

    slot_gpu_map = {}
    for slot in condor_status:
        machine = slot["Machine"].split('.')[0]
        gpu_map = {}
        if machine in slot_gpu_map:
            gpu_map = slot_gpu_map[machine]
        else:
            slot_gpu_map[machine] = {}
        if not "AssignedGPUs" in slot:
            continue
        gpus = slot["AssignedGPUs"].split(',')
        for gpu_id in gpus:
            gpu = slot["GPUs_" + gpu_id.strip().replace("-", "_")]
            gpu_map[gpu["Id"]] = gpu["DevicePciBusId"]
        slot_gpu_map[machine] = gpu_map
    print(json.dumps(slot_gpu_map))
