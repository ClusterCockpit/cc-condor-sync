# HTCondor to ClusterCockpit Sync
## HTCondor ClassAdLog Plugin

### Building
Requirements:
A build environment reasonably similar to the submission nodes (might want to use the HTCondor nmi build docker containers).

Use CMake to configure the project.
```bash
mkdir build ; cd build
cmake .. -DCONDOR_SRC=<path/to/htcondor> -DCONDOR_BUILD=<path/to/htcondor/build> -DCMAKE_BUILD_TYPE=Release
```

### Configuration
The target system will need the corresponding `curl` package installed.

Adapt and add to `condor_config.local` or any other HTCondor config file:
```
SCHEDD.PLUGINS = $(SCHEDD.PLUGINS) /path/to/libhtcondor_cc_sync_plugin.so

CCSYNC_URL=<ClusterCockpit-URL>
CCSYNC_APIKEY=<API-Key>
CCSYNC_CLUSTER_NAME=<ClusterCockpit's cluster name this submit node works for>
CCSYNC_GPU_MAP=/path/to/gpu_map.json
CCSYNC_SUBMIT_ID=<Unique submission node id, expected to be in 0..3 (see #globalJobIdToInt)>
```

`gpu_map.json` is expected in the format and can be generated with `condor_status_to_gpu_map.py <path/to/condor_status.json>`, where `condor_status.json` is generated by calling `condor_status -json > condor_status.json` on the cluster:
```
{
    "hostname1": {
        "GPU-acb66c44": "0000:07:00.0",
        ...
    },
    "hostname2": {
        "GPU-31f57da0": "0000:0A:00.0",
        ...
    }
}
```

For getting a debug dump of the class ads at the end of the `endTransaction`, build with `-DVERBOSE` (automatically set for `Debug` or `RelWithDebInfo` builds) and set `SCHEDD_DEBUG=D_FULLDEBUG` in the condor config.