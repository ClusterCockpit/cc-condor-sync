#include "ClassAdLogPlugin.h"
#include "blahp/src/blahpd.h"
#include "condor_common.h"
#include "condor_config.h"
#include "condor_debug.h"
#include "condor_job.hpp"
#include "condor_sys_linux.h"
#include "proc.h"

// this position is important for dprintf.. :/
#include "htcondor_cc_sync_plugin.hpp"

#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <exception>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#include <curl/curl.h>
#include <curlpp/Easy.hpp>
#include <curlpp/Option.hpp>
#include <curlpp/Options.hpp>
#include <curlpp/cURLpp.hpp>
#include <unordered_map>

#include "json.hpp"

// Global from the condor_schedd, its name
extern char *Name;

using namespace std;
using namespace nlohmann;

static const std::unordered_map<int, std::string> jobStateMap = {
    {RUNNING, "running"},
    {REMOVED, "cancelled"},
    {HELD, "failed"},
    {COMPLETED, "completed"},
    {SUSPENDED, "stopped"}};

void CCSyncPlugin::earlyInitialize() {
  lock_guard<std::mutex> guard(data_mutex);

  param(url, "CCSYNC_URL");
  if (url.empty())
    dprintf(D_ALWAYS, "WARNING: missing CCSYNC_URL in local config!\n");

  param(apiKey, "CCSYNC_APIKEY");
  if (apiKey.empty())
    dprintf(D_ALWAYS, "WARNING: missing CCSYNC_APIKEY in local config!\n");

  param(clusterName, "CCSYNC_CLUSTER_NAME");
  if (clusterName.empty())
    dprintf(D_ALWAYS,
            "WARNING: missing CCSYNC_CLUSTER_NAME in local config!\n");

  std::string submitNodeIdS;
  param(submitNodeIdS, "CCSYNC_SUBMIT_ID");
  if (submitNodeIdS.empty())
    dprintf(D_ALWAYS, "WARNING: missing CCSYNC_SUBMIT_ID in local config!\n");
  else
    submitNodeId = std::stoi(submitNodeIdS);

  std::string gpuMapFile;
  param(gpuMapFile, "CCSYNC_GPU_MAP");
  if (gpuMapFile.empty())
    dprintf(D_ALWAYS, "WARNING: missing CCSYNC_GPU_MAP in local config!\n");
  else if (std::ifstream file{gpuMapFile}) {
    gpuMap.clear();
    auto jsonMap = json::parse(file);
    for (auto nodeIt = jsonMap.cbegin(); nodeIt != jsonMap.cend(); nodeIt++) {
      auto &node = gpuMap[nodeIt.key()];
      for (auto gpuIt = nodeIt.value().cbegin(); gpuIt != nodeIt.value().cend();
           gpuIt++) {
        node[gpuIt.key()] = gpuIt.value();
      }
    }
  }
}

void CCSyncPlugin::initialize() {
  lock_guard<std::mutex> guard(data_mutex);

  //   dprintf(D_ALWAYS, "Initializing TCP Forwarding plugin.\n");
  initializing = false;
}

void CCSyncPlugin::shutdown() {}

void CCSyncPlugin::newClassAd(const char *key) {
  lock_guard<std::mutex> guard(data_mutex);

  // CRITICAL SECTION
  int clusterId, procId;
  // dprintf(D_FULLDEBUG,"New classAd registered: %s\n",key);
  if (sscanf(key, "%d.%d", &clusterId, &procId) == 2) {
    if (clusterId ==
        0) { // Key "0.0" means generic parameters we are not interested in.
      return;
    }
    if (currentStatus.count(clusterId) &&
        currentStatus[clusterId].count(
            procId)) { // The key already exists, it's some replay / duplicate,
                       // just ignore
      return;
    }
    if (procId == -1) { // Key "xxx.-1" is a "virtual" cluster entry.
      currentStatus[clusterId][-1] = {};
    } else {
      // We now have to copy the "template", the "-1" entry, if it exists. If
      // there is no "-1" entry, it should be created with an empty attribute
      // map. Don't you love C++11? (hope it works as expected, though)
      currentStatus[clusterId][procId] = currentStatus[clusterId][-1];
    }
  }
}

void CCSyncPlugin::destroyClassAd(const char *key) {
  lock_guard<std::mutex> guard(data_mutex);

  // CRITICAL SECTION
  int clusterId, procId;
  // dprintf(D_FULLDEBUG,"Destroying classAd: %s\n", key);
  if (sscanf(key, "%d.%d", &clusterId, &procId) == 2) {
    if (clusterId ==
        0) { // Key "0.0" means "generic parameters we are not interested in".
      return;
    }
    if (!currentStatus.count(clusterId) ||
        !currentStatus[clusterId].count(
            procId)) { // The key doesn't exist, just ignore
      return;
    }
    //		currentStatus.erase({clusterId,-1});
    currentStatus[clusterId].erase(procId);
    if (procId != -1) {    // Key "xxx.-1" is a "virtual" cluster entry.
      if (!initializing) { // HTCondor bug? We should'n be receiving events
                           // before initialization is done. We will ignore them
                           // if it happens.
      }
    }
    if (currentStatus[clusterId].empty()) {
      currentStatus.erase(clusterId);
    }
  }
}

void CCSyncPlugin::setAttribute(const char *key, const char *_name,
                                const char *_value) {
  lock_guard<std::mutex> guard(data_mutex);

  // CRITICAL SECTION
  int clusterId, procId;
  // dprintf(D_FULLDEBUG,"Setting attribute for key %s: [%s]: %s\n", key, _name,
  // _value);
  //  We need to copy these values.

  string name(_name);
  string value(_value);

  if (sscanf(key, "%d.%d", &clusterId, &procId) == 2) {
    if (clusterId ==
        0) { // Key "0.0" means "generic parameters we are not interested in".
      return;
    }
    if (!currentStatus.count(clusterId) ||
        !currentStatus[clusterId].count(
            procId)) { // The key should exist by now, if not just log and
                       // ignore
      // dprintf(D_FULLDEBUG,"Attribute received for non-existing ClassAd:
      // {%d.%d}", clusterId, procId);
    }
    if (procId == -1) { // Template value, we must apply it to every current job
                        // (including the "-1" virtual job, so it will be
                        // applied to new jobs)
      // dprintf(D_FULLDEBUG,"We received a '-1' attribute. Assigning it to
      // procs: ");
      for (auto &jobEntry : currentStatus[clusterId]) {
        int actualProcId = jobEntry.first;
        currentStatus[clusterId][actualProcId][name] = value;
        // dprintf(D_FULLDEBUG,"[%d.%d] ", clusterId, actualProcId);
        if (actualProcId != -1) {
          // dprintf(D_FULLDEBUG,"{%d.%d} ", clusterId, actualProcId);
          if (!initializing) { // HTCondor bug? We should'n be receiving events
            // before initialization is done. We will ignore
            // them if it happens.
            if (name == "CpusProvisioned" || name == "JobStatus")
              toConsider.push_back({clusterId, procId});
          }
        }
      }
      // dprintf(D_FULLDEBUG,".\n");
    } else {
      // dprintf(D_FULLDEBUG,"Assigning attribute to JobId %d.%d\n", clusterId,
      // procId);
      currentStatus[clusterId][procId][name] = value;
      if (!initializing) { // HTCondor bug? We should'n be receiving events
                           // before initialization is done. We will ignore them
                           // if it happens.
        if (name == "CpusProvisioned" || name == "JobStatus")
          toConsider.push_back({clusterId, procId});
      }
    }
  }
}

void CCSyncPlugin::deleteAttribute(const char *key, const char *name) {
  lock_guard<std::mutex> guard(data_mutex);

  // CRITICAL SECTION
  int clusterId, procId;
  // dprintf(D_FULLDEBUG,"Deleting attribute for key %s: [%s] \n", key, name);
  if (sscanf(key, "%d.%d", &clusterId, &procId) == 2) {
    if (!currentStatus.count(clusterId) ||
        !currentStatus[clusterId].count(procId) ||
        !currentStatus[clusterId][procId].count(
            key)) { // The entry doesn't exist, just ignore
      return;
    }
    if (procId == -1) { // Template value, we must apply it to every current job
                        // (including the "-1" virtual job, so it will be
                        // applied to new jobs)
      for (auto &jobEntry : currentStatus[clusterId]) {
        int actualProcId = jobEntry.first;
        currentStatus[clusterId][actualProcId].erase(name);
      }
    } else {
      currentStatus[clusterId][procId].erase(name);
    }
  }
}

void CCSyncPlugin::beginTransaction() {
  if (initializing) { // HTCondor bug? We should'n be receiving events before
                      // initialization is done. We will ignore them if it
                      // happens.
    return;
  }
}

std::uint64_t CCSyncPlugin::globalJobIdToInt(const JobId jobId) {
  return std::uint64_t(jobId.first) << 32 | ((jobId.second & 0x3FFFFFFF) << 2) |
         submitNodeId;
}

void printJobStatus(const JobId jobId, const CondorJob &job) {
  dprintf(D_ALWAYS, "=============== Job ClassAds: %d.%d\n", jobId.first,
          jobId.second);
  for (auto value : job) {
    dprintf(D_ALWAYS, "%s: %s\n", value.first.c_str(), value.second.c_str());
  }
  dprintf(D_ALWAYS, "=============== End Job ClassAds: %d.%d\n", jobId.first,
          jobId.second);
}

std::string getRemoteHost(std::string_view slot) {
  return "uller";
  auto atIt = std::find(slot.begin(), slot.end(), '@');
  auto subIt = std::find(atIt, slot.end(), '.');
  return {++atIt, subIt};
}

std::vector<std::string>
CCSyncPlugin::getAccelerators(const CondorJob &job,
                              const std::string &hostname) {
  if (auto hostIt = gpuMap.find(hostname); hostIt != gpuMap.end()) {
    if (auto gpuIt = job.find("AssignedGPUs"); gpuIt != job.end()) {
      std::istringstream gpuStream{gpuIt->second};
      std::vector<std::string> gpus;
      std::string gpu;
      while (std::getline(gpuStream, gpu, ',')) {
        if (auto gpuIt = hostIt->second.find(gpu);
            gpuIt != hostIt->second.end()) {
          gpus.push_back(gpuIt->second);
        } else {
          dprintf(D_ALWAYS, "Didn't find GPU '%s' for '%s'\n", gpu.c_str(),
                  hostname.c_str());
        }
      }
      return gpus;
    }
  } else {
    dprintf(D_ALWAYS, "Didn't find hostname in GPU list '%s'\n",
            hostname.c_str());
  }
  return {};
}

bool jobHasClassAd(const CondorJob &job, const std::string &field) {
  if (job.find(field) == job.end()) {
    dprintf(D_ALWAYS, "Missing ClassAD: %s\n", field.c_str());
    return false;
  }
  return true;
}

bool jobHasRequiredClassAds(const CondorJob &job) {
  return jobHasClassAd(job, "JobStatus") &&
         jobHasClassAd(job, "LastJobStatus") &&
         jobHasClassAd(job, "RemoteHost") &&
         jobHasClassAd(job, "GlobalJobId") &&
         jobHasClassAd(job, "CurrentHosts") &&
         jobHasClassAd(job, "CpusProvisioned") &&
         jobHasClassAd(job, "JobCurrentStartDate") &&
         jobHasClassAd(job, "AcctGroup") && jobHasClassAd(job, "Owner") &&
         jobHasClassAd(job, "EnteredCurrentStatus");
}

std::string removeQuotes(const std::string &value) {
  if (value[0] == '"' && value.back() == '"') {
    return value.substr(1, value.size() - 2);
  }
  return value;
}

void CCSyncPlugin::sendPostRequest(const std::string &route,
                                   const std::string &body) const noexcept {
  dprintf(D_ALWAYS, "POST body: %s\n", body.c_str());
  try {
    curlpp::Easy request;
    request.setOpt(curlpp::options::Url(url + route));

    std::list<std::string> header;
    header.push_back("Content-Type: application/json");
    header.push_back("Accept: application/ld+json");
    header.push_back("Authorization: Bearer " + apiKey);
    request.setOpt(curlpp::options::HttpHeader(header));

    request.setOpt(curlpp::options::Post(1));
    request.setOpt(curlpp::options::PostFields(body));
    request.setOpt(curlpp::options::PostFieldSize(body.size()));

    std::stringstream buf;
    request.setOpt(curlpp::options::WriteStream(&buf));

    request.setOpt(curlpp::options::DebugFunction(
        [](curl_infotype type, char *ptr, unsigned long length) -> int {
          // todo: eh.. if we need a length, are we potentially reading
          // oob??
          dprintf(D_ALWAYS, "%d: %s\n", type, ptr);
          return length;
        }));
    // request.setOpt(curlpp::options::Verbose(true));

    request.perform();

    dprintf(D_ALWAYS, "response: %s\n", buf.str().c_str());
  } catch (const curlpp::LogicError &e) {
    dprintf(D_ALWAYS, "LogicError: %s\n", e.what());
  } catch (const curlpp::RuntimeError &e) {
    dprintf(D_ALWAYS, "RuntimeError: %s\n", e.what());
  } catch (const std::exception &e) {
    dprintf(D_ALWAYS, "exception: %s\n", e.what());
  } catch (...) {
    dprintf(D_ALWAYS, "unknown exception\n");
  }
}

// std::unordered_map<std::string, std::string> parseToE(const std::string &toe)
// {
//   // toe = "[ Who = "itself"; How = "OF_ITS_OWN_ACCORD"; ExitCode = 2;
//   HowCode =
//   // 0; When = 1671108732; ExitBySignal = false ]";
//   auto start = toe.find_first_not_of("[ ");
//   auto stop = toe.find_last_not_of("] ");
//   std::istringstream toeStream{toe.substr(start + 1, stop - start)};

//   std::unordered_map<std::string, std::string> map;

//   std::string element;
//   while (std::getline(toeStream, element, ';')) {
//     auto splitter = element.find(" = ");
//     auto key = element.substr(0, splitter);
//     auto value = element.substr(splitter + sizeof(" = "));
//     map.emplace(key, value);
//     dprintf(D_ALWAYS, "ToE field '%s': '%s'\n", key.c_str(), value.c_str());
//   }

//   return map;
// }

// std::uint64_t getEndTime(const CondorJob &job) {
//   if (auto toeIt = job.find("ToE"); toeIt != job.end()) {
//     auto toe = parseToE(toeIt->second);
//     if (auto whenIt = toe.find("When"); whenIt != toe.end())
//       return std::stoull(whenIt->second); // todo: always an int here?
//   } else if (auto enteredStateTimeIt = job.find("EnteredCurrentStatus");
//              enteredStateTimeIt != job.end()) {
//     return std::stoull(enteredStateTimeIt->second);
//   }
//   return 0;
// }

void CCSyncPlugin::endTransaction() {
  if (initializing) { // HTCondor bug? We should'n be receiving events before
                      // initialization is done. We will ignore them if it
                      // happens.
    return;
  }
  lock_guard<std::mutex> guard(data_mutex);

  // CRITICAL SECTION
  // dprintf(D_FULLDEBUG,"Ending transaction.\n");

  for (auto jobId : toConsider) {
    if (jobId.second == -1)
      continue; // don't care about the "template"

    auto &job = currentStatus[jobId.first][jobId.second];
    printJobStatus(jobId, job);
    if (job.empty() || !jobHasRequiredClassAds(job))
      continue; // not yet ready
    
    dprintf(D_ALWAYS, "JobStatus: %s\n", job["JobStatus"].c_str());
    int state = std::stoi(job["JobStatus"]), lastState;

    if (auto lastStateIt = job.find("LastJobStatus");
        lastStateIt != job.end()) {
      dprintf(D_ALWAYS, "LastJobStatus: %s\n", lastStateIt->second.c_str());
      if (lastState = std::stoi(lastStateIt->second); lastState == state)
        continue;
    } else
      continue;

    if (state == RUNNING) {
      auto jobBatchNameIt = job.find("JobBatchName");
      auto globalJobId = globalJobIdToInt(jobId);

      auto hostname = getRemoteHost(job["RemoteHost"]);
      json resources = {{"hostname", hostname}};
      auto accs = getAccelerators(job, hostname);
      if (!accs.empty()) {
        resources["accelerators"] = accs;
      }

      json j = {
          {"jobId", globalJobId},
          {"arrayJobId", jobId.first * 10 + submitNodeId},
          {"user", removeQuotes(job["Owner"])},
          {"cluster", clusterName},
          {"numNodes", std::stoi(job["CurrentHosts"])},
          {"numHwthreads", std::stoi(job["CpusProvisioned"])},
          {"startTime", std::stoull(job["EnteredCurrentStatus"])},
          {"project", removeQuotes(job["AcctGroup"])},
          {"partition", "main"},
          {"exclusive", 0},
          {"resources", json::array({resources})},
          {"numAcc", accs.size()},
          {"metadata",
           {{"jobName",
             jobBatchNameIt != job.end() ? jobBatchNameIt->second : ""}}}};

      sendPostRequest("/api/jobs/start_job/", j.dump());

    } else if (lastState == RUNNING &&
               (state == REMOVED || state == COMPLETED || state == HELD ||
                state == SUSPENDED)) {

      std::uint64_t startTime = std::stoull(job["JobCurrentStartDate"]),
                    stopTime = std::stoull(job["EnteredCurrentStatus"]);

      json j = {{"jobId", globalJobIdToInt(jobId)},
                {"cluster", clusterName},
                {"jobState", jobStateMap.at(state)},
                {"startTime", startTime},
                {"stopTime",
                 (stopTime - startTime > 10) ? stopTime : startTime + 10}};

      sendPostRequest("/api/jobs/stop_job/", j.dump());
    }
  }

  toConsider.clear();
}

static CCSyncPlugin instance;
