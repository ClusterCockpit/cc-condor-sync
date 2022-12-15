#pragma once

#include <condition_variable>
#include <ctime>
#include <curlpp/cURLpp.hpp>
#include <deque>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "ClassAdLogPlugin.h"
#include "condor_job.hpp"

#include <curlpp/Options.hpp>
#include <curlpp/cURLpp.hpp>

class CCSyncPlugin : public ClassAdLogPlugin {

public:
  void earlyInitialize();
  void initialize();
  void shutdown();
  void newClassAd(const char *key);
  void destroyClassAd(const char *key);
  void setAttribute(const char *key, const char *name, const char *value);
  void deleteAttribute(const char *key, const char *name);
  void beginTransaction();
  void endTransaction();

  CCSyncPlugin() {}
  ~CCSyncPlugin() {}

private:
  std::uint64_t globalJobIdToInt(const JobId jobId);
  std::vector<std::string> getAccelerators(const CondorJob &job,
                                           const std::string &hostname);
  std::string getGpuDeviceId(const std::string &hostname,
                             const std::string &slug);
  void sendPostRequest(const std::string &route,
                       const std::string &body) const noexcept;

  std::mutex data_mutex;
  bool initializing = true;
  CondorJobCollection currentStatus;
  std::deque<JobId> toConsider;

  curlpp::Cleanup cleanup;

  std::string url;
  std::string apiKey;
  std::string clusterName;
  int submitNodeId;

  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      gpuMap;
};
