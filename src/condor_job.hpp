#pragma once

#include <unordered_map>
#include <functional>
#include <algorithm>

using namespace std;

// std::string cannot be empty, null, or whatever. Luckily, there are no empty strings
#define NULL_STR ""

typedef unordered_map<string, string> CondorJob;
typedef pair<int,int> JobId;

typedef unordered_map<int,CondorJob> CondorJobCluster;
typedef unordered_map<int,CondorJobCluster> CondorJobCollection;
