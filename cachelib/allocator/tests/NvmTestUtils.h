/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/dynamic.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"

namespace facebook {
namespace cachelib {
namespace tests {
namespace utils {
using NavyConfig = navy::NavyConfig;
inline NavyConfig getNvmTestConfig(const std::string& cacheDir) {
  NavyConfig config{};
  config.setSimpleFile(cacheDir + "/navy", 100 * 1024ULL * 1024ULL);
  config.setDeviceMetadataSize(4 * 1024 * 1024);
  config.setBlockSize(1024);
  config.setNavyReqOrderingShards(10);
  config.blockCache().setRegionSize(4 * 1024 * 1024);
  config.bigHash()
      .setSizePctAndMaxItemSize(50, 100)
      .setBucketSize(1024)
      .setBucketBfSize(8);
  return config;
}

} // namespace utils
} // namespace tests
} // namespace cachelib
} // namespace facebook
