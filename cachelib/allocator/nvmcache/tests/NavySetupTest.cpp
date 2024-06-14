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

#include <folly/File.h>
#include <gtest/gtest.h>

#include <stdexcept>
#include <system_error>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/allocator/nvmcache/NavySetup.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

namespace {
// chmod on a file.
// Throw if the file does not exist.
void changeMode(const std::string& name, mode_t mode) {
  if (!util::pathExists(name)) {
    throw std::invalid_argument(folly::sformat(
        "Trying to chmod on file {} that does not exist!", name));
  }
  char tmp[256];
  snprintf(tmp, sizeof(tmp), "%s", name.c_str());

  chmod(tmp, mode);
}
} // namespace
TEST(NavySetupTest, RAID0DeviceSize) {
  // Verify size is reduced when we pass in a size that's not aligned to
  // stripeSize for RAID0Device

  auto filePath =
      folly::sformat("/tmp/navy_device_raid0io_test-{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  std::vector<std::string> files = {filePath + "/CACHE0", filePath + "/CACHE1",
                                    filePath + "/CACHE2", filePath + "/CACHE3"};

  int size = 9 * 1024 * 1024;
  int ioAlignSize = 4096;
  int stripeSize = 8 * 1024 * 1024;

  std::vector<std::string> navyFileArray;
  for (const auto& file : files) {
    navyFileArray.push_back(file);
  }

  navy::NavyConfig cfg{};
  cfg.setRaidFiles(navyFileArray, size, true);
  cfg.blockCache().setRegionSize(stripeSize);
  cfg.setBlockSize(ioAlignSize);

  auto device = createDevice(cfg, nullptr);
  EXPECT_GT(size * files.size(), device->getSize());
  EXPECT_EQ(files.size() * 8 * 1024 * 1024, device->getSize());
}

// Make sure that we throw when the device failed to create.
TEST(NavySetupTest, FileCreationFailure) {
  auto filePath =
      folly::sformat("/tmp/navy_device_raid0io_test-{}", ::getpid());
  util::makeDir(filePath);
  // Change the directory permission so that cachelib can't create file.
  changeMode(filePath, 0111);
  SCOPE_EXIT {
    changeMode(filePath, 0777);
    util::removePath(filePath);
  };
  std::vector<std::string> files = {filePath + "/CACHE0", filePath + "/CACHE1",
                                    filePath + "/CACHE2", filePath + "/CACHE3"};

  int size = 9 * 1024 * 1024;
  int ioAlignSize = 4096;
  int stripeSize = 8 * 1024 * 1024;

  std::vector<std::string> navyFileArray;
  for (const auto& file : files) {
    navyFileArray.push_back(file);
  }

  navy::NavyConfig cfg{};
  cfg.setRaidFiles(navyFileArray, size, true);
  cfg.blockCache().setRegionSize(stripeSize);
  cfg.setBlockSize(ioAlignSize);

  // Expect to throw.
  EXPECT_THROW({ createDevice(cfg, nullptr); }, std::system_error);

  // Change the permission back and expect to initialize normally.
  changeMode(filePath, 0777);
  auto device = createDevice(cfg, nullptr);
  EXPECT_GT(size * files.size(), device->getSize());
  EXPECT_EQ(files.size() * 8 * 1024 * 1024, device->getSize());
}
} // namespace cachelib
} // namespace facebook
