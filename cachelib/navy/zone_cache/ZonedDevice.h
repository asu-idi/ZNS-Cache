#pragma once
#include <fcntl.h>
#include <fmt/core.h>
#include <folly/Executor.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>
#include <folly/synchronization/Baton.h>
#include <libexplain/fsync.h>
#include <libexplain/pwrite.h>
#include <libzbd/zbd.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <tuple>
#include <vector>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/zone_cache/Zone.h"
namespace facebook {
namespace cachelib {
namespace navy {

using DeviceInfo = zbd_info;

class ZonedDevice final : public Device {
 public:
  ZonedDevice(std::string devpath,
              uint64_t size,
              uint32_t zoneNum,
              uint32_t ioAlignSize,
              std::shared_ptr<DeviceEncryptor> encryptor,
              uint32_t maxDeviceWriteSize,
              int flags = O_RDONLY)
      : devpath_(devpath),
        flags_(flags),
        zoneNum_(zoneNum),
        Device{size, std::move(encryptor), ioAlignSize, maxDeviceWriteSize} {
    this->GCBuffer_ = this->makeIOBuffer(16777216);
    for (int i = 0; i < numBuffers_; i++) {
      this->buffers_[i] = this->makeIOBuffer(16777216);
    }

    fd_ =
        zbd_open(devpath.c_str(), flags | O_DIRECT | O_LARGEFILE, &deviceInfo_);
    if (fd_ == -1) {
      throw std::runtime_error(fmt::format("open {} error", devpath));
    }
    const auto& deviceInfo = deviceInfo_;
    XLOGF(INFO,
          "open {} success. It has {} zones, each has 0x{:X} size, device "
          "aligns to 0x{:X}.",
          devpath, deviceInfo.nr_zones, deviceInfo.zone_size,
          deviceInfo.pblock_size);
    initZoneInfo();
  };

  bool writeImpl(uint64_t offset, uint32_t size, const void* value) override {
    auto bytesWritten = znsWrite(value, size, offset);
    return bytesWritten == size;
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    auto bytesRead = znsRead(value, size, offset);
    // XCHECK_EQ(bytesRead, size);
    return bytesRead == size;
  }

  void flushImpl() override { flush(); }

  ZonedDevice(const ZonedDevice&) = delete;
  ZonedDevice& operator=(const ZonedDevice&) = delete;

  void flush();

  bool trimBlock(uint64_t offset, uint64_t size);

  std::shared_ptr<DataBlockInfo> realPhyAddress(uint64_t offset, uint64_t size);

  int fd() const { return fd_; }
  ~ZonedDevice() {
    stopGC.post();
    sleep(2);
    XLOG(INFO, "free up zns");
    exit(0);
  }

 private:
  /* Zone Related */
  void initZoneInfo();

  ssize_t znsRead(void* buf, uint64_t count, uint64_t offset);

  ssize_t znsWrite(const void* buf, uint64_t count, uint64_t offset);

  std::shared_ptr<DataBlockInfo> writeRegionToZone(const void* buf,
                                 uint64_t count,
                                 uint64_t offset,
                                 std::shared_ptr<Zone> zone);

  std::shared_ptr<Zone> openZoneForWrite();

  std::tuple<CallResult, std::shared_ptr<Zone>> allocateWritingZone();

  // bool isZoneFull(int wi);
  bool isZoneFull(std::shared_ptr<Zone> zone);

  // void finishZone(int wi);
  void finishWritingZone(std::shared_ptr<Zone> zone);

  void closeWritingZone(std::shared_ptr<Zone> zone);

  uint64_t getZoneId(uint64_t offset) {
    // note: computing id should use zone_size rather capacity
    return offset / deviceInfo_.zone_size;
  }

  std::shared_ptr<Zone>& getZone(uint64_t offset) {
    // note: computing id should use zone_size rather capacity
    return allZones_.at(getZoneId(offset));
  }

  /* Movement Algorithm Related */
  void startMovement();

  bool needReclaim();

  std::shared_ptr<Zone> findVictim(bool must = false);

  bool moveValidData(std::shared_ptr<Zone> &from, const std::vector<DataBlockInfo> &allValidBlock, 
                     std::vector<std::shared_ptr<Zone>> to, std::vector<std::shared_ptr<DataBlockInfo>> &allMovedInfo);

  std::shared_ptr<DataBlockInfo> moveBlock(const DataBlockInfo &info, std::shared_ptr<Zone> from, std::shared_ptr<Zone> to);

  /* Mapping Related */
  void insertMapping(std::shared_ptr<DataBlockInfo> info);

  std::shared_ptr<DataBlockInfo> deleteMapping(std::shared_ptr<DataBlockInfo> info) ;

  std::shared_ptr<DataBlockInfo> deleteMappingInGC(const DataBlockInfo& info);

  // use balaned tree to maintain the logical address
  // mapping store the **end** offset
  std::map<uint64_t, std::shared_ptr<DataBlockInfo>> mapping_;

  // hash map or tree map is same here
  // reverse mapping store the **start** offset
  // std::unordered_map<uint64_t, std::shared_ptr<DataBlockInfo>>
  // reverseMapping_;

  std::mutex mappingLock_;
  std::mutex zoneStateLock_;
  std::atomic<int32_t> openZones_{0};

  int fd_;
  const int flags_;
  const uint32_t zoneNum_;
  const std::string devpath_;
  DeviceInfo deviceInfo_;

  int maxWrtingZoneNumber{3};
  std::vector<std::shared_ptr<Zone>> writingZones_;
  std::vector<std::shared_ptr<Zone>> readingZones_;
  std::vector<std::shared_ptr<Zone>> allZones_;

  folly::Baton<> stopGC;
  // just for movement? 127 * 0x1000 bytes
  // alignas(4096) char GCBuffer_[16777216];
  Buffer GCBuffer_;

  static constexpr int numBuffers_ = 8;
  std::array<Buffer, numBuffers_> buffers_;
  std::array<bool, numBuffers_> bufferInUse_;
  std::mutex bufferLock_;
  std::condition_variable bufferCV_;

  std::shared_ptr<Zone> tempZone_ = nullptr;
  std::shared_ptr<Zone> GCZone_ = nullptr;
};

class DirectZonedDevice final : public Device {
 public:
  DirectZonedDevice(std::string devpath,
                    uint64_t size,
                    uint32_t ioAlignSize,
                    std::shared_ptr<DeviceEncryptor> encryptor,
                    uint32_t maxDeviceWriteSize,
                    int flags = O_RDONLY)
      : devpath_(devpath),
        flags_(flags),
        Device{size, std::move(encryptor), ioAlignSize, maxDeviceWriteSize} {
    fd_ =
        zbd_open(devpath.c_str(), O_RDWR | O_DIRECT | O_LARGEFILE, &deviceInfo_);
    if (fd_ == -1) {
      throw std::runtime_error(fmt::format("open {} error", devpath));
    }
    zbd_reset_zones(fd_, 0, 0);

    zbd_get_info(fd_, &deviceInfo_);
    zones_.resize(deviceInfo_.nr_zones);
    const auto& deviceInfo = deviceInfo_;
    XLOGF(INFO,
          "open {} success. It has {} zones, each has 0x{:X} size, device "
          "aligns to 0x{:X}.",
          devpath, deviceInfo.nr_zones, deviceInfo.zone_size,
          deviceInfo.pblock_size);
    updateZoneInfo();
  };

  DirectZonedDevice(const DirectZonedDevice&) = delete;
  DirectZonedDevice& operator=(const DirectZonedDevice&) = delete;
  ~DirectZonedDevice() override {}

  bool writeImpl(uint64_t offset, uint32_t size, const void* value) override {
    XDCHECK_EQ(offset % Device::getIOAlignmentSize(), 0);
    auto zid = (offset / deviceInfo_.zone_size);
    auto ptr = getWritePointer(zid);
    
    XLOGF(DBG, "write at zone {} offset 0x{:X}, size 0x{:X}", zid, offset, size);
    XDCHECK_EQ(ptr, offset);
    ssize_t bytesWritten = pwrite(fd_, value, size, offset);
    if (bytesWritten != size) {
      XLOGF(ERR, "write at zone {} offset 0x{:X}, size 0x{:X}", zid, offset, size);
      XLOG(ERR, explain_pwrite(fd_, value, size, offset));
      reportIOError("write", offset, size, bytesWritten);
      return false;
    }
    if (bytesWritten == size) {
      incWritePointer(zid, bytesWritten);
      return true;
    }
    return false;
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    // XLOGF(DBG, "read at offset 0x{:X}, size 0x{:X}", offset, size);
    ssize_t bytesRead = ::pread(fd_, value, size, offset);
    if (bytesRead != size) {
      reportIOError("read", offset, size, bytesRead);
    }
    return bytesRead == size;
  }

  void flushImpl() override { ::fsync(fd_); }

  bool isZoneDeviceImpl() { return true; }

  bool open(uint64_t offset) {
    opened ++;
    XDCHECK_EQ(offset % deviceInfo_.zone_size, 0);
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_OPEN,
                               offset / deviceInfo_.zone_size);
  }

  bool reset(uint64_t offset) {
    XDCHECK_EQ(offset % deviceInfo_.zone_size, 0);
    auto zoneId = offset / deviceInfo_.zone_size;
    auto ok = doZoneCtrlOperation(zbd_zone_op::ZBD_OP_RESET, zoneId);
    XDCHECK(ok);

    if (ok) {
      updateZoneInfo(zoneId);
      return true;
    }
    return false;
  }

  bool close(uint64_t offset) {
    XDCHECK_EQ(offset % deviceInfo_.zone_size, 0);
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_CLOSE,
                               offset / deviceInfo_.zone_size);
  }

  bool finish(uint64_t offset) {
    XDCHECK_EQ(offset % deviceInfo_.zone_size, 0);
    opened --;
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_FINISH,
                               offset / deviceInfo_.zone_size);
  }

 private:
  void reportIOError(const char* opName,
                     uint64_t offset,
                     uint32_t size,
                     ssize_t ioRet) {
    XLOG_EVERY_N_THREAD(
        ERR, 1000,
        folly::sformat("opened {}, IO error: {} offset={} size={} ret={} errno={} ({})",
                       opened, opName, offset, size, ioRet, errno,
                       std::strerror(errno)));
  }

  bool updateZoneInfo(int zoneId) {
    const auto& zone = zones_[zoneId];
    unsigned int nr_zone_1 = 1;
    zbd_report_zones(fd_, zone.start, zone.len, zbd_report_option::ZBD_RO_ALL,
                     zones_.data() + zoneId, &nr_zone_1);
    zones_[zoneId].wp = zones_[zoneId].start;
    XLOGF(DBG, "updating zoneId=0x{:X} info from zns", zoneId);
    return nr_zone_1 == 1;
  }

  bool updateZoneInfo() {
    unsigned int readSize = zones_.size();
    zbd_report_zones(fd_, 0, 0, zbd_report_option::ZBD_RO_ALL, zones_.data(),
                     &readSize);
    XDCHECK_EQ(readSize, deviceInfo_.nr_zones, zones_.size());
    XLOGF(DBG, "update 0x{:X} bytes zbd_zones info from zns", readSize);
    return readSize == zones_.size();
  }

  bool doZoneCtrlOperation(zbd_zone_op op, int zoneId) {
    auto zone = zones_[zoneId];
    auto res = zbd_zones_operation(fd_, op, zone.start, zone.len);
    if (!res) {
      // update_zone_info(dev_fd, zone);
      XLOGF(DBG, "operation 0x{:X} on 0x{:X} success", op,
            (unsigned long long)zone.start);
      return true;
    } else {
      XLOGF(ERR, "operation 0x{:X} on 0x{:X} error", op,
            (unsigned long long)zone.start);
      return false;
    }
  }

  uint64_t getWritePointer(int zoneId) {
    // updateZoneInfo(zoneId);
    return zones_[zoneId].wp;
  }

  uint64_t incWritePointer(int zoneId, int inc) {
    // updateZoneInfo(zoneId);
    zones_[zoneId].wp += inc;
    return zones_[zoneId].wp;
  }

  int fd_;
  const int flags_;
  const std::string devpath_;
  std::vector<zbd_zone> zones_;
  zbd_info deviceInfo_;
  int opened = 0;
};


} // namespace navy
} // namespace cachelib
} // namespace facebook