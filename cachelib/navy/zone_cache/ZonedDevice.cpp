#include "cachelib/navy/zone_cache/ZonedDevice.h"
#include <folly/logging/xlog.h>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include "cachelib/navy/zone_cache/Zone.h"
namespace facebook {
namespace cachelib {
namespace navy {

/* Zone Related */
void ZonedDevice::initZoneInfo() {
  // int flags = O_RDWR;
  zbd_get_info(fd_, &deviceInfo_);

  maxWrtingZoneNumber = 3;
  // set up the zone number
  if (zoneNum_) {
    deviceInfo_.nr_zones = zoneNum_;
  }
  auto nr_zones = deviceInfo_.nr_zones;
  XLOGF(INFO, "using test nr_zones={} and maxWrtingZoneNumber={}", nr_zones, maxWrtingZoneNumber);

  std::vector<zbd_zone> zones;
  zones.resize(deviceInfo_.nr_zones);

  unsigned int readSize = zones.size();
  uint64_t zeroZone = 0;
  
  zbd_reset_zones(fd_, 0, 0);
  zbd_report_zones(fd_, zeroZone * deviceInfo_.zone_size, 0, zbd_report_option::ZBD_RO_ALL, zones.data(), &readSize);
  const auto zone0 = zones.at(zeroZone);
  tempZone_ = std::make_shared<Zone>(fd_, zeroZone, zone0.start, zone0.len, zone0.capacity);
  allZones_.push_back(tempZone_);

  uint64_t firstZone = 1;
  uint64_t lastZone = zones.size() - 1;
  for (uint64_t i = firstZone; i < lastZone; i++) {
    const auto &zone = zones.at(i);
    XCHECK_EQ(i, zone.start / zone.len);
    auto p = std::make_shared<Zone>(fd_, i, zone.start, zone.len, zone.capacity);
    // p->reset();
    writingZones_.push_back(p);
    allZones_.push_back(p);
  }

  const auto &gcZone = zones.at(lastZone);
  GCZone_ = std::make_shared<Zone>(fd_, lastZone, gcZone.start, gcZone.len, gcZone.capacity);
  // GCZone_->reset();
  allZones_.push_back(GCZone_);

  auto backGC = true;
  if (backGC) {
    folly::getGlobalIOExecutor()->add([=]() {
      startMovement();
    });
  }
}

std::shared_ptr<DataBlockInfo> ZonedDevice::realPhyAddress(uint64_t offset, uint64_t size) {
  std::lock_guard<std::mutex> mlck(mappingLock_);
  auto it = mapping_.find(offset + size);
  if (it == mapping_.end()) {
    return nullptr;
  }
  auto info = it->second;
  return info;
}

bool ZonedDevice::trimBlock(uint64_t offset, uint64_t size) {
  std::lock_guard<std::mutex> mlck(mappingLock_);
  auto it = mapping_.find(offset + size);
  if (it == mapping_.end()) {
    // we don't need to do anything.
    return true;
  }

  auto info = it->second;
  auto removedInfo = deleteMapping(info);
  if (removedInfo) {
    auto otherZone = getZone(removedInfo->znsOffset);
    otherZone->deleteLogicalMapping(removedInfo->znsOffset);
  }
  return true;
}

ssize_t ZonedDevice::znsRead(void *buf, uint64_t count, uint64_t offset) {
  // XLOGF(DBG, "begin reading 0x{:X} bytes using logical offset 0x{:X}.", count, offset);
  ssize_t readBytes = 0;
  int64_t leftBytes = count;

  // find zone/block to read
  // std::lock_guard<std::mutex> mlck(mappingLock_);
  mappingLock_.lock();
  // only insert the end of file map.insert(offset + size, info);
  auto it = mapping_.lower_bound(offset + 1);
  if (it == mapping_.end()) {
    mappingLock_.unlock();
    // there is no value larger than offset
    XLOGF(ERR, "no data at {}", offset);
    throw std::logic_error("offset not right");
  }
  
  if (not (it->second->logOffset <= offset and offset < it->second->logOffset + it->second->size)) {
    mappingLock_.unlock();
    XLOGF(ERR, "offset is already reclaimed, 0x{:X}, [{}, {})", offset, it->second->logOffset, it->second->size);
    throw std::logic_error("offset not right");
  }
  XCHECK_GE(offset, it->second->logOffset);
  XCHECK_LT(offset, it->second->logOffset + it->second->size);

  auto shift = offset - it->second->logOffset;
  auto info = it->second;

  auto zone = getZone(info->znsOffset + shift);
  std::shared_lock<std::shared_mutex> lck(zone->lock_);
  mappingLock_.unlock();

  // read data
  while (leftBytes > 0) {
    auto toReadBytes = std::min((int64_t) (info->size - shift), leftBytes);
    // XLOGF(DBG, "read 0x{:X} bytes from physic offset 0x{:X}, using logical offset 0x{:X}.", toReadBytes, info->znsOffset + shift, offset);
    auto sz = zone->read((uint8_t *)buf + readBytes, toReadBytes, info->znsOffset + shift);
    if (sz == 0) {
      return 0;
    }
    if (sz == -1) {
      throw std::ios_base::failure("read from zns error!");
      return sz;
    }
    offset += sz;
    leftBytes -= sz;
    readBytes += sz;
  }
  if (readBytes != count and readBytes == 0) {
    // fake read
    memset(buf, '\0', count);
    readBytes += count;
  }
  XCHECK_EQ(readBytes, count);
  return readBytes;
}

std::tuple<CallResult, std::shared_ptr<Zone>> ZonedDevice::allocateWritingZone() {
  // auto maxOpen = maxWrtingZoneNumber - 2;
  if (writingZones_.size() > 0) {
    // return {CallResult::SUCCESS, writingZones_.size() - 1};
    // pop out the last one
    auto wi = writingZones_.size() - 1;
    auto zone = writingZones_.at(wi);
    if (zone->getWritePointer() == zone->getStart()) {
      if (openZones_ >= maxWrtingZoneNumber) {
        // wait
        return {CallResult::RETRY, nullptr};
      }
      openZones_ += 1;
    }
    writingZones_.erase(writingZones_.begin() + wi);
    return {CallResult::SUCCESS, zone};
  } else {
    return {CallResult::RETRY, nullptr};
    // throw std::logic_error("no writing zone");
  }
}

bool ZonedDevice::isZoneFull(std::shared_ptr<Zone> zone) {
  // auto &p = writingZones_.at(wi);
  auto wp = zone->getWritePointer();
  if (wp + 0x1000000 > zone->getStart() + zone->getSize()) {
    return true;
  } else {
    return false;
  }
}

void ZonedDevice::finishWritingZone(std::shared_ptr<Zone> zone) {
  // auto &p = writingZones_.at(wi);
  auto wp = zone->getWritePointer();
  XLOGF(DBG, "change full writing zones {} to reading zones. wp is 0x{:X}", zone->getZoneId(), wp);
  zone->finish();
  openZones_ -= 1;
  readingZones_.push_back(std::move(zone));
}

void ZonedDevice::closeWritingZone(std::shared_ptr<Zone> zone) {
  if (isZoneFull(zone)) {
    finishWritingZone(zone);
  } else {
    writingZones_.push_back(std::move(zone));
  }
}

ssize_t ZonedDevice::znsWrite(const void *buf, uint64_t count, uint64_t offset) {
  // XLOGF(INFO, "begin write 0x{:X} bytes using logical offset 0x{:X}.", count, offset);
  int cnt = 0;
  reopen:
  zoneStateLock_.lock();
  auto zone = openZoneForWrite();
  zoneStateLock_.unlock();

  if (zone == nullptr) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // XLOG(ERR, "waiting");
    cnt += 1;
    // if (cnt > 2) {
    //   return -1;
    // }
    goto reopen;
  }
  auto info = writeRegionToZone(buf, count, offset, zone);
  // zone->lock_.unlock();

  std::lock_guard<std::mutex> mlck(mappingLock_);
  std::lock_guard<std::mutex> vlck(zoneStateLock_);

  if (info) {
    zone->insertLogicalMapping(info->znsOffset, info);
    auto removedInfo = deleteMapping(info);
    insertMapping(info);

    // other zone can be reading zone, but can it be writing zone?
    if (removedInfo) {
      auto otherZone = getZone(removedInfo->znsOffset);
      otherZone->deleteLogicalMapping(removedInfo->znsOffset);
    }
    // close the zone
    closeWritingZone(zone);
    return count;
  } else {
    XLOGF(ERR, "write {} bytes data to zone {} failed.", count, zone->getZoneId());
    throw std::logic_error("write error");
  }
}

std::shared_ptr<Zone> ZonedDevice::openZoneForWrite() {
  auto [res, wzone] = allocateWritingZone();
  if (res == CallResult::RETRY) {
    return nullptr;
  }
  return wzone;
}

std::shared_ptr<DataBlockInfo> ZonedDevice::writeRegionToZone(const void *buf, uint64_t count, uint64_t offset, std::shared_ptr<Zone> zone) {
  uint64_t maxWriteSize = 0x1000 * 64;
  const uint8_t* data = reinterpret_cast<const uint8_t*>(buf);
  auto remainingSize = count;
  auto logicalAddress = offset;
  maxWriteSize = (maxWriteSize == 0) ? remainingSize : maxWriteSize;

  int64_t dataOffset = -1;
  if (remainingSize == 0) {
    throw std::logic_error("can't write zero size data");
  }

  while (remainingSize > 0) {
    auto writeSize = std::min<size_t>(maxWriteSize, remainingSize);
    XCHECK_EQ(offset % 0x1000, 0ul);
    XCHECK_EQ(writeSize % 0x1000, 0ul);
    auto appRes = zone->append(data, writeSize);
    if (appRes.status == AppendStatus::DONE) {
      if (dataOffset == -1) {
        dataOffset = appRes.dataOffset;
      }
      offset += writeSize;
      data += writeSize;
      remainingSize -= writeSize;
    } else {
      return nullptr;
    }
  }
  auto info = std::make_shared<DataBlockInfo>(dataOffset, count, logicalAddress);
  return info;
}

bool ZonedDevice::needReclaim() {
  // std::lock_guard<std::mutex> l(vectorLock_);
  // when writing zone is less
  auto isNeedReclaim = writingZones_.size() < maxWrtingZoneNumber;
  return isNeedReclaim;
}

std::shared_ptr<Zone> ZonedDevice::findVictim(bool must) {
  // if must is true, it will return the zone with least valid data
  std::shared_ptr<Zone> victim = nullptr;
  int victimIdx = -1;

  std::shared_ptr<Zone> minptr = nullptr;
  int mini = -1;
  int64_t minv = INT64_MAX;

  for (int i = 0; i < readingZones_.size(); i++) {
    auto &zone = readingZones_.at(i);
    if (true) {
      auto validPercent = zone->getDataSize() / (double) zone->getSize();
      if (validPercent < 0.5) {
        victim = zone;
        victimIdx = i;
        break;
      }
      if (must and minv > zone->getDataSize()) {
        minv = zone->getDataSize();
        mini = i;
        minptr = zone;
      }
    }
  }
  if (victimIdx != -1) {
    readingZones_.erase(readingZones_.begin() + victimIdx);
    return victim;
  } else if (must and mini != -1) {
    readingZones_.erase(readingZones_.begin() + mini);
    return minptr;
  }
  return nullptr;
}


/* Movement Algorithm Related */
std::shared_ptr<DataBlockInfo> ZonedDevice::moveBlock(const DataBlockInfo &info, std::shared_ptr<Zone> from, std::shared_ptr<Zone> zone) {
  auto readBytes = from->read(GCBuffer_.data(), info.size, info.znsOffset);
  if (readBytes == 0) {
    return nullptr;
  }

  if (readBytes == info.size) {
    auto movedInfo = writeRegionToZone(GCBuffer_.data(), info.size, info.logOffset, zone);
    if (movedInfo->size == readBytes) {
      XLOGF(DBG, "data move at logical 0x{:X} is done", movedInfo->logOffset);
      std::lock_guard<std::mutex> lck(mappingLock_);
      auto victim = getZone(info.znsOffset);
      auto otherZone = getZone(movedInfo->znsOffset);
      auto it = victim->logicalMapping_.find(info.znsOffset);
      if (it == victim->logicalMapping_.end()) {
        otherZone->changeDataSize(-movedInfo->size);
      } else {
        auto removedInfo = deleteMappingInGC(info);
        XCHECK(removedInfo != nullptr);
        insertMapping(movedInfo);
        otherZone->insertLogicalMapping(movedInfo->znsOffset, movedInfo);
      }
      return movedInfo;
    } else {
      throw std::logic_error("move error.");
    }
  } else {
    XLOGF(ERR, "can't read {} bytes at 0x{:X} for move", info.size, info.znsOffset);
    throw std::ios_base::failure("can read when move data");
  }
}

bool ZonedDevice::moveValidData(std::shared_ptr<Zone> &from, const std::vector<DataBlockInfo> &allValidBlock, 
                                std::vector<std::shared_ptr<Zone>> to, std::vector<std::shared_ptr<DataBlockInfo>> &allMovedInfo) {
  uint64_t offset = from->getStart();
  uint64_t endOffset = from->getStart() + from->getSize();

  XCHECK_EQ(to.size(), 2);
  std::reverse(to.begin(), to.end());

  auto toMoveBytes = from->getDataSize();

  for (const auto &info : allValidBlock) {
    // canAllocate is not locked
    while (not to.back()->canAllocate(info.size)) {
      auto z = to.back();
      z->finish();
      to.pop_back();
      XCHECK_EQ(to.size(), 1);
    }

    auto zone = to.back();

    const auto evictStartTime = getSteadyClock();
    auto movedInfo = moveBlock(info, from, zone);
    XLOGF(DBG, "move one region time {} us", toMicros(getSteadyClock() - evictStartTime).count());

    if (movedInfo) {
      allMovedInfo.push_back(movedInfo);
    } else {
      // XLOGF(ERR, "callback return false when handle block zns: 0x{:X} log: 0x{:X} sz: {}.", info.znsOffset, info.logOffset, info.size);
      // throw std::logic_error("callback return false");
    }
  }
  XCHECK(to.size() == 2 or to.size() == 1);
  return to.size() == 1;
}

void ZonedDevice::startMovement() {
  while (not stopGC.ready()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    zoneStateLock_.lock();
    if (not needReclaim()) {
      zoneStateLock_.unlock();
      continue;
    }
    XLOGF(DBG, "current have {} writing zones.", writingZones_.size());
    auto must = writingZones_.size() < maxWrtingZoneNumber;
    auto victim = findVictim(must);
    if (victim == nullptr) {
      zoneStateLock_.unlock();
      continue;
    }
    XLOGF(INFO, "find victim {} data size {} M {} must {}.", victim->getZoneId(), victim->getDataSize() / 1024 / 1024, victim->getDataSize(), must ? "true" : "false");
    zoneStateLock_.unlock();

    std::vector<DataBlockInfo> allValidBlock;
    std::vector<std::shared_ptr<DataBlockInfo>> allMovedBlock;
    auto it = victim->logicalMapping_.begin();
    while (it != victim->logicalMapping_.end()) {
      allValidBlock.push_back(*it->second);
      it++;
    }

    // 2. I/O
    auto useTemp = moveValidData(victim, allValidBlock, 
                                       {GCZone_, tempZone_}, allMovedBlock);


    // relock mapping
    std::lock_guard<std::mutex> mlck(mappingLock_);
    std::lock_guard<std::mutex> vlck(zoneStateLock_);

    victim->lock_.lock();
    victim->reset();
    victim->lock_.unlock();

    if (useTemp) {
      // make sure GCZone_ is not using by other thread
      auto nReadingPtr = std::move(GCZone_);
      GCZone_ = std::move(tempZone_);
      tempZone_ = std::move(victim);
      readingZones_.push_back(nReadingPtr);
    } else {
      auto nWritingPtr = std::move(victim);
      nWritingPtr->close();
      writingZones_.insert(writingZones_.begin(), nWritingPtr);
    }
  }
}

/* Mapping Related */
void ZonedDevice::insertMapping(std::shared_ptr<DataBlockInfo> info) {
  XLOGF(DBG, "insert logical offset 0x{:X}'s mapping to 0x{:X}, size is 0x{:X}.", info->logOffset, info->znsOffset, info->size);
  mapping_.emplace(info->logOffset + info->size, info);
}

std::shared_ptr<DataBlockInfo> ZonedDevice::deleteMappingInGC(const DataBlockInfo& info) {
  XLOGF(DBG, "try to delete logical offset 0x{:X}'s mapping in GC.", info.logOffset);
  auto it = mapping_.find(info.logOffset + info.size);
  if (it == mapping_.end()) {
    return nullptr;
  }
  if (it->second->znsOffset != info.znsOffset) {
    // avoid data mapping was removed after cleaning
    return nullptr;
  }
  XCHECK_EQ(info.size, it->second->size);
  auto res = it->second;
  XLOGF(DBG, "delete logical offset 0x{:X}'s mapping success in GC.", info.logOffset);
  mapping_.erase(it);
  return res;
}

std::shared_ptr<DataBlockInfo> ZonedDevice::deleteMapping(std::shared_ptr<DataBlockInfo> info) {
  XLOGF(DBG, "delete logical offset 0x{:X}'s mapping.", info->logOffset);
  auto it = mapping_.find(info->logOffset + info->size);
  if (it == mapping_.end()) {
    // XLOGF(ERR, "can not find logical offset 0x{:X}'s mapping for deleting.", logOffset);
    return nullptr;
  }
  XCHECK_EQ(info->size, it->second->size);
  auto res = it->second;
  XLOGF(DBG, "delete logical offset 0x{:X}'s mapping success.", info->logOffset);
  mapping_.erase(it);
  return res;
}

void ZonedDevice::flush() {
  std::lock_guard<std::mutex> l(mappingLock_);
  auto res = fsync(fd_);
  if (res == -1) {
    throw std::ios_base::failure("flash to zns device error.");
  }
}

std::unique_ptr<Device> createZonedDevice(
    std::string devPath,
    uint64_t size,
    uint32_t zoneNum,
    uint32_t ioAlignSize,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize) {
      return std::make_unique<ZonedDevice>(devPath, size, zoneNum, ioAlignSize, encryptor, maxDeviceWriteSize, O_RDWR);
}

 std::unique_ptr<Device> createDirectIoZNSDevice(
     folly::StringPiece file,
     uint64_t size,
     uint32_t ioAlignSize,
     std::shared_ptr<DeviceEncryptor> encryptor,
     uint32_t maxDeviceWriteSize) {
   XDCHECK(folly::isPowTwo(ioAlignSize));
  return std::make_unique<DirectZonedDevice>(file.str(), size, ioAlignSize, encryptor, maxDeviceWriteSize, O_RDWR);
}

}
}
}