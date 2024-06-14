#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <stdexcept>
#include <tuple>

#include <folly/logging/xlog.h>
#include <libexplain/pwrite.h>
#include <set>
#include <bitset>
#include <libzbd/zbd.h>
namespace facebook {
namespace cachelib {
namespace navy {

using ZoneInvalidData = std::set<std::pair<uint64_t, uint64_t>>;

struct DataBlockInfo {
  uint64_t znsOffset;
  // logical offset
  uint64_t logOffset;
  uint64_t size;
  DataBlockInfo(uint64_t znsOffset, uint64_t size, uint64_t logOffset) :
    znsOffset(znsOffset), size(size), logOffset(logOffset) {}
};

// TO REMOVE
enum CallResult {
  SUCCESS,
  RETRY,
  DENY,
  FAILED
};

enum AppendStatus {
  DONE,
  NOSPACE,
  ERROR
};

struct AppendResult {
  AppendStatus status;
  uint64_t dataOffset;
};


struct ZoneSlot {
  uint32_t zone_id;
  uint32_t offset;
  bool operator < (const ZoneSlot& rhs) const;
  bool operator == (const ZoneSlot& p) const;
};

class ZoneSlotHashFunction {
public:
  size_t operator()(const ZoneSlot& p) const{
    size_t zone_id = std::hash<int>()(p.zone_id);
    size_t offset = std::hash<int>()(p.offset) << 1;
    return zone_id ^ offset;
  }
};

inline bool ZoneSlot::operator==(const ZoneSlot& p) const {
    return zone_id == p.zone_id && offset == p.offset;
}

inline bool ZoneSlot::operator < (const ZoneSlot& rhs) const {
    if(zone_id < rhs.zone_id) return true;
    if(zone_id > rhs.zone_id) return false;
    if(offset < rhs.offset) return true;
    if(offset > rhs.offset) return false;
    return false;
}

class Zone {
 public:
  Zone(int fd, uint64_t zoneId, uint64_t start, uint64_t len, uint64_t size) : 
   fd_(fd), zoneId_(zoneId), start_(start), len_(len), size_(size), writePointer_(start) {
  }

  uint64_t getWritePointer() {
    // std::lock_guard<std::mutex> l(lock_);
    return writePointer_;
  }
  uint64_t getSize() const {return size_;}
  uint64_t getZoneId() const {return zoneId_;}
  uint64_t getStart() const {return start_;}

  void changeDataSize(int64_t delta) {
    dataSize_ = dataSize_ + delta;
  }
  
  bool canAllocate(uint64_t size) {
    return writePointer_ + size <= start_ + size_;
  }

  // TO REMOVE
  std::tuple<CallResult, uint64_t> allocate(uint64_t size) {
    // assert canAllocate(size)
    XDCHECK(canAllocate(size));
    if (writePointer_ == start_) {
      open();
    }
    writePointer_ += size;
    return {CallResult::SUCCESS, writePointer_};
  }

  // thread safe
  AppendResult append(const void *buf, uint64_t size, bool flashAfterWrite=false) {
    // std::lock_guard<std::mutex> l(lock_);
    // assert canAllocate(size)
    XCHECK_EQ((uint64_t) buf % 4096, 0);
    // XCHECK_EQ(alignof(buf), 4096);
    if (not canAllocate(size)) return {AppendStatus::NOSPACE, 0};

    // TODO: open cnt
    if (writePointer_ == start_) {
      open();
    }
    auto dataOffset = writePointer_;
    auto sz = pwrite(fd_, buf, size, dataOffset);

    if (sz != size) {
      XLOGF(ERR, "append 0x{:X} bytes to physic offset 0x{:X}, sz is {}.", size, writePointer_, sz);
      XLOGF(ERR, "{}", explain_pwrite(fd_, buf, size, dataOffset));
      throw std::ios_base::failure("append to zns error!");
    }

    writePointer_ += size;
    dataSize_ += size;

    if (flashAfterWrite) {
      auto err = fsync(fd_);
      if (err != 0) {
        XLOGF(ERR, "flush error after append 0x{:X} bytes to physic offset 0x{:X}, sz is {}.", size, writePointer_, sz);
        throw std::ios_base::failure("flush zns error!");
      }
    }

    return {AppendStatus::DONE, dataOffset};
  }

  ssize_t read(uint8_t* buf, uint64_t size, uint64_t offset) {
    return pread(fd_, buf, size, offset);
  }

  bool open() {
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_OPEN);
  }

  bool reset() {
    XLOG(DBG, "lock invalidlock in reset");
    // ensure logical is cleared
    logicalMapping_.clear();
    dataSize_ = 0;
    writePointer_ = start_;
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_RESET);
  }

  bool finish() {
    // std::lock_guard<std::mutex> l(lock_);
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_FINISH);
  }

  bool close() {
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_CLOSE);
  }

  uint32_t getFreeSizeLocked() const {
    return size_ - writePointer_;
  }

  double getFreePercentLocked() const {
    return (size_ - writePointer_) / (double) size_;
  }

  uint64_t getDataSize() const {
    return dataSize_;
  }

  void insertLogicalMapping(uint64_t k, std::shared_ptr<DataBlockInfo> v) {
    // std::lock_guard<std::mutex> lk(lock_);
    logicalMapping_.emplace(k, v);
  }

  void deleteLogicalMapping(uint64_t k) {
    // std::lock_guard<std::mutex> lk(lock_);
    auto it = logicalMapping_.find(k);
    if (it != logicalMapping_.end()) {
      logicalMapping_.erase(it);
      dataSize_ -= it->second->size;
    }
  }

  mutable std::shared_mutex lock_;

  // 4k block
  std::map<uint64_t, std::shared_ptr<DataBlockInfo>> logicalMapping_;

 private:

  // basic info
  const uint64_t zoneId_;
  const uint64_t start_;
  const uint64_t size_;
  const uint64_t len_;
  const int fd_;

  uint64_t writePointer_{0};
  uint64_t dataSize_{0};

  bool doZoneCtrlOperation(zbd_zone_op op) {
    auto res = zbd_zones_operation(fd_, op, start_, len_);
    return res;
  }

};

}
}
}