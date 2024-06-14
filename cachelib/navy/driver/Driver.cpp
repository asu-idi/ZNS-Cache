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

#include "cachelib/navy/driver/Driver.h"

#include <folly/synchronization/Baton.h>

#include "cachelib/navy/admission_policy/DynamicRandomAP.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/driver/NoopEngine.h"
#include "folly/Format.h"

namespace facebook {
namespace cachelib {
namespace navy {
Driver::Config& Driver::Config::validate() {
  if (smallItemCache != nullptr && smallItemMaxSize == 0) {
    throw std::invalid_argument("invalid small item cache params");
  }
  if (smallItemCache != nullptr) {
    if (smallItemMaxSize > smallItemCache->getMaxItemSize()) {
      throw std::invalid_argument(folly::sformat(
          "small item max size should not excceed: {}, but is set to be: {}",
          smallItemCache->getMaxItemSize(), smallItemMaxSize));
    }
  }
  return *this;
}

Driver::Driver(Config&& config)
    : Driver{std::move(config.validate()), ValidConfigTag{}} {}

Driver::Driver(Config&& config, ValidConfigTag)
    : smallItemMaxSize_{config.smallItemCache ? config.smallItemMaxSize : 0},
      maxConcurrentInserts_{config.maxConcurrentInserts},
      maxParcelMemory_{config.maxParcelMemory},
      metadataSize_{config.metadataSize},
      device_{std::move(config.device)},
      deviceForBigHash_{std::move(config.deviceForBigHash)},
      scheduler_{std::move(config.scheduler)},
      largeItemCache_{std::move(config.largeItemCache)},
      smallItemCache_{std::move(config.smallItemCache)},
      admissionPolicy_{std::move(config.admissionPolicy)} {
  if (!largeItemCache_) {
    XLOG(INFO, "Large item cache is noop");
    largeItemCache_ = std::make_unique<NoopEngine>();
  }
  if (!smallItemCache_) {
    XLOG(INFO, "Small item cache is noop");
    smallItemCache_ = std::make_unique<NoopEngine>();
  }
  XLOGF(INFO, "Max concurrent inserts: {}", maxConcurrentInserts_);
  XLOGF(INFO, "Max parcel memory: {}", maxParcelMemory_);
}

Driver::~Driver() {
  XLOG(INFO, "Driver: finish scheduler");
  scheduler_->finish();
  XLOG(INFO, "Driver: finish scheduler successful");
  // Destroy this for safety first
  scheduler_.reset();
}

std::pair<Engine&, Engine&> Driver::select(HashedKey key,
                                           BufferView value) const {
  if (isItemLarge(key, value)) {
    return {*largeItemCache_, *smallItemCache_};
  } else {
    return {*smallItemCache_, *largeItemCache_};
  }
}

bool Driver::isItemLarge(HashedKey key, BufferView value) const {
  return key.key().size() + value.size() > smallItemMaxSize_;
}

bool Driver::couldExist(HashedKey hk) {
  auto couldExist =
      smallItemCache_->couldExist(hk) || largeItemCache_->couldExist(hk);
  if (!couldExist) {
    lookupCount_.inc();
  }
  return couldExist;
}

Status Driver::insert(HashedKey key, BufferView value) {
  folly::Baton<> done;
  Status cbStatus{Status::Ok};
  auto status = insertAsync(key, value,
                            [&done, &cbStatus](Status s, HashedKey /* key */) {
                              cbStatus = s;
                              done.post();
                            });
  if (status != Status::Ok) {
    return status;
  }
  done.wait();
  return cbStatus;
}

bool Driver::admissionTest(HashedKey hk, BufferView value) const {
  // If this parcel makes our memory above the limit, we reject it and
  // revert back increment we made. We can't split check and increment!
  // We can't check value before - it will over admit things. Same with
  // concurrent inserts.
  size_t parcelSize = hk.key().size() + value.size();
  auto currParcelMemory = parcelMemory_.add_fetch(parcelSize);
  auto currConcurrentInserts = concurrentInserts_.add_fetch(1);

  if (!admissionPolicy_ || admissionPolicy_->accept(hk, value)) {
    if (currConcurrentInserts <= maxConcurrentInserts_) {
      if (currParcelMemory <= maxParcelMemory_) {
        acceptedCount_.inc();
        acceptedBytes_.add(parcelSize);
        return true;
      } else {
        rejectedParcelMemoryCount_.inc();
      }
    } else {
      rejectedConcurrentInsertsCount_.inc();
    }
  }
  rejectedCount_.inc();
  rejectedBytes_.add(parcelSize);

  // Revert counter modifications. Remember, can't assign back atomic.
  concurrentInserts_.dec();
  parcelMemory_.sub(parcelSize);

  return false;
}

Status Driver::insertAsync(HashedKey hk, BufferView value, InsertCallback cb) {
  insertCount_.inc();

  if (hk.key().size() > kMaxKeySize) {
    rejectedCount_.inc();
    rejectedBytes_.add(hk.key().size() + value.size());
    return Status::Rejected;
  }

  if (!admissionTest(hk, value)) {
    return Status::Rejected;
  }

  auto timeBegin = getSteadyClock();

  scheduler_->enqueueWithKey(
      [this, cb = std::move(cb), hk, value, timeBegin, skipInsertion = false]() mutable {
        auto selection = select(hk, value);
        Status status = Status::Ok;
        if (!skipInsertion) {
          status = selection.first.insert(hk, value);
          if (status == Status::Retry) {
            return JobExitCode::Reschedule;
          }
          skipInsertion = true;
        }
        if (status != Status::DeviceError) {
          auto rs = selection.second.remove(hk);
          if (rs == Status::Retry) {
            return JobExitCode::Reschedule;
          }
          if (rs != Status::Ok && rs != Status::NotFound) {
            XLOGF(ERR, "Insert failed to remove other: {}", toString(rs));
            status = Status::BadState;
          }
        }

        if (cb) {
          cb(status, hk);
        }
        parcelMemory_.sub(hk.key().size() + value.size());
        concurrentInserts_.dec();

        switch (status) {
        case Status::Ok:
          writeLatencyEstimator_.trackValue(
              toMicros(getSteadyClock() - timeBegin).count());
          succInsertCount_.inc();
          break;
        case Status::BadState:
        case Status::DeviceError:
          ioErrorCount_.inc();
          break;
        default:;
        }
        return JobExitCode::Done;
      },
      "insert",
      JobType::Write,
      hk.keyHash());
  return Status::Ok;
}

void Driver::updateLookupStats(Status status) const {
  switch (status) {
  case Status::Ok:
    succLookupCount_.inc();
    break;
  case Status::DeviceError:
    ioErrorCount_.inc();
    break;
  default:;
  }
}

Status Driver::lookup(HashedKey hk, Buffer& value) {
  // We do busy wait because we don't expect many retries.
  lookupCount_.inc();
  Status status{Status::NotFound};
  while ((status = largeItemCache_->lookup(hk, value)) == Status::Retry) {
    std::this_thread::yield();
  }
  if (status == Status::NotFound) {
    while ((status = smallItemCache_->lookup(hk, value)) == Status::Retry) {
      std::this_thread::yield();
    }
  }
  updateLookupStats(status);
  return status;
}

Status Driver::lookupAsync(HashedKey hk, LookupCallback cb) {
  lookupCount_.inc();
  XDCHECK(cb);

  auto timeBegin = getSteadyClock();
  scheduler_->enqueueWithKey(
      [this, cb = std::move(cb), hk, timeBegin, skipLargeItemCache = false]() mutable {
        Buffer value;
        Status status{Status::NotFound};
        if (!skipLargeItemCache) {
          status = largeItemCache_->lookup(hk, value);
          if (status == Status::Retry) {
            return JobExitCode::Reschedule;
          }
          skipLargeItemCache = true;
        }
        if (status == Status::NotFound) {
          status = smallItemCache_->lookup(hk, value);
          if (status == Status::Retry) {
            return JobExitCode::Reschedule;
          }
        }

        if (status == Status::Ok) {
          readLatencyEstimator_.trackValue(
              toMicros(getSteadyClock() - timeBegin).count());
        }
        if (cb) {
          cb(status, hk, std::move(value));
        }

        updateLookupStats(status);
        return JobExitCode::Done;
      },
      "lookup",
      JobType::Read,
      hk.keyHash());
  return Status::Ok;
}

Status Driver::removeHashedKey(HashedKey hk, bool& skipSmallItemCache) {
  removeCount_.inc();
  Status status = Status::NotFound;
  if (!skipSmallItemCache) {
    status = smallItemCache_->remove(hk);
  }
  if (status == Status::NotFound) {
    status = largeItemCache_->remove(hk);
    skipSmallItemCache = true;
  }
  switch (status) {
  case Status::Ok:
    succRemoveCount_.inc();
    break;
  case Status::DeviceError:
    ioErrorCount_.inc();
    break;
  default:;
  }
  return status;
}

Status Driver::remove(HashedKey hk) {
  Status status{Status::Ok};
  bool skipSmallItemCache = false;
  while ((status = removeHashedKey(hk, skipSmallItemCache)) == Status::Retry) {
    std::this_thread::yield();
  }
  return status;
}

Status Driver::removeAsync(HashedKey hk, RemoveCallback cb) {
  scheduler_->enqueueWithKey(
      [this, cb = std::move(cb), hk = hk,
       skipSmallItemCache = false]() mutable {
        auto status = removeHashedKey(hk, skipSmallItemCache);
        if (status == Status::Retry) {
          return JobExitCode::Reschedule;
        }
        if (cb) {
          cb(status, hk);
        }
        return JobExitCode::Done;
      },
      "remove",
      JobType::Write,
      hk.keyHash());
  return Status::Ok;
}

void Driver::flush() {
  scheduler_->finish();
  smallItemCache_->flush();
  largeItemCache_->flush();
}

void Driver::reset() {
  XLOG(INFO, "Reset Navy");
  scheduler_->finish();
  smallItemCache_->reset();
  largeItemCache_->reset();
  if (admissionPolicy_) {
    admissionPolicy_->reset();
  }
}

void Driver::persist() const {
  auto rw = createMetadataRecordWriter(*device_, metadataSize_);
  if (rw) {
    largeItemCache_->persist(*rw);
    smallItemCache_->persist(*rw);
  }
}

bool Driver::recover() {
  auto rr = createMetadataRecordReader(*device_, metadataSize_);
  if (!rr) {
    return false;
  }
  if (rr->isEnd()) {
    return false;
  }
  // Because we insert item and remove from the other engine, partial recovery
  // is potentially possible.
  bool recovered =
      largeItemCache_->recover(*rr) && smallItemCache_->recover(*rr);
  if (!recovered) {
    reset();
  }
  if (recovered) {
    // If recovery is successful, invalidate the metadata
    auto rw = createMetadataRecordWriter(*device_, metadataSize_);
    if (rw) {
      return rw->invalidate();
    } else {
      recovered = false;
    }
  }
  return recovered;
}

bool Driver::updateMaxRateForDynamicRandomAP(uint64_t maxRate) {
  DynamicRandomAP* ptr = dynamic_cast<DynamicRandomAP*>(admissionPolicy_.get());
  if (ptr) {
    ptr->setMaxWriteRate(maxRate);
    return true;
  }
  return false;
}

uint64_t Driver::getSize() const { return device_->getSize(); }

void Driver::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_inserts", insertCount_.get());
  visitor("navy_succ_inserts", succInsertCount_.get());
  visitor("navy_lookups", lookupCount_.get());
  visitor("navy_succ_lookups", succLookupCount_.get());
  visitor("navy_removes", removeCount_.get());
  visitor("navy_succ_removes", succRemoveCount_.get());
  visitor("navy_rejected", rejectedCount_.get());
  visitor("navy_rejected_concurrent_inserts",
          rejectedConcurrentInsertsCount_.get());
  visitor("navy_rejected_parcel_memory", rejectedParcelMemoryCount_.get());
  visitor("navy_rejected_bytes", rejectedBytes_.get());
  visitor("navy_accepted_bytes", acceptedBytes_.get());
  visitor("navy_accepted", acceptedCount_.get());
  visitor("navy_io_errors", ioErrorCount_.get());
  visitor("navy_parcel_memory", parcelMemory_.get());
  visitor("navy_concurrent_inserts", concurrentInserts_.get());
  readLatencyEstimator_.visitQuantileEstimator(visitor,
                                               "navy_cache_read_latency_us");
  writeLatencyEstimator_.visitQuantileEstimator(visitor,
                                                "navy_cache_write_latency_us");
  scheduler_->getCounters(visitor);
  largeItemCache_->getCounters(visitor);
  smallItemCache_->getCounters(visitor);
  if (admissionPolicy_) {
    admissionPolicy_->getCounters(visitor);
  }
  // Can be nullptr in driver tests
  if (device_) {
    device_->getCounters(visitor);
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
