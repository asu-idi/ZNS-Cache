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

#include "cachelib/navy/bighash/Bucket.h"

#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {
static_assert(sizeof(Bucket) == 24,
              "Bucket overhead. If this changes, you may have to adjust the "
              "sizes used in unit tests.");

namespace {
const details::BucketEntry* getIteratorEntry(BucketStorage::Allocation itr) {
  return reinterpret_cast<const details::BucketEntry*>(itr.view().data());
}
} // namespace

BufferView Bucket::Iterator::key() const {
  return getIteratorEntry(itr_)->key();
}

uint64_t Bucket::Iterator::keyHash() const {
  return getIteratorEntry(itr_)->keyHash();
}

BufferView Bucket::Iterator::value() const {
  return getIteratorEntry(itr_)->value();
}

bool Bucket::Iterator::keyEqualsTo(HashedKey hk) const {
  return getIteratorEntry(itr_)->keyEqualsTo(hk);
}

uint32_t Bucket::computeChecksum(BufferView view) {
  constexpr auto kChecksumStart = sizeof(checksum_);
  auto data = view.slice(kChecksumStart, view.size() - kChecksumStart);
  return navy::checksum(data);
}

Bucket& Bucket::initNew(MutableBufferView view, uint64_t generationTime) {
  return *new (view.data())
      Bucket(generationTime, view.size() - sizeof(Bucket));
}

BufferView Bucket::find(HashedKey hk) const {
  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (entry->keyEqualsTo(hk)) {
      return entry->value();
    }
    itr = storage_.getNext(itr);
  }
  return {};
}

uint32_t Bucket::insert(HashedKey hk,
                        BufferView value,
                        const DestructorCallback& destructorCb) {
  const auto size =
      details::BucketEntry::computeSize(hk.key().size(), value.size());
  XDCHECK_LE(size, storage_.capacity());

  const auto evictions = makeSpace(size, destructorCb);
  auto alloc = storage_.allocate(size);
  XDCHECK(!alloc.done());
  details::BucketEntry::create(alloc.view(), hk, value);

  return evictions;
}

uint32_t Bucket::makeSpace(uint32_t size,
                           const DestructorCallback& destructorCb) {
  const auto requiredSize = BucketStorage::slotSize(size);
  XDCHECK_LE(requiredSize, storage_.capacity());

  auto curFreeSpace = storage_.remainingCapacity();
  if (curFreeSpace >= requiredSize) {
    return 0;
  }

  uint32_t evictions = 0;
  auto itr = storage_.getFirst();
  while (true) {
    evictions++;

    if (destructorCb) {
      auto* entry = getIteratorEntry(itr);
      destructorCb(
          entry->hashedKey(), entry->value(), DestructorEvent::Recycled);
    }

    curFreeSpace += BucketStorage::slotSize(itr.view().size());
    if (curFreeSpace >= requiredSize) {
      storage_.removeUntil(itr);
      break;
    }
    itr = storage_.getNext(itr);
    XDCHECK(!itr.done());
  }
  return evictions;
}

uint32_t Bucket::remove(HashedKey hk, const DestructorCallback& destructorCb) {
  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (entry->keyEqualsTo(hk)) {
      if (destructorCb) {
        destructorCb(
            entry->hashedKey(), entry->value(), DestructorEvent::Removed);
      }
      storage_.remove(itr);
      return 1;
    }
    itr = storage_.getNext(itr);
  }
  return 0;
}

Bucket::Iterator Bucket::getFirst() const {
  return Iterator{storage_.getFirst()};
}

Bucket::Iterator Bucket::getNext(Iterator itr) const {
  return Iterator{storage_.getNext(itr.itr_)};
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
