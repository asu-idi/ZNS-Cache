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

// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include "cachelib/allocator/CacheAllocator.h"

namespace facebook::cachelib::tests {

// For internal testing ONLY
// This class expose some private testing APIs from CacheAllocator
class CacheAllocatorTestWrapper {
 public:
  template <class Allocator, typename... Params>
  static bool removeFromRamForTesting(Allocator* alloc, Params&&... args) {
    return alloc->removeFromRamForTesting(std::forward<Params>(args)...);
  }

  template <class Allocator, typename... Params>
  static void removeFromNvmForTesting(Allocator* alloc, Params&&... args) {
    alloc->removeFromNvmForTesting(std::forward<Params>(args)...);
  }

  template <class Allocator, typename... Params>
  static bool pushToNvmCacheFromRamForTesting(Allocator* alloc,
                                              Params&&... args) {
    return alloc->pushToNvmCacheFromRamForTesting(
        std::forward<Params>(args)...);
  }

  template <class Allocator>
  static typename Allocator::Item& getParentItem(
      Allocator* alloc, const typename Allocator::Item& child) {
    return child.asChainedItem().getParentItem(alloc->compressor_);
  }

  template <class Allocator>
  static MemoryAllocator& getMemoryAllocator(Allocator* alloc) {
    return *alloc->allocator_;
  }

  template <class Allocator>
  static typename Allocator::WriteHandle createHandle(
      Allocator* alloc, typename Allocator::Item& item) {
    return alloc->acquire(&item);
  }
};

} // namespace facebook::cachelib::tests
