# ZNS-Cache

Source code for *Can ZNS SSDs be Better Storage Devices for Persistent Cache?* in HotStorage '24.

## Build

Our Code is based on [CacheLib](<https://github.com/facebook/CacheLib>). You can follow the steps in https://cachelib.org/docs/installation.

```bash
# install libzbd and libexplain
git clone https://github.com/asu-idi/ZNS-Cache
cd ZNS-Cache
./contrib/build.sh -j -T

# The resulting library and executables:
./opt/cachelib/bin/cachebench --help
```

## Benchmark

The configuration files can be found in `./hotstorage/configs`.

> [!CAUTION]
> The device path must be updated before execution.

```bash
./opt/cachelib/bin/cachebench \
--json_test_config ./hotstorage/configs/zone-cache-bc.json \
--logging=INFO
```

