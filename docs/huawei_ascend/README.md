# Ascend Direct Transport

DLSlime supports RDMA on Ascend NPU via CANN's ADXL transport library.

## Overview

ADXL is a library which provides the RDMA communication APIs on Ascend environments.
DLSlime uses ADXL APIs to register memory region, connect to other devices, read/write data.

## Source code

Under folder `/csrc/engine/ascend_direct`

## Dependencies

- CANN version >= 8.2 including ADXL
- NPU HDK driver version tested on 25.2.0

## Benchmark

### Build the test case

Go to the repo root and compile DLSlime with Asecond Direct
```
mkdir build
cd build
cmake -DBUILD_TEST=ON -DBUILD_ASCEND_DIRECT=ON ..
make -j
```

### Run

Run "initiator" and "target" two instances. For example:

On initiator device:
```
./bin/ascend_direct_perf --mode=target --localhost="10.201.20.25" --local_port=16789 --remote_host="10.201.20.25" --remote_port=16777 --device_id=0
```

On target device:
```
./bin/ascend_direct_perf --mode=initiator --localhost="10.201.20.25" --local_port=16777 --remote_host="10.201.20.25" --remote_port=16789 --device_id=2
```

 - The test makes initiator read data from target.
 - `localhost` and `remote_host` specify the IP address of the two communication devices.
 - `device_id` set the local NPU used to do the test.
 - Then our test binds `local_port` and `remote_port` to zmq sockets to exchange meta data.
 - Once the meta is ready, `local_port` + 1 and `remote_port` + 1 are used to do ADXL rdma connection and transport.
 - Finally you could see perf result like these from the initiator output:

```
Block iteration 0 test completed: duration 98188us, block size 32KB, total size 1024KB, throughput 0.01 GB/s
Block iteration 1 test completed: duration 341us, block size 64KB, total size 2048KB, throughput 6.15 GB/s
Block iteration 2 test completed: duration 326us, block size 128KB, total size 4096KB, throughput 12.87 GB/s
Block iteration 3 test completed: duration 468us, block size 256KB, total size 8192KB, throughput 17.92 GB/s
Block iteration 4 test completed: duration 869us, block size 512KB, total size 16384KB, throughput 19.31 GB/s
Block iteration 5 test completed: duration 1672us, block size 1024KB, total size 32768KB, throughput 20.07 GB/s
Block iteration 6 test completed: duration 3275us, block size 2048KB, total size 65536KB, throughput 20.49 GB/s
Block iteration 7 test completed: duration 6488us, block size 4096KB, total size 131072KB, throughput 20.69 GB/s
Block iteration 8 test completed: duration 12898us, block size 8192KB, total size 262144KB, throughput 20.81 GB/s
Block iteration 9 test completed: duration 25714us, block size 16384KB, total size 524288KB, throughput 20.88 GB/s
```






