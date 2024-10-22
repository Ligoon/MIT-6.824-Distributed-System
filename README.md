# MIT 6.824 Distributed System

- Lab1: MapReduce (moderate/hard) - Completed 09/04/2024
- Lab2: Raft - TODO
    - Lab2A: leader election (moderate) - Completed 09/24/2024
    - Lab2B: log (hard) - Completed 10/22/2024
    - Lab2C: persistence (hard)
    - Lab2D: log compaction (hard)
- Lab3: Fault-tolerant Key/Value Service - TODO
    - Lab3A: Key/value service without snapshots (moderate/hard)
    - Lab3B: Key/value service with snapshots (hard)
- Lab4: Sharded Key/Value Service - TODO
    - Lab4A: The Shard controller (30 points)(easy)
    - Lab4B: Sharded Key/Value Server (60 points)(hard)

## Test Result
- Lab1
```
$ bash test-mr.sh                                               
*** Cannot find timeout command; proceeding without timeouts.
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```
- Lab2A
```
$ time go test -run 2A
Test (2A): initial election ...
  ... Passed --   3.0  3   54   15082    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  120   25448    0
Test (2A): multiple elections ...
  ... Passed --   5.6  7  570  120464    0
PASS
ok  	6.5840/raft	13.499s
go test -run 2A  0.45s user 0.43s system 6% cpu 13.990 total
```
- Lab2B
```
$ time go test -run 2B                                       
Test (2B): basic agreement ...
  ... Passed --   0.8  3   18    4800    3
Test (2B): RPC byte count ...
  ... Passed --   1.7  3   48  114164   11
Test (2B): test progressive failure of followers ...
  ... Passed --   4.7  3  120   26765    3
Test (2B): test failure of leaders ...
  ... Passed --   5.1  3  182   44267    3
Test (2B): agreement after follower reconnects ...
  ... Passed --   4.8  3   97   25449    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.7  5  204   44300    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.8  3   14    3972    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.1  3  175   46297    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  19.3  5 1979 1517015  103
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.3  3   42   12336   12
PASS
ok  	6.5840/raft	49.698s
go test -run 2B  1.14s user 0.73s system 3% cpu 50.259 total
```