# MIT6.5840

[MIT6.5840(6.524)](https://pdos.csail.mit.edu/6.824/schedule.html): Distributed Systems(Spring 2023)

## Progress

- [x] [Lab 1](./Project/Lab1.md)
- [ ] Lab 2
  - [x] Lab 2A
  - [x] Lab 2B
  - [x] Lab 2C
  - [ ] Lab 2D
- [ ] Lab 3
  - [ ] Lab 3A
  - [ ] Lab 3B
- [ ] Lab 4
  - [ ] Lab 4A
  - [ ] Lab 4B

## Illustration

Complete accuracy of code cannot be guaranteed due to the randomness of execution. I have tested 2A over 150 times and 2B over 50 times, and no failed test cases haved occurred. The election timeout should be selected carefully. In my implementation, I opted low initial value but large interval between initial value and final value. 

## Test

### Lab 2

```bash
go test -run 2A
Test (2A): initial election ...
  ... Passed --   3.0  3   58   16584    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  134   27654    0
Test (2A): multiple elections ...
  ... Passed --   5.5  7  654  136903    0
PASS
ok  	6.5840/raft	13.087s

```

```bash
go test -run 2B
Test (2B): basic agreement ...
  ... Passed --   0.6  3   22    6377    3
Test (2B): RPC byte count ...
  ... Passed --   1.0  3   70  161262   11
Test (2B): test progressive failure of followers ...
  ... Passed --   4.3  3  136   29406    3
Test (2B): test failure of leaders ...
  ... Passed --   4.5  3  186   43143    3
Test (2B): agreement after follower reconnects ...
  ... Passed --   3.7  3  102   27567    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.3  5  244   52354    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.5  3   22    6460    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   5.6  3  188   49511    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  10.7  5 1516 1234584  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.0  3   50   14998   12
PASS
ok  	6.5840/raft	36.257s

```

```bash
go test -run 2C
Test (2C): basic persistence ...
  ... Passed --   2.7  3   96   26609    6
Test (2C): more persistence ...
  ... Passed --  18.0  5 1240  277471   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.7  3   44   11517    4
Test (2C): Figure 8 ...
  ... Passed --  29.8  5 1756  933272   63
Test (2C): unreliable agreement ...
  ... Passed --   4.8  5 89784 27740493  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  32.2  5 4624 7586656  125
Test (2C): churn ...
  ... Passed --  16.4  5 1984 1596565  314
Test (2C): unreliable churn ...
  ... Passed --  28.3  5 384076 253317381  219
PASS
ok  	6.5840/raft	134.032s

```

