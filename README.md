# MIT6.5840

[MIT6.5840(6.524)](https://pdos.csail.mit.edu/6.824/schedule.html): Distributed Systems(Spring 2023)

## Progress

- [x] [Lab 1](./Project/Lab1.md)
- [x] [Lab 2](./Project/Lab2.md)
  - [x] Lab 2A
  - [x] Lab 2B
  - [x] Lab 2C
  - [x] Lab 2D
- [x] Lab 3
  - [x] Lab 3A
  - [x] Lab 3B
- [ ] Lab 4
  - [ ] Lab 4A
  - [ ] Lab 4B

## Illustration

Complete accuracy of code cannot be guaranteed due to the randomness of execution. Basically, we can nearly all pass unit tests from 2A to 2D over 500 times, and there are only failed tests occurred.

```bash
$ dstest -s -p 20 -n 500 2A 2B 2C 2D
┏━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Test ┃ Failed ┃ Total ┃          Time ┃
┡━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━┩
│ 2A   │      0 │   500 │  13.82 ± 0.51 │
│ 2B   │      0 │   500 │  34.72 ± 1.70 │
│ 2C   │      1 │   500 │ 130.59 ± 5.68 │
│ 2D   │      0 │   500 │ 137.03 ± 3.47 │
└──────┴────────┴───────┴───────────────┘
```

## Test

We take lab2 as an example. Firstly, write the following two files to `/usr/local/bin` and saved them as `dslogs` and `dstest`, resepctively. You can go into [`util.go`](./src/raft/util.go) and set `debug` to allow logs to be open or not.

```bash
#!/bin/bash
if [ $# -eq 0 ]; then
    exit 1
fi
python3 /path/to/your_directory/src/raft/dslogs.py $@
```

```bash
#!/bin/bash
python3 /path/to/your_directory/src/raft/dstest.py $@
```

Also bear in mind that change these file mods to executable. [`dslogs.py`](https://gist.github.com/JJGO/e64c0e8aedb5d464b5f79d3b12197338) and [`dstest.py`](https://gist.github.com/JJGO/0d73540ef7cc2f066cb535156b7cbdab) are provided by [Lab Instruction](https://blog.josejg.com/debugging-pretty/).

The variable `debug` in [`util.go`](./src/raft/util.go) controls whether or not logs are output. You can input `dslogs --help` or `dstest --help` to get ideas of how to use it.

In [`auxiliary.go`](./src/raft/auxiliary.go), The function `WriteLog` can be used to examine which logs have been applied for different servers. If you want to use it, please uncomment `logDebuger` in [`raft.go`](./src/raft/raft.go)

