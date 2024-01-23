# Lab 1

- [Lab 1](#lab-1)
  - [Prerequesite](#prerequesite)
  - [Design \& Implementation](#design--implementation)
    - [Worker Actions](#worker-actions)
    - [Coordinator Actions](#coordinator-actions)
    - [Architecture](#architecture)


## Prerequesite

在具体开始写代码之前需要先将官方提供的 [实验文档](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html) 全部认真看一遍，除此之外还需要详细理解 [`MapReduce`](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf) 的原理，否者这个实验无法进行下去

这个实验的难点在于 `Master/Coordinator` 与 `Worker` 之间需要交流哪些信息以及 `Coordinator` 需要维护哪些信息。由于实验只给出了最基本的 `RPC` 调用示例，关于 `MapReduce` 的部分几乎没有代码，因此可以自由发挥的空间十分的大（~~换句话说，也就是无从下手~~）

## Design & Implementation

### Worker Actions

对于 `Worker` 而言：

- 向 `Coordinator` 请求任务
- 执行 `Map/Reduce Task` 或者处于空闲状态
- 向 `Coordinator` 报告 `Task` 是否执行完毕

`Worker` 的行为有两种：请求任务和报告信息，因此 `Coordinator` 需要提供两个函数供 `Worker` 调用

关于 `Map Task` 和 `Reduce Task`：

- 由于本实验的输入文件全部给出，因此我们不需要按照论文当中说的对输入文件进行分割
- `Coordinator` 通过将输入文件名发送给 `Worker`，之后 `Worker` 便可以读取该文件  
- `Map Task` 的数量是固定的，就是输入文件数；而 `Reduce Task` 则可以由用户进行指定（在 `mrcoordinator.go` 中）
- 假设 `Map Task` 的数量为 $X$，`Reduce Task` 的数量为 $Y$，那么会产生的中间文件个数为 $XY$，每个中间文件用 `mr-i-y` 表示（这里 `i` 表示 `Map Task` 的编号，`j` 表示 `Reduce Task` 的编号）
- 对于编号为 $y$ 的 `Reduce Task` 而言，它需要读取所有 `mr-i-y` 的文件
- 对于每个中间文件，我们可以使用 `os.CreateTemp` 在当前路径创建临时文件，**对文件写入完毕后**使用 `os.Rename` 来对其进行原子重命名
  - 在我最开始的实现中，我直接使用 `os.Create` 来创建一个新文件，然后对文件进行写入。但这种设计存在问题，因为如果两个不同的 `Worker` 执行同一个 `Map Task`，那么它们产生的中间文件就会同名，这时同一个文件就会被两个 `Worker` 进行写入
  - 而我们使用临时文件加原子重命名这种方式则可以避免这个问题，因为我们总是在文件写入完毕后对其重命名，因此哪怕有两个相同的 `Worker` 执行同一个 `Map Task`，最终的结果也只会生成一个文件，并且不存在两个 `Worker` 都向这个文件写入的情况


### Coordinator Actions

对于 `Coordinator` 而言：

- 响应 `Worker` 的请求，向其分配 `Task`，并记录信息
- 对于超时的 `Task`，重新分配一遍

由于我们采用 `RPC` 来实现通信，因此 `Worker` 通过**不断调用（也就是轮询）** `Coordinator` 暴露出来的函数来交流信息。由于这个过程中存在多个 `Worker` 同时执行函数，因此 `Coordinator` 需要加锁保护数据

需要说明的是，`RPC` 所使用的消息的输入参数和回复参数当中的字段都需要大写，不然无法读取到正确的数据

`Coordinator` 需要记录每个 `Task` 的执行情况，对于那些超时未完成的 `Task`，需要重新分配一遍

- 在我的设计中，`Worker` 每隔 `5` 秒向 `Coordinator` 报告一次执行情况用于更新 `Task` 的时间戳
- 如果 `Coordinator` 发现某个 `Task` 的时间戳于当前时间戳超过 `10` 秒，那么认为原先执行该 `Task` 的 `Worker` 已经 `Crash`，直接重新分配该任务

### Architecture

最后给出整体的框架图：

实现代码：[Code](https://github.com/NishikiChisato/MIT6.5840)

![Lab1](./img/Lab1.png)
