# Lab 2






















# log

## Lab 2A

- 发送 `RequestVote RPC` 时一定要等到所有的 `RPC` 都回复之后再进行后续操作。如果要开多个线程的话，必须要等待所有的线程执行完毕才可以

- `Follower` 该如何做到每次选举只进行一次投票 -> `Follower` 该如何区分每次选举

- 当一个 `Candidate` 收到的 `AE RPC` 中的 `term` 与其本身的 `term` 满足什么条件时，该 `Candidate` 需要降级为 `Follower`
  - `leader_term >= candidate_term`，必须是大于等于

- 均使用原子变量，也就是全部使用 `int32`

- `Concurrency Issue`

官方的文档里面给了一种错误加锁的示例：

```go
  rf.mu.Lock()
  rf.currentTerm += 1
  rf.state = Candidate
  for <each peer> {
    go func() {
      rf.mu.Lock()
      args.Term = rf.currentTerm
      rf.mu.Unlock()
      Call("Raft.RequestVote", &args, ...)
      // handle the reply...
    } ()
  }
  rf.mu.Unlock()
```

这种加锁方案会造成问题，这是因为从 `goroutine` 被创建到 `rf.mu.Lock` 被执行的这段时间内，`rf.currentTerm` 有可能会被更改，这就造成了问题。要解决这个问题，我们需要把 `rf.currentTerm` 先复制一份，然后在 `goroutine` 中使用，即

```go
  rf.mu.Lock()
  rf.currentTerm += 1
  rf.state = Candidate
  copy := rf.currentTerm
  for <each peer> {
    go func() {
      rf.mu.Lock()
      args.Term = copy
      rf.mu.Unlock()
      Call("Raft.RequestVote", &args, ...)
      // handle the reply...
    } ()
  }
  rf.mu.Unlock()

```

- 对于 `candidate` 而言，收到的 `vote` 是大于等于 `a majority of server`

- 对于一个 `candidate` 而言，其任何时候都有可能收到 `RPC` 然后转变为 `follower`，**为了不出现死锁**，我们只能在 `Request Vote` 和转变为 `leader` 之后立刻发送 `AppendEntries` 时加锁，其他时候不能加锁

- 在一个 `election` 中，如果某个 `server` 无法回复 `RequestVote`，那么 `candidate` 需要一直重试

- 对于可能会放在 `if` 中的变量，我们不需要 `atomic`；而对于可能放在 `for` 循环判断条件的变量，则需要 `atomic`

- 当线程 `sleep` 时，**不能携带 `lock`**

- 如果当前线程判断其为 `follower`，那么需要 `sleep` 一段时间（`election_timeout`），在 `sleep` 结束后，检查其 `timestamp`，判断是否超时（如果对 `timestamp` 执行复制的话需要在 `sleep` 之后执行复制）

## lab 2B

- `follower` 需要有跳过冗余 `log` 的机制

- 无论 `heartbeat message` 是否存在 `log`，我们都不需要等待其执行完毕；对于有 `log` 的情况，我们在 `go routine` 中对 `matchIndex` 进行更新
  - 我们周期性地检查 `matchIndex`，更新 `committed_index` 即可

- 假设原先的 `leader` 崩溃了，新的 `leader` 该如何更新 `nextIndex` 和 `matchIndex`

- 本实验中，一个断网的 `server` 会不断 `election timeout`，因此其 `term` 会一直增加

- 因此一个 `server` 重新连接回网络后，其状态会变为 `candidate`，所以若 `candiate` 接受到 `AppendEntries`，需要返回 `false`（`leader` 也同理）

- 对于 `RequestVote`，`candidate` 有可能会保持原有状态，而对于 `AppendEntries`，`candidate` 一定要变回 `follower`

- `leader` 只有判断有大多数 `server` 已经**复制**了该 `log entry` ，这可以提交；对于 `follower` 而言，一旦有新的 `log entry` 到达则对其进行提交

- `committed_index` 和 `last_applied` 的区别：
  - 前者记录 `leader` 提交的 `index`，后者记录 `follower` 提交的 `index`
  - 换句话说，对于 `leader` 而言，`last_applied` 没有用；对于 `follower` 而言，`committed_index` 没有用

- 假设一个集群有 `7` 台机器，由于网络故障，导致分为了两组，一组 `3` 个，另一组 `4` 个。这两组分别都会有 `leader`，当网络故障消失时，该如何同步
  - 以提交 `log` 更多的那个 `leader` 为准

- `concurrent map issue`




