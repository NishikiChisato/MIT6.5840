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







