我先读取这几个文件，把观察者链路的四段串起来。

继续读取 `ProtocolMetaData` 与 `DumpService#dumpOperate`。

现在追 `LEADER_META_DATA` 的写入方与 `EXTEND_NEED_READ_UNTIL_HAVE_DATA` 的消费方。

追 `RaftEvent` 的发布方与"read until have data"的消费逻辑。

补齐 `NacosStateMachine` 事件发布、`JRaftServer.get` 的读失败语义与 `Observable` 实现。

最后核对 `raftMetaData` 扩展位的消费方与 `ConfigExecutor` 调度池，避免断言无据。

## 结论速览：这是一条"把启动 dump 变成事件响应"的链

四步合起来干的事：**在 derby+raft 模式下，节点启动时本地 Derby 是被清空的、raft 可能还没主，此时全量 dump 必然读不到东西；于是 Nacos 用 `ProtocolMetaData` 的观察者把 `DumpService#dumpOperate()` 挂到 `nacos_config/leader` 这个元数据项上，等 leader 第一次出现时执行一次 dump，做完立刻摘掉监听。**

| 步骤 | 代码位置 | 职责 |
|---|---|---|
| ① `init()` | `EmbeddedDumpService` L82-149 | 分叉：standalone 直接 dump；集群模式改为"注册监听 + 阻塞等待 dump 完成"，失败则抛异常让启动失败 |
| ② `subscribe` | L136-137 → `ProtocolMetaData` L88-90 → L122-125 → `Observable.addObserver` L43-48 | 三层惰性建结构；**注册即回调一次**（Nacos 自研 Observable 与 JDK 不同） |
| ③ `dumpOperate()` | L115 → `DumpService` L208-259 | 真正业务：全量 dump config + gray 到本地磁盘/内存缓存，并注册 4 类周期任务 |
| ④ `unSubscribe` | L116-117 → `ProtocolMetaData` L92-94 → L127-133 | one-shot 收尾：避免 leader 每次变更都重跑 dump、重复注册定时任务 |

---

## 一、全链路时序

```mermaid
sequenceDiagram
    participant SPR as "Spring 启动线程"
    participant DDI as "DistributedDatabaseOperateImpl"
    participant JP as "JRaftProtocol"
    participant FSM as "jraft FSM 线程"
    participant NC as "NotifyCenter 共享线程"
    participant PM as "ProtocolMetaData.ValueItem"
    participant GE as "GlobalExecutor common (4线程)"
    participant DS as "DumpService.dumpOperate"
    participant RD as "Derby (raft 线性读)"

    Note over SPR: Bean 构造期（依赖注入即完成前置初始化）
    SPR->>DDI: new(...)+init() L182
    DDI->>DDI: cleanAndReopenDerby() L192  ★本地库被清空
    DDI->>JP: addRequestProcessors L214 → createMultiRaftGroup
    SPR->>JP: new(...) → protocolMetaData()=metaData
    SPR->>DS: @PostConstruct init() L84
    DS->>PM: subscribe(nacos_config, leader, observer) L136
    PM->>GE: addObserver 立即 o.update(this) Observable L47
    GE->>PM: getData()=null → return  (L105-107)
    SPR->>SPR: latchAwait(waitDumpFinish) L141  ★无超时阻塞

    Note over FSM: 稍后 leader 产生
    FSM->>NC: NacosStateMachine.onLeaderStart L190 / onStartFollowing L204 → publishEvent(RaftEvent)
    NC->>JP: Subscriber.onEvent L128 (单消费者串行)
    JP->>PM: metaData.load(value) L146 → MetaData.put → setData L162
    PM->>PM: writeLock + setChanged + notifyObservers L166-167
    PM->>GE: observer.update → executeByCommon L103
    GE->>GE: putExtendInfo(EXTEND_NEED_READ_UNTIL_HAVE_DATA) L109
    loop for(;;) 直到成功或不可重试
        GE->>DS: dumpOperate() L115
        DS->>RD: findConfigMaxId / findAllConfigInfoFragment
        RD->>RD: blockRead=true → aGetData(request).join() 无超时
    end
    GE->>PM: unSubscribe L116
    GE->>SPR: countDown L126
    GE->>GE: cleanAllContext L131
```

---

## 二、① `init()` 里观察者到底解决什么问题

```java
@PostConstruct
@Override
protected void init() throws Throwable {
    if (EnvUtil.getStandaloneMode()) {   // 单机：没有选主概念，直接 dump
        dumpOperate();
        return;
    }
    CPProtocol protocol = protocolManager.getCpProtocol();
    AtomicReference<Throwable> errorReference = new AtomicReference<>(null);
    CountDownLatch waitDumpFinish = new CountDownLatch(1);
    // watch path => /nacos_config/leader/ has value ?
    Observer observer = new Observer() { ... };
    protocol.protocolMetaData().subscribe(CONFIG_MODEL_RAFT_GROUP, LEADER_META_DATA, observer);
    ThreadUtils.latchAwait(waitDumpFinish);   // ★ 无超时
    final Throwable ex = errorReference.get();
    if (Objects.nonNull(ex)) { throw ex; }    // ★ 触发 Spring 启动失败
}
```

它解决的是**三个耦合在一起的启动竞态**：

**竞态 1：本地库是空的。** `DistributedDatabaseOperateImpl` 的构造函数（L178-184）里就调 `init()`，而 `init()` 第一件事是 `cleanAndReopenDerby()`（L192），随后才 `addRequestProcessors`（L214）建 raft group。也就是说 `EmbeddedDumpService` 开始跑时，`$NACOS_HOME/data/derby-data` 是一个刚重建的空库，真实数据要等 raft 快照加载 + 日志重放才有。

**竞态 2：可能还没有 leader。** dump 的每一次查询都是一次 raft 线性读，无 leader 时直接 `NoLeaderException`（`JRaftServer.invokeToLeader` L424-425，消息为 `"The Raft Group [nacos_config] did not find the Leader node"`）。

**竞态 3：默认读超时太短。** `JRaftProtocol.getData` L167-170 只等 **5 秒**，启动期 raft 正忙（选主、追日志、装快照），5 秒必然不够。

所以 `init()` 的设计是：**不猜时机，改由事件驱动**——`leader` 这个元数据项第一次有值 = "raft 已经可用"，此时才 dump；并且在 dump 线程内用 `for(;;)` + 500ms 重试兜住"leader 有了但状态机还没 ready"的窗口。

两个配套设计要点：

- **构造器注入是刻意的**（L73-80 + `DumpService` L100-104 的注释"ensuring that some of the dependent functionality is initialized ahead of time"）：`EmbeddedDumpService` 依赖 `ConfigInfoPersistService` 等，后者依赖 `DatabaseOperate` → `DistributedDatabaseOperateImpl`，而它的**构造函数**里已经完成了建 raft group。因此 `@PostConstruct init()` 执行时 `nacos_config` group 必然存在，不会出现"订阅了一个永不存在的 group"。
- **`@PostConstruct` + `latchAwait` 把 dump 变成了启动门禁**：`ThreadUtils.latchAwait(latch)`（L61-67）不带超时，Spring 主线程就此挂住，直到 dump 真正完成才继续。这保证容器 ready 时本地缓存已可用——是 fail-fast 而非"先起来再补"。

---

## 三、② `subscribe`：三层惰性结构 + "注册即回调"

```java
// ProtocolMetaData  L88-90
public void subscribe(final String group, final String key, final Observer observer) {
    metaDataMap.computeIfAbsent(group, s -> new MetaData(group)).subscribe(key, observer);
}
// MetaData  L122-125
void subscribe(final String key, final Observer observer) {
    final ValueItem item = itemMap.computeIfAbsent(key, s -> new ValueItem(group + "/" + key));
    item.addObserver(observer);
}
```

结构是 `<group, <key, ValueItem>>`，两级 `computeIfAbsent` **惰性创建**，所以 L137 订阅时 `nacos_config/leader` 这一项即使不存在也会被建出来（`data = null`）。`ValueItem` 的 `path` 就是 `"nacos_config" + "/" + "leader"`，正对应 L94 那句注释里的 `watch path => /nacos_config/leader/`。

### 最关键的一处：Nacos 的 `Observable` 不是 JDK 的

```java
// com.alibaba.nacos.common.utils.Observable  L43-48
public synchronized void addObserver(Observer o) {
    Objects.requireNonNull(o, "Observer");
    obs.add(o);
    observerCnt++;
    o.update(this);        // ★★★ 注册成功后立刻同步回调一次
}
```

`java.util.Observable.addObserver` **不会**回调。Nacos 重写版加了这一行，语义变成"**注册 = 立刻用当前值判一次**"。这解决了一个真实的丢事件问题：

- 若 raft 已经选好主（`metaData.load` 早就跑过），observer 后注册就再也收不到通知了 —— 有了 L47 这次立即回调，subscribe 当场就能拿到 leader 并 dump；
- 反之 leader 尚未产生时，这次立即回调走进 L105-107 的 `Objects.isNull(arg) → return`，什么也不做，latch 不 countDown，`init()` 继续等。

所以 `init()` 的正确性依赖这一行，**这是整段代码里最容易被读漏、也最不能改的地方**（换成 JDK `Observable` 或者去掉这行，就会出现"leader 早于 subscribe 出现 → 永不 dump → 启动永久挂死"）。

### 谁来写这个值

```java
// NacosStateMachine  L185-192 / L200-207
public void onLeaderStart(long term) { ...; NotifyCenter.publishEvent(RaftEvent.builder().groupId(groupId).leader(leaderIp)...build()); }
public void onStartFollowing(LeaderChangeContext ctx) { ...; NotifyCenter.publishEvent(RaftEvent...leader(ctx.getLeaderId().getEndpoint().toString())...build()); }

// JRaftProtocol.init  L126-150（registerToSharePublisher L120，注释 L124-125 说明只有一个消费者保证串行）
MapUtil.putIfValNoEmpty(properties, MetadataKey.LEADER_META_DATA, leader);   // L140  leader 为空则不写
value.put(groupId, properties);
metaData.load(value);                                                        // L146
injectProtocolMetaData(metaData);                                            // L149 → Member.extendVal("raftMetaData")

// ProtocolMetaData  L162-171
void setData(Object data) {
    writeLock.lock();
    try { this.data = data; setChanged(); notifyObservers(); } finally { writeLock.unlock(); }
}
```

三条值得注意的性质：

1. **`onLeaderStop` 不发事件**（L195-198 只把 `isLeader` 置 false），加上 `putIfValNoEmpty` 的过滤 —— 意味着 `leader` 这一项**只会被写成非空值，永远不会被写空**。因此这个观察者的语义严格来说是"leader 出现"，而不是"leader 变化"。
2. `notifyObservers` 是在 **`ValueItem` 的 writeLock 内**同步遍历 observer 的（`Observable` L86-88），所以 observer 必须**只做一件事：把活儿丢给 `GlobalExecutor`**（L103），立刻返回。若 observer 里直接同步跑 dump，就会长时间持有 writeLock，进而把所有 `getData()`（readLock，L154）以及 NotifyCenter 共享线程一起堵住。
3. 写入线程不是 raft 线程：`RaftEvent extends SlowEvent` 且 `registerToSharePublisher`（L120），所以 `setData` 发生在 NotifyCenter 的共享异步线程上；`NacosStateMachine.onLeaderStart` 本身只 publish 就返回，不阻塞状态机。

---

## 四、③ 回调里的实际业务：`dumpOperate()` 与"读到有数据为止"

```java
GlobalExecutor.executeByCommon(() -> {
    if (Objects.isNull(arg)) { return; }                                   // (a) leader 还没值
    EmbeddedStorageContextHolder.putExtendInfo(EXTEND_NEED_READ_UNTIL_HAVE_DATA, "true");  // (b)
    boolean canEnd = false;
    for ( ; ; ) {                                                          // (c)
        try {
            dumpOperate();
            protocol.protocolMetaData().unSubscribe(CONFIG_MODEL_RAFT_GROUP, LEADER_META_DATA, this);
            canEnd = true;                                                 // (d)
        } catch (Throwable ex) {
            if (!shouldRetry(ex)) { errorReference.set(ex); canEnd = true; }
        }
        if (canEnd) { ThreadUtils.countDown(waitDumpFinish); break; }
        ThreadUtils.sleep(500L);
    }
    EmbeddedStorageContextHolder.cleanAllContext();                        // (e)
});
```

### (b) `EXTEND_NEED_READ_UNTIL_HAVE_DATA` 是整段的"开关"

这个 key 本身很有辨识度：`PersistenceConstant` L51 定义为 `"00--0-read-join-0--00"`，注释写的是 "read-join"。它是 `EmbeddedStorageContextHolder` 的一个 **ThreadLocal**（`EXTEND_INFO_CONTEXT`，L35），作用只有一处：

```java
// DistributedDatabaseOperateImpl.queryMany / queryOne 等 6 个方法里都有同一句 L329-330
final boolean blockRead = EmbeddedStorageContextHolder.containsExtendInfo(EXTEND_NEED_READ_UNTIL_HAVE_DATA);
Response response = innerRead(ReadRequest..., blockRead);

// L376-381
private Response innerRead(ReadRequest request, boolean blockRead) throws Exception {
    if (blockRead) { return (Response) protocol.aGetData(request).join(); }   // ★ 无超时
    return protocol.getData(request);                                         // 5s 超时
}
```

正因为设置了它，dump 期间的**所有查询都变成"无限等待的异步读"**，绕开了 5 秒超时。注意它只在**当前这条 `com.alibaba.nacos.core.common` 线程**上生效（池大小 4，`GlobalExecutor` L37-39），不影响业务请求线程 —— 这就是用 ThreadLocal 而不是全局配置的原因：**把"启动期容忍无限等待"这个特例严格限制在自己的线程里**。

### dumpOperate 真正干了四件事

```java
protected void dumpOperate() throws NacosException {
    TimerContext.start("CONFIG_DUMP_TO_FILE");
    try {
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        try {
            dumpAllConfigInfoOnStartup(dumpAllProcessor);          // ① clearAll() + DumpAllTask(true)
            ConfigDiskServiceFactory.getInstance().clearAllGray(); // ② 清灰度缓存
            dumpAllGrayProcessor.process(new DumpAllGrayTask());   //    dump 灰度配置
        } catch (Exception e) {
            throw new NacosException(SERVER_ERROR, "Nacos Server did not start because dumpservice bean construction failure :\n" + ...);
        }
        if (!EnvUtil.getStandaloneMode()) {
            long initialDelay = random.nextInt(INITIAL_DELAY_IN_MINUTE) + 10;      // ③ 6h 周期的全量/灰度全量
            ConfigExecutor.scheduleConfigTask(new DumpAllProcessorRunner(), initialDelay, DUMP_ALL_INTERVAL_IN_MINUTE, MINUTES);
            ConfigExecutor.scheduleConfigTask(new DumpAllGrayProcessorRunner(), initialDelay, DUMP_ALL_INTERVAL_IN_MINUTE, MINUTES);
            ConfigExecutor.scheduleConfigChangeTask(new DumpChangeConfigWorker(...), ...);      //    增量补偿
            ConfigExecutor.scheduleConfigChangeTask(new DumpChangeGrayConfigWorker(...), ...);
        }
        ConfigExecutor.scheduleConfigTask(new ConfigHistoryClear(cleaner), 10, 10, TimeUnit.MINUTES);  // ④ 历史清理
    } finally { TimerContext.end("CONFIG_DUMP_TO_FILE", LogUtil.DUMP_LOG); }
}
```

① 是"为什么必须等 leader"的直接答案：`DumpAllProcessor.process` L63 第一件事就是 `configInfoPersistService.findConfigMaxId()` → `databaseOperate.queryOne` → raft 线性读；然后 L82 分页 `findAllConfigInfoFragment` 也是。这些读全部经过 `DistributedDatabaseOperateImpl.onRequest`（readLock，L514 才 unlock）。

③④ 说明**"为什么 dump 只能执行一次"**：`dumpOperate` 会往 `ConfigExecutor` 的共享调度池注册 5 个周期任务。它不是幂等的"跑一遍就完事"，重复调用等于重复注册。这一点直接决定了第 ④ 步 `unSubscribe` 的必要性。

### 失败分类：白名单重试 vs fail-fast

```java
final String[] retryMessages = {"The conformance protocol is temporarily unavailable for reading"};   // L59
final String[] errorMessages  = {"FSMCaller is overload.", "STATE_ERROR"};                            // L64

private boolean shouldRetry(Throwable ex) {
    for (String failedMsg : errorMessages)     { if (containsIgnoreCase(errMsg, failedMsg)) return false; }  // 先判不可重试
    for (final String retryMsg : retryMessages){ if (containsIgnoreCase(errMsg, retryMsg))  return true;   }
    return false;                                                                            // ★ 默认不重试
}
```

两类语义在 L62 的注释里点得很清楚：*"If it's just a normal reading failure, it can be resolved by retrying"* vs *"If the read failed due to an internal problem in the Raft state machine, it cannot be remedied by retrying"*。

对应的抛出点在 `JRaftServer.get`：

```java
if (status.isOk()) {
    try { future.complete(processor.onRequest(request)); }
    catch (Throwable t) { future.completeExceptionally(new ConsistencyException("The conformance protocol is temporarily unavailable for reading", t)); }  // L295-296 → 可重试
} else { readFromLeader(request, future); }      // L303 → 走 commit → applyOperation → node.apply
```

也就是说：**"readIndex 成功但 SQL 执行失败"（典型如 derby 表还没被快照/日志建出来、或正在装快照）→ 可重试**；而"FSMCaller is overload / STATE_ERROR"（jraft 状态机内部错误）→ 直接判死。

---

## 五、④ `unSubscribe`：one-shot 监听器的收尾

```java
// ProtocolMetaData L92-94  —— 注意与 subscribe 对称地也用了 computeIfAbsent
public void unSubscribe(final String group, final String key, final Observer observer) {
    metaDataMap.computeIfAbsent(group, s -> new MetaData(group)).unSubscribe(key, observer);
}
// MetaData L127-133  —— 这里退化为普通 get，不创建
void unSubscribe(final String key, final Observer observer) {
    final ValueItem item = itemMap.get(key);
    if (item == null) { return; }
    item.deleteObserver(observer);
}
```

必须取消的三个理由，按重要性排：

1. **`obs` 集合不会自动清理，而 `RaftEvent` 会持续到来。** `Observable` L31 的 `obs = new ConcurrentHashSet<>()` 生命周期等同于 `ValueItem`，而 `ValueItem` 生命周期等同于 `JRaftProtocol`（进程）。`onLeaderStart` / `onStartFollowing` / `onError`（L190/L204/L219）都会 publish RaftEvent → `setData` → `notifyObservers`。集群正常运行期间主从切换、错误上报是常事，**只要还挂着监听就会再跑一遍 `dumpOperate()`**。
2. **后果不是"多读一次数据库"，而是定时任务重复注册。** 第二次进 `dumpOperate` 会再 `scheduleConfigTask` 一遍 `DumpAllProcessorRunner` / `DumpAllGrayProcessorRunner` / `ConfigHistoryClear`，并且再 `clearAll()` 一次磁盘缓存 —— 后者会让所有客户端短暂读到 miss，前者会让 full dump 频率翻倍、进而抢占 raft 读与 FSM 线程。这就是 L111 注释 *"Remove your own listening to avoid task accumulation"* 的确切含义。
3. **语义上它本来就该是一次性的。** 它订阅的不是"leader 地址是多少"，而是"leader 出现了没有"这个**启动事件**。做完即弃，避免成为长期存在的隐式钩子（对比 `canExecute()` L167-175 用的是 `protocol.isLeader(group)` 实时判断 —— 一个用事件、一个用查询，分工清晰）。

顺带两个实现细节：

- `unSubscribe` 与 `notifyObservers` 之间有天然竞态，`Observable` L69-80 的原注释就写明了"recently unregistered Observer will be wrongly notified"。这里无害，因为是 observer 自己在回调里取消自己。
- `deleteObserver`（L56-59）**无条件 `observerCnt--`**，即使该 observer 不在 `obs` 里。重复取消会让 `countObservers()` 变成负数 —— 只影响监控读数，不影响功能。

---

## 六、线程与阶段归属表

| 阶段 | 所在线程 | 证据 |
|---|---|---|
| `init()` / `latchAwait` | Spring 主启动线程 | `@PostConstruct` L82 |
| `subscribe` 引发的首次 `update` | 同上（`addObserver` 内同步回调） | `Observable` L47 |
| `onLeaderStart` → publish RaftEvent | jraft FSM 线程（`JRaft-FSMCaller-Disruptor`） | `NacosStateMachine` L185-192 |
| `metaData.load` → `setData` → `notifyObservers` | NotifyCenter 共享（SlowEvent）线程 | `JRaftProtocol` L120/L146 |
| `dumpOperate()` 及其中的 raft 读 | `com.alibaba.nacos.core.common`（4 线程） | `GlobalExecutor` L37-39、L61-66 |
| 真正写磁盘/内存缓存 | `dump all executor`（`DumpAllProcessor` 临时建的池） | L66-74、L127 |
| SQL 在 Derby 上执行 | 本节点 jraft FSM 线程（`onRequest`）或 leader 节点 | `DistributedDatabaseOperateImpl` L474-516 |

一次启动共 **4 次线程跳转**（Spring → NotifyCenter → GlobalExecutor → dump all executor），这也是为什么"dump 完成"这件事必须靠 `CountDownLatch` 回传，而不能靠返回值。

---

## 七、关键点

| # | 关键点 | 说明 |
|---|---|---|
| 1 | 观察者解决的是**启动时机**问题，不是集群同步问题 | 本地库被 `cleanAndReopenDerby`(L192) 清空，必须等 raft 可用 |
| 2 | 订阅路径就是 `nacos_config/leader` | `MetadataKey.LEADER_META_DATA="leader"`(L26) + `CONFIG_MODEL_RAFT_GROUP="nacos_config"` |
| 3 | **注册即回调**，依赖自研 `Observable.addObserver` L47 | 与 JDK 行为不同；去掉这行会导致"leader 早于订阅出现 → 永久挂死" |
| 4 | 三层结构全 `computeIfAbsent` 惰性建 | 订阅不存在的 group/key 不报错，只会拿到 null |
| 5 | `leader` 只被写非空、永不写空 | `putIfValNoEmpty` L140 + `onLeaderStop` 不发事件 L195-198 |
| 6 | observer 只投递任务，不同步执行 | 否则在 `ValueItem.writeLock` 内跑 IO 会连带堵死 `getData()` |
| 7 | `EXTEND_NEED_READ_UNTIL_HAVE_DATA` = 把读切成 `.join()` 无超时 | `innerRead` L376-381，绕开 `getData` 的 5s |
| 8 | 用 ThreadLocal 而非全局开关 | 只对启动这一条线程生效，业务读仍保留超时保护 |
| 9 | 重试策略是**白名单**，默认不重试 | `shouldRetry` L151-165 |
| 10 | `init` 是启动门禁 | `latchAwait` 无超时 + `throw ex` L147 → 失败即容器起不来（fail-fast） |
| 11 | `unSubscribe` 的真实动机是防定时任务重复注册 | `dumpOperate` L236-253 会 schedule 5 个周期任务，非幂等 |
| 12 | dump 在每个节点都跑，与"谁是 leader"无关 | observer 里没有 `isLeader(self)` 判断；缓存是节点本地的 |
| 13 | `init` 等 leader ≠ `canExecute` 等 leader | 前者要"集群里有 leader"（读），后者要"我是 leader"（写历史清理） |
| 14 | 读降级会进 raft log | `readFromLeader`→`commit`→`applyOperation`→`node.apply`；follower 侧靠 `NacosStateMachine` L110-116 对 ReadRequest 跳过执行 |
| 15 | `metaData` 是 `AbstractConsistencyProtocol` 的 final 字段 | L38，构造即存在，订阅不会 NPE；同时被注入 `Member.extendVal("raftMetaData")` L215 |

---

## 八、坑

1. **`NoLeaderException` 不在重试白名单 → 直接启动失败**
   `invokeToLeader` L424-425 抛 `"The Raft Group [nacos_config] did not find the Leader node"`，既不匹配 `errorMessages` 也不匹配 `retryMessages` → `shouldRetry=false` → `errorReference` 赋值 → `init()` 抛出 → 整个 Nacos 起不来。而这个异常恰恰发生在"leader 刚刚又换了、`RouteTable` 还没刷新"这种**明显可恢复**的场景（`refreshRouteTable` 是周期任务，`JRaftServer` L267-270 周期为 `electionTimeoutMs + rand(5s)`）。`shouldRetry` 的默认分支写成 `return true` 会更稳。

2. **靠异常 message 做子串匹配，非常脆**
   白名单是英文字面量数组。任何一次 jraft 版本升级、`ConsistencyException` 包装文案变化、或异常在 `NacosException` 拼接（`DumpService` L226-228）中被截断/改写，都会让"本该重试"变成"启动失败"。排障时容易误判为"raft 有问题"，实际是分类逻辑失效。

3. **`for(;;)` 无重试上限**
   如果失败一直落在可重试分支（例如 derby 表因快照损坏始终不存在），这个循环会 500ms 一次**永久跑下去**：一方面永久占用 `GlobalExecutor` 共享池 4 个线程之一（该池还被 core 其他任务复用），另一方面 Spring 主线程卡在 `latchAwait` —— 进程既不退出也不 ready，`isReady()`/健康检查也不会把它摘掉，只能从 `config-fatal.log` 里反复刷的 `Nacos Server did not start because dumpservice bean construction failure` 看出来。

4. **`ThreadLocal` 依赖单线程串行执行**
   `putExtendInfo` 与 `dumpOperate` 必须在同一线程才有效。当前代码成立（都在这一个 lambda 里）。但如果有人把 dump 改成异步/线程池派发，`blockRead` 会静默变 false → 退回 5s 超时 → 启动期大量失败，而且现象是"偶发"。这是一处隐式契约。

5. **`unSubscribe` 只在成功路径调用**
   走不可重试分支时（L120-123）observer 仍挂在 `ValueItem.obs` 上，且 `latchAwait` 已 countDown、`init()` 抛异常 → Spring 启动失败。若外层容器没真正退出进程（部分测试/嵌入式场景），后续每次 leader 变更都会再触发一次 dump + 重复注册定时任务。收尾应当放进 `finally`。

6. **`observerCnt` 记账会失真**
   `deleteObserver` L56-59 无条件 `observerCnt--`。`getMetaDataMap()`(L45-50) 与 `Member.extendVal("raftMetaData")` 输出的元数据里看不到这个计数，但任何用它做诊断的地方都会得到负数。

7. **dump 读的是本节点，但读失败会退化成"leader 上执行"**
   `readFromLeader` → `commit` → 非 leader 走 `invokeToLeader` 把 `ReadRequest` 发给 leader，leader `applyOperation` → 进 raft log → `onApply` → `processor.onRequest`。启动期若大量线性读降级，会给 leader 叠加一批"读也走日志"的压力，并占用日志槽位。多节点同时重启时这个放大效应明显（三节点各自 dump × 分页查询）。

8. **standalone 分支跳过了整套保护**
   L85-88 直接 `dumpOperate()`，不设置 `EXTEND_NEED_READ_UNTIL_HAVE_DATA`。单机模式下 raft group 只有 1 个节点，读走 `readIndex` 通常没问题；但如果单机也出现 `onRequest` 慢（例如大库首次预热），就会命中 5 秒超时 → `NacosRuntimeException` → 注意此时异常直接来自 `@PostConstruct`，不经过 `errorReference`，行为与集群模式不同（集群模式会先重试）。

---

## 九、总结

这四步是一条完整的"**事件驱动的启动门禁**"。`EmbeddedDumpService#init` 之所以不像 `ExternalDumpService` 那样直接 dump，是因为 derby+raft 模式下它面临三重竞态：本地库刚被 `DistributedDatabaseOperateImpl.init()` L192 的 `cleanAndReopenDerby()` 清空、raft 可能还没选出 leader、以及 `getData` 默认只有 5 秒超时。于是 L136-137 通过 `CPProtocol#protocolMetaData` 拿到 `AbstractConsistencyProtocol` 里那个 final 的 `ProtocolMetaData`，在 `nacos_config` 组的 `leader` 项上注册观察者；这个项由 `NacosStateMachine.onLeaderStart/onStartFollowing` 发布 `RaftEvent`、经 NotifyCenter 单消费者串行调用 `metaData.load`、最终 `ValueItem.setData` 写入 —— 也就是"**leader 第一次可见**"这个事件本身就是"可以开始 dump"的信号。真正干活的是 `dumpOperate()`：它把 `findConfigMaxId` 和分页 `findAllConfigInfoFragment` 全部转成 raft 线性读落到本地内存+磁盘缓存，再注册 5 个周期任务；为了让这些读在启动期不被 5 秒超时打断，回调在投递到 `GlobalExecutor` 的线程上先 `putExtendInfo(EXTEND_NEED_READ_UNTIL_HAVE_DATA)`，使 `DistributedDatabaseOperateImpl.innerRead` 走 `aGetData().join()` 的无超时分支，并用 `for(;;)+500ms` 配合 `shouldRetry` 的白名单把"暂时读不到"与"状态机真坏了"区分开。最后 L116 的 `unSubscribe` 不是礼节性清理，而是必需的收尾：`Observable.obs` 与 `ValueItem` 同寿命，`onLeaderStart/onStartFollowing/onError` 会不断再来，而 `dumpOperate` 里的 `scheduleConfigTask` 和 `clearAll()` 都不幂等，不摘监听就等于把"全量 dump + 重复注册定时任务 + 清缓存"绑到了每次主从切换上。

设计上最值得学的三点：**用事件替代轮询/猜时机**；**用 ThreadLocal 把"启动期特例"限制在一条线程内**；**用 `@PostConstruct` + 无超时 latch 把缓存预热做成硬性启动门禁（fail-fast 而非降级启动）**。同时它也留了三处硬风险：`addObserver` 会立即回调一次这个**非 JDK 标准行为**（依赖它才不丢事件，也最容易在重构中被删）；`shouldRetry` 的默认分支是"不重试"，导致 `NoLeaderException` 这类明显可恢复的错误直接把进程判死；以及失败分类建立在英文 message 子串匹配上，对版本升级毫无韧性。

排障时按这条链查最省事：先看 raft 日志有没有 `This Raft event changes`（`JRaftProtocol` L129）判断 leader 事件是否到过；再到 `config-fatal.log` 找 `Nacos Server did not start because dumpservice bean construction failure` 的原始 cause 落在哪个分支（含 `temporarily unavailable` 说明在重试，含 `NoLeaderException`/`FSMCaller is overload`/`STATE_ERROR` 说明已判死）；若进程"起不来又没报错退出"，几乎一定卡在 `latchAwait` + `for(;;)` 的组合上，此时看 `com.alibaba.nacos.core.common` 线程的栈即可确认。