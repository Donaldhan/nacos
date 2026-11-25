
# 发布配置



* 使用的是嵌入是的数据库Apache Derby；
1. 将发布配置写到嵌入式存储上下文中EmbeddedStorageContextHolder，并在拓展字段记录需要dump事件；
2. 如果是leader，直接写到底层数据库嵌入db存储Apache Derby；
3. 否则转发请求给leader，leader实际的处理逻辑为不走2；
4. 发布配置变更事件ConfigDataChangeEvent；

**leader和follow通过jraft集群一致性写，保证集群数据一致，重放写请求**

com.alibaba.nacos.config.server.controller.ConfigController.publishConfig   
com.alibaba.nacos.config.server.service.ConfigOperationService.publishConfig
com.alibaba.nacos.config.server.service.repository.embedded.EmbeddedConfigInfoPersistServiceImpl.addConfigInfo(java.lang.String, java.lang.String, com.alibaba.nacos.config.server.model.ConfigInfo, java.util.Map<java.lang.String,java.lang.Object>)
com.alibaba.nacos.config.server.service.repository.embedded.EmbeddedConfigInfoPersistServiceImpl.addConfigInfoAtomic
com.alibaba.nacos.persistence.repository.embedded.EmbeddedStorageContextHolder.addSqlContext(java.lang.String, java.lang.Object...)
com.alibaba.nacos.config.server.service.sql.EmbeddedStorageContextUtils.onModifyConfigInfo
com.alibaba.nacos.persistence.repository.embedded.operate.DatabaseOperate.blockUpdate(java.util.function.BiConsumer<java.lang.Boolean,java.lang.Throwable>)

ConfigDataChangeEvent
com.alibaba.nacos.config.server.service.ConfigChangePublisher.notifyConfigChange


## 分布式数据库
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.update
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.write


WriteRequest

### leader模式
1. 直接执行写请求，根据配置变更请求（SQL），写入数据库嵌入db存储Apache Derby
com.alibaba.nacos.core.distributed.raft.JRaftServer.applyOperation
com.alipay.sofa.jraft.core.NodeImpl.apply
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onApply
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.onApply
com.alibaba.nacos.persistence.repository.embedded.operate.BaseDatabaseOperate.update(org.springframework.transaction.support.TransactionTemplate, org.springframework.jdbc.core.JdbcTemplate, java.util.List<com.alibaba.nacos.persistence.repository.embedded.sql.ModifyRequest>)


com.alibaba.nacos.persistence.repository.embedded.operate.BaseDatabaseOperate.doDataImport




com.alibaba.nacos.persistence.utils.PersistenceExecutor.executeEmbeddedDump
com.alibaba.nacos.config.server.service.repository.embedded.EmbeddedConfigDumpApplyHook.afterApply

ConfigDumpEvent




leader执行后



### follow节点
转发写请求给leader；
com.alibaba.nacos.core.distributed.raft.JRaftServer.invokeToLeader


## leader处理写请求
直接执行写请求，根据配置变更请求（SQL），写入数据库嵌入db存储Apache Derby

com.alibaba.nacos.core.distributed.raft.processor.NacosWriteRequestProcessor.handleRequest
com.alibaba.nacos.core.distributed.raft.processor.AbstractProcessor.handleRequest
com.alibaba.nacos.core.distributed.raft.processor.AbstractProcessor.execute
com.alibaba.nacos.core.distributed.raft.JRaftServer.applyOperation


### 单机模式
com.alibaba.nacos.persistence.repository.embedded.operate.StandaloneDatabaseOperateImpl.update(java.util.List<com.alibaba.nacos.persistence.repository.embedded.sql.ModifyRequest>)
com.alibaba.nacos.persistence.repository.embedded.operate.BaseDatabaseOperate.update(org.springframework.transaction.support.TransactionTemplate, org.springframework.jdbc.core.JdbcTemplate, java.util.List<com.alibaba.nacos.persistence.repository.embedded.sql.ModifyRequest>, java.util.function.BiConsumer<java.lang.Boolean,java.lang.Throwable>)


## 配置变更事件处理
**这个是在jraft集群一致性写之后，执行的操作**

ConfigDataChangeEvent
com.alibaba.nacos.config.server.service.dump.DumpService.handleConfigDataChange
com.alibaba.nacos.config.server.service.dump.EmbeddedDumpService.init


## dump事件的处理
ConfigDumpEvent

com.alibaba.nacos.persistence.utils.PersistenceExecutor.executeEmbeddedDump
com.alibaba.nacos.config.server.service.repository.embedded.EmbeddedConfigDumpApplyHook.afterApply
com.alibaba.nacos.config.server.service.dump.EmbeddedDumpService.EmbeddedDumpService

这个dump的处理与外部存储的dump是一致的，包含dump所有配置及灰度配置，
配置历史数据清理ExternalDumpService第一个服务ip清理，**EmbeddedDumpService单例或集群leader可清理**

具体dump的时机参见

[derby+raft模式下dump的时机](derby+raft模式下dump的时机.md) 

com.alibaba.nacos.config.server.service.dump.DumpService.DumpService
com.alibaba.nacos.config.server.service.dump.processor.DumpProcessor.process

com.alibaba.nacos.config.server.service.dump.DumpService.dumpOperate

com.alibaba.nacos.config.server.service.repository.embedded.EmbeddedConfigInfoPersistServiceImpl.findConfigInfo(java.lang.String, java.lang.String, java.lang.String)
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.queryOne(java.lang.String, java.lang.Class<R>)
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.innerRead
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.getData
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.aGetData
com.alibaba.nacos.core.distributed.raft.JRaftServer.get
com.alibaba.nacos.core.distributed.raft.JRaftServer.readFromLeader

**dump的配置是从leader直接查询的**

### 全量dump

**dump的配置是从leader直接查询的**

com.alibaba.nacos.config.server.service.dump.processor.DumpProcessor.process
com.alibaba.nacos.config.server.service.dump.processor.DumpAllProcessor.process

com.alibaba.nacos.config.server.service.dump.DumpService.dumpOperate
com.alibaba.nacos.config.server.service.dump.DumpChangeConfigWorker.run



com.alibaba.nacos.config.server.service.repository.embedded.EmbeddedConfigInfoPersistServiceImpl.findConfigInfo(java.lang.String, java.lang.String, java.lang.String)
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.queryOne(java.lang.String, java.lang.Class<R>)
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.innerRead
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.getData
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.aGetData
com.alibaba.nacos.core.distributed.raft.JRaftServer.get
com.alibaba.nacos.core.distributed.raft.JRaftServer.readFromLeader



### 订阅leader事件leader
[derby+raft模式下dump的时机](derby+raft模式下dump的时机.md)

1. dump监听leader产生，leader产生式，leader节点和follow节点会发布RaftEvent；
2. JRaftProtocol处理RaftEvent，写协议元数据；
3. 观察监听写协议元数据，则执行观察业务逻辑**update**

com.alibaba.nacos.config.server.service.dump.EmbeddedDumpService.init
com.alibaba.nacos.consistency.ProtocolMetaData.subscribe
com.alibaba.nacos.consistency.ProtocolMetaData.MetaData.subscribe

leader节点和follow节点会发布RaftEvent
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onLeaderStart
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onStartFollowing

com.alibaba.nacos.core.distributed.raft.JRaftProtocol.init
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.injectProtocolMetaData

com.alibaba.nacos.consistency.ProtocolMetaData.MetaData.put
com.alibaba.nacos.consistency.ProtocolMetaData.ValueItem.setData
com.alibaba.nacos.common.utils.Observable.notifyObservers
com.alibaba.nacos.common.utils.Observer.update


### 定时清除历史数据


[配置发布follower如何leader保持一致](配置发布follower如何leader保持一致.md)

配置历史数据清理ExternalDumpService第一个服务ip清理，**EmbeddedDumpService单例或集群leader可清理**
清理是全局唯一动作，不该重复；dump 是本地动作，必须重复。



leader发起清除历史配置写请求，raft集群其他节点执行清除SQL，完成一致性的历史配置清除操作；

com.alibaba.nacos.config.server.service.dump.DumpService.ConfigHistoryClear.run
com.alibaba.nacos.config.server.service.dump.DefaultHistoryConfigCleaner.cleanHistoryConfig
com.alibaba.nacos.config.server.service.repository.embedded.EmbeddedHistoryConfigInfoPersistServiceImpl.removeConfigHistory
com.alibaba.nacos.persistence.repository.embedded.EmbeddedPaginationHelperImpl.updateLimit
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.update


# 快照备份与恢复

初始化快照操作器

com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.init
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.addRequestProcessors
com.alibaba.nacos.core.distributed.raft.JRaftServer.createMultiRaftGroup
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.NacosStateMachine
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.adapterToJRaftSnapshot
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.loadSnapshotOperate
com.alibaba.nacos.core.persistence.DerbySnapshotOperation.DerbySnapshotOperation




# 定时快照数据
定时生成配置快照数据, 使用的是Derby引擎级备份机制

本节点 snapshotTimer 定时 1800s，首次随机化到 900~1350s

com.alipay.sofa.jraft.core.NodeImpl.init
com.alipay.sofa.jraft.core.NodeImpl.handleSnapshotTimeout
com.alipay.sofa.jraft.core.NodeImpl.doSnapshot
com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl.doSnapshot(com.alipay.sofa.jraft.Closure, boolean)
com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl.doSnapshot(com.alipay.sofa.jraft.Closure, boolean)
com.alipay.sofa.jraft.core.FSMCallerImpl.onSnapshotSaveSync(com.alipay.sofa.jraft.closure.SaveSnapshotClosure)
com.alipay.sofa.jraft.core.FSMCallerImpl.onSnapshotSaveSync(com.alipay.sofa.jraft.core.FSMCallerImpl.ApplyTask)
com.alipay.sofa.jraft.core.FSMCallerImpl.doSnapshotSave
com.alipay.sofa.jraft.core.StateMachineAdapter.onSnapshotSave


com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onSnapshotSave
com.alibaba.nacos.core.persistence.DerbySnapshotOperation.onSnapshotSave
com.alibaba.nacos.core.persistence.DerbySnapshotOperation.doDerbyBackup

[Derby定时快照配置数据](Derby定时快照配置数据.md)

# 启动是从快照加载

[Derby快照恢复数据](Derby快照恢复数据.md)

启动时，则从加载快照，实际是将快照数据恢复到Derby数据库（**将备份文件拷贝到Derby数据目录**）； 快照加 raftlog 恢复，保证数据一致

[配置发布follower如何leader保持一致](配置发布follower如何leader保持一致.md)

onSnapshotLoad 节点启动加载本地最近快照
SnapshotExecutorImpl.init() 里 snapshotStorage.open() 拿到快照
com.alipay.sofa.jraft.core.NodeImpl.init
com.alipay.sofa.jraft.core.NodeImpl.initSnapshotStorage
com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl.init
com.alipay.sofa.jraft.core.FSMCallerImpl.onSnapshotLoad
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onSnapshotLoad
com.alibaba.nacos.core.persistence.DerbySnapshotOperation.onSnapshotLoad

com.alibaba.nacos.core.persistence.DerbySnapshotOperation.doDerbyRestoreFromBackup

com.alibaba.nacos.persistence.datasource.LocalDataSourceServiceImpl.restoreDerby
com.alibaba.nacos.persistence.datasource.LocalDataSourceServiceImpl.doDerbyClean
com.alibaba.nacos.persistence.datasource.LocalDataSourceServiceImpl.initialize

DerbyLoadEvent:没有相关的处理器，处理这个事件


## raft日志重放

[Raft日志重放](Raft日志重放.md)

[Raft日志重放-log&commit索引来源](Raft日志重放-log&commit索引来源.md)

快照加载完，会抬高水位；


com.alipay.sofa.jraft.core.NodeImpl.init
```java
// NodeImpl.init()  L1012 → L1026
if (!initFSMCaller(new LogId(0, 0))) { ... }     // ① lastAppliedIndex := 0
...
if (!initSnapshotStorage()) { ... }              // ② 内部会加载本地快照
final Status st = this.logManager.checkConsistency();   // ③
```
com.alipay.sofa.jraft.core.NodeImpl.initFSMCaller



重放起点被抬高到这里
com.alipay.sofa.jraft.core.FSMCallerImpl.doSnapshotLoad

```java
/ FSMCallerImpl.doSnapshotLoad()  L710-L730
if (!this.fsm.onSnapshotLoad(reader)) {                    // → NacosStateMachine → DerbySnapshotOperation
    done.run(new Status(-1, "StateMachine onSnapshotLoad failed"));
    setError(new RaftException(ERROR_TYPE_STATE_MACHINE, ESTATEMACHINE, "StateMachine onSnapshotLoad failed"));
    return;                                                 // ★失败即让节点进 ERROR
}
...
this.fsm.onConfigurationCommitted(conf);                   // L725 顺带恢复集群配置
this.lastCommittedIndex.set(meta.getLastIncludedIndex());   // L727
this.lastAppliedIndex.set(meta.getLastIncludedIndex());     // L728 ★重放起点被抬高到这里
this.lastAppliedTerm = meta.getLastIncludedTerm();          // L729
done.run(Status.OK());                                      // L730 → onSnapshotLoadDone
```

com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta.SnapshotMeta()

对齐集群commitIndex 
com.alipay.sofa.jraft.core.NodeImpl.LeaderStableClosure.run
com.alipay.sofa.jraft.core.BallotBox.commitAt

重发日志

com.alipay.sofa.jraft.core.FSMCallerImpl.doCommitted
com.alipay.sofa.jraft.core.IteratorImpl.next

先从内存加载日志，如果没有则从磁盘加载
com.alipay.sofa.jraft.storage.impl.LogManagerImpl.getEntry
com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage.getEntry


#### Raft日志重放-log&commit索引来源
[Raft日志重放-log&commit索引来源](Raft日志重放-log&commit索引来源.md)

FSMCallerImpl
写 lastAppliedIndex到快照文件
com.alipay.sofa.jraft.core.FSMCallerImpl.doSnapshotSave
com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl.onSnapshotSaveDone


抬高水位点

Replicator.installSnapshot
com.alipay.sofa.jraft.core.Replicator.installSnapshot


**集群节点落后的话**
[Raft集群commit索引落后](Raft集群commit索引落后.md)  

[Raft选举及落后节点复制日志](Raft选举及落后节点复制日志.md)

[Raft落后节点追日志](Raft落后节点追日志.md)


ReplicatorGroupImpl.checkReplicator  
com.alipay.sofa.jraft.core.Replicator.start
com.alipay.sofa.jraft.core.Replicator.onRpcReturned

处理日志请求
com.alipay.sofa.jraft.core.NodeImpl.handleAppendEntriesRequest

### 关键点

1. **`lastAppliedIndex` 不持久化**（`NodeImpl.init` L1012 传 `new LogId(0,0)`）——这是"leader 和 follower 都必须重放"的根本原因，也是 jraft 不需要维护"apply 位点"这种额外持久化状态的代价。

2. **快照加载与日志重放在同一条线程上串行**（`FSMCallerImpl.runApplyTask` 的 `TaskType.SNAPSHOT_LOAD` 与 `TaskType.COMMITTED` 共用一个 disruptor 单 handler）。所以"装完快照才开始重放"是**天然有序**的，不需要额外同步——这是 `DerbySnapshotOperation` 敢做整库替换的安全前提。

3. **重放没有独立代码路径**：`doCommitted` 对"实时新提交"和"重启重放"完全一致，唯一区别是入参 `lastAppliedIndex` 离 `committedIndex` 有多远。理解这一点，就不会去找"重放在哪个类里"这种不存在的答案。

4. **Nacos 的清库与 jraft 恢复顺序严格咬合**：`DistributedDatabaseOperateImpl.init()` L192 `cleanAndReopenDerby()` → L214 `addRequestProcessors` → `JRaftServer.createMultiRaftGroup` → `NodeImpl.init` → 本地快照恢复 → 重放。先清空、后覆盖、再补日志，所以"不信任任何本地残留"是成立的。

5. **`writeLock` 挡的是读请求，不是 apply**：`DerbySnapshotOperation.onSnapshotLoad` L123 持写锁，而 `onApply`（L523）、`onRequest`（L474）持读锁。因为 apply 与 snapshot load 在同一条 FSM 线程串行，写锁不是用来防自己的，是用来防止**客户端读请求在整库替换的中间态上拿到半截数据**。

6. **重放会顺带重建缓存，快照恢复不会**：`onApply` 的非导入分支 L537 `PersistenceExecutor.executeEmbeddedDump(...)` 会跑 `EmbeddedApplyHook`，`EmbeddedConfigDumpApplyHook.afterApply` 从 `WriteRequest.extendInfo` 还原 `ConfigDumpEvent` 并发布 → `DumpConfigHandler` dump 到磁盘/内存缓存。日志重放时 extendInfo 是随日志条目一起来的，所以缓存能重建。而 `onSnapshotLoad` 只发 `DerbyLoadEvent`（L148，无订阅者）——**整库替换后没有任何缓存重建动作**。这解释了两类不一致窗口的来源不同。

7. **follower 不需要写入流量也能完成重放**：心跳分支 L1987 就会推进 commit。所以 3 节点集群里单独重启一个 follower，几秒内数据即追平（前提是快照/日志够用）。

8. **`setSnapshot` 故意保留上次快照点之后的日志**（LogManagerImpl L637-L647，注释原文）——保证 follower 还能按日志追赶，避免"leader 刚存完快照就把自己和 follower 都逼到只能传快照"。

---


#  derby+raft 集群下，启动是清楚derby数据

com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.init
com.alibaba.nacos.persistence.datasource.LocalDataSourceServiceImpl.cleanAndReopenDerby
com.alibaba.nacos.persistence.datasource.LocalDataSourceServiceImpl.doDerbyClean


# 数据导入
解析配置文件，raft一致性写
com.alibaba.nacos.config.server.controller.ConfigOpsController.importDerby
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.dataImport
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.writeAsync
com.alibaba.nacos.core.distributed.raft.JRaftServer.commit

com.alibaba.nacos.common.notify.NotifyCenter.publishEvent(com.alibaba.nacos.common.notify.Event)

DerbyImportEvent
目前没有发布DerbyImportEvent事件的相关处理逻辑



# 查询配置
直接从底层磁盘文件加载配置

com.alibaba.nacos.config.server.controller.ConfigController.getConfig
com.alibaba.nacos.config.server.controller.ConfigServletInner.doGetConfig
com.alibaba.nacos.config.server.service.query.ConfigQueryChainService.handle
com.alibaba.nacos.config.server.service.query.handler.FormalHandler.handle
com.alibaba.nacos.config.server.service.dump.disk.ConfigRocksDbDiskService.getContent
com.alibaba.nacos.config.server.service.dump.disk.ConfigRocksDbDiskService.getContentInner


