
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

[//]: # (TODO)


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


