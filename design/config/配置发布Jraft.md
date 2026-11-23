
# 发布配置



* 使用的是嵌入是的数据库Apache Derby；
1. 将发布配置写到嵌入式存储上下文中EmbeddedStorageContextHolder；
2. 如果是leader，直接写到底层数据库嵌入db存储Apache Derby；
3. 否则转发请求给leader，leader实际的处理逻辑为不走2；
4. 发布配置变更事件ConfigDataChangeEvent；

**follow通过jraft集群一致性写，保证集群数据一致，重放写请求**

com.alibaba.nacos.config.server.controller.ConfigController.publishConfig   
com.alibaba.nacos.config.server.service.ConfigOperationService.publishConfig
com.alibaba.nacos.config.server.service.repository.embedded.EmbeddedConfigInfoPersistServiceImpl.addConfigInfo(java.lang.String, java.lang.String, com.alibaba.nacos.config.server.model.ConfigInfo, java.util.Map<java.lang.String,java.lang.Object>)
com.alibaba.nacos.config.server.service.repository.embedded.EmbeddedConfigInfoPersistServiceImpl.addConfigInfoAtomic
com.alibaba.nacos.persistence.repository.embedded.EmbeddedStorageContextHolder.addSqlContext(java.lang.String, java.lang.Object...)
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
com.alibaba.nacos.config.server.service.dump.DumpService.handleConfigDataChange

[//]: # (TODO)
com.alibaba.nacos.config.server.service.dump.EmbeddedDumpService.init



# 查询配置
直接从底层磁盘文件加载配置

com.alibaba.nacos.config.server.controller.ConfigController.getConfig
com.alibaba.nacos.config.server.controller.ConfigServletInner.doGetConfig
com.alibaba.nacos.config.server.service.query.ConfigQueryChainService.handle
com.alibaba.nacos.config.server.service.query.handler.FormalHandler.handle
com.alibaba.nacos.config.server.service.dump.disk.ConfigRocksDbDiskService.getContent
com.alibaba.nacos.config.server.service.dump.disk.ConfigRocksDbDiskService.getContentInner


