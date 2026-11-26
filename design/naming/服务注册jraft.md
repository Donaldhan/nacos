状态机定时快照及加载快照的时机参加

[Jraft状态机的快照保存与加载时机](/design/core/Jraft状态机的快照保存与加载时机.md) 

具体链路参见

[配置发布Jraft](/design/config/配置发布Jraft.md)


**集群模式下，元数据处理逻辑，持久化只有服务，没有服务对应的元数据和服务实例元数据，通过InstanceMetadataProcessor和ServiceMetadataProcessor处理，在状态机定时持久化快照和启动时进行加载，与配置模式一致**

1. 定时快照数据

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


2. 启动是从快照加载

onSnapshotLoad 节点启动加载本地最近快照
SnapshotExecutorImpl.init() 里 snapshotStorage.open() 拿到快照
com.alipay.sofa.jraft.core.NodeImpl.init
com.alipay.sofa.jraft.core.NodeImpl.initSnapshotStorage
com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl.init
com.alipay.sofa.jraft.core.FSMCallerImpl.onSnapshotLoad
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onSnapshotLoad
com.alibaba.nacos.core.persistence.DerbySnapshotOperation.onSnapshotLoad


# 服务注册发现
1. 服务注册注销时，通过jraft机制，大多数同意写时，集群节点维护服务及服务元数据；
2. 并定时快照，在集群节点重启时加载快照到客户端管理器PersistentIpPortClientManager及元数据管理器NamingMetadataManager

## 服务端实例注册注销
1. 服务注册注销时，添加或从客户端管理器Manager PersistentIpPortClientManager中移除服务实例；
2. 定时保存快照到磁盘，并在启动时加载快照到客户端管理器PersistentIpPortClientManager


## 服务实例注册注销处理
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.onApply

### 服务端实例快照保存
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
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.loadSnapshotOperate
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.PersistentClientOperationServiceImpl
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.loadSnapshotOperate
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.PersistentInstanceSnapshotOperation.writeSnapshot
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.PersistentInstanceSnapshotOperation.readSnapshot

### 服务端实例快照加载
1. 更新服务实例，包含元数据和实例数据，不存在的，则添加，非活跃则移除；
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onSnapshotLoad
com.alibaba.nacos.naming.consistency.persistent.impl.AbstractSnapshotOperation.onSnapshotLoad
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.PersistentInstanceSnapshotOperation.readSnapshot


## 服务实例元数据维护
1. 服务实例注册注销时，添加或从元数据管理器NamingMetadataManager中移除服务实例元数据；
2. 定时保存快照到磁盘，并在启动时加载快照到元数据管理器NamingMetadataManager


### 服务注册注销元数据处理
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.onApply



### 服务实例元信息快照保存
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.InstanceMetadataProcessor
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.loadSnapshotOperate
com.alibaba.nacos.naming.core.v2.metadata.AbstractMetadataSnapshotOperation.writeSnapshot
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataSnapshotOperation.dumpSnapshot


### 服务实例元信息快照加载
com.alibaba.nacos.naming.core.v2.metadata.AbstractMetadataSnapshotOperation.readSnapshot
com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager.loadInstanceMetadataSnapshot


## 服务元数据维护


1. 服务注册注销时，添加或从元数据管理器NamingMetadataManager中移除服务元数据；
2. 定时保存快照到磁盘，并在启动时加载快照到元数据管理器NamingMetadataManager

### 服务注册注销元数据处理

com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataProcessor.onApply


### 服务元数据定时快照


com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataProcessor.ServiceMetadataProcessor
com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataProcessor.loadSnapshotOperate
com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataSnapshotOperation.dumpSnapshot


### 服务元数据快照加载
com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataSnapshotOperation.loadSnapshot


