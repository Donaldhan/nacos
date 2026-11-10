
# GRPC连接建立处理器

处理客户端的连接，添加连接到连接管理器
com.alibaba.nacos.core.remote.grpc.GrpcBiStreamRequestAcceptor.requestBiStream
com.alibaba.nacos.core.remote.ConnectionManager.register
连接属性connetionId， port，address配置
com.alibaba.nacos.core.remote.grpc.AddressTransportFilter.transportReady




# GRPC请求处理器（包含配置、健康检查、订阅服务、服务查询、会员报告请求）
com.alibaba.nacos.core.remote.grpc.GrpcRequestAcceptor.request
com.alibaba.nacos.core.remote.RequestHandlerRegistry.getByRequestType
com.alibaba.nacos.core.remote.RequestHandlerRegistry.onApplicationEvent
包含各类GPRC处理器


qcode 总结RequestHandler的类型及作用

## 批量请求配置变更处理器

处理客户端配置的监听和注销，及配置变更确认（缓存层的MD5对比），如果配置是灰度控制，则进行灰度匹配
com.alibaba.nacos.config.server.remote.ConfigChangeBatchListenRequestHandler
com.alibaba.nacos.config.server.service.ConfigCacheService.isUptodate(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Map<java.lang.String,java.lang.String>)
com.alibaba.nacos.config.server.service.ConfigCacheService.getContentMd5(java.lang.String, java.lang.String, java.lang.String, java.util.Map<java.lang.String,java.lang.String>)
com.alibaba.nacos.config.server.model.ConfigCacheGray.match
com.alibaba.nacos.config.server.model.gray.BetaGrayRule.match
com.alibaba.nacos.config.server.model.gray.TagGrayRule.match

## 配置查询请求处理器
1. 从缓存读取缓存CacheItem；
2. 配置类型处理器；
3. 处理灰度标志；并从磁盘加载配置文件
4. tag标志处理
5. 正式发布处理
com.alibaba.nacos.config.server.remote.ConfigQueryRequestHandler
com.alibaba.nacos.config.server.service.query.ConfigQueryChainService.handle
com.alibaba.nacos.config.server.service.query.DefaultConfigQueryHandlerChainBuilder.build



### ConfigChainEntryHandler
从缓存读取缓存CacheItem
com.alibaba.nacos.config.server.service.query.handler.ConfigChainEntryHandler.handle
com.alibaba.nacos.config.server.service.ConfigCacheService.getContentCache


```java
public enum ConfigQueryStatus {
        /**
         * Indicates that the configuration was found and is formal.
         */
        CONFIG_FOUND_FORMAL,
        
        /**
         * Indicates that the configuration was found and is gray.
         */
        CONFIG_FOUND_GRAY,
        
        /**
         * Indicates that the configuration special tag was not found.
         */
        SPECIAL_TAG_CONFIG_NOT_FOUND,
        
        /**
         * Indicates that the configuration was not found.
         */
        CONFIG_NOT_FOUND,
        
        /**
         * Indicates a conflict in the configuration query.
         */
        CONFIG_QUERY_CONFLICT,
    }
```

### ConfigContentTypeHandler
配置类型处理器
com.alibaba.nacos.config.server.service.query.handler.ConfigContentTypeHandler.handle

### GrayRuleMatchHandler
处理灰度标志
com.alibaba.nacos.config.server.service.query.handler.GrayRuleMatchHandler.handle
com.alibaba.nacos.config.server.service.dump.disk.ConfigDiskService.getGrayContent

### SpecialTagNotFoundHandler
tag标志处理
com.alibaba.nacos.config.server.service.query.handler.SpecialTagNotFoundHandler.handle

### FormalHandler
com.alibaba.nacos.config.server.service.query.handler.FormalHandler.handle
com.alibaba.nacos.config.server.service.dump.disk.ConfigDiskService.getContent
com.alibaba.nacos.config.server.service.dump.disk.ConfigRocksDbDiskService.getContent

## 配置发布请求处理器

与控制台发配配置的逻辑一致
com.alibaba.nacos.config.server.remote.ConfigPublishRequestHandler

com.alibaba.nacos.config.server.service.ConfigOperationService.publishConfig


## 移除配置请求处理器
从底层存储删除相关配置

com.alibaba.nacos.config.server.remote.ConfigRemoveRequestHandler
com.alibaba.nacos.config.server.service.ConfigOperationService.deleteConfig

## 集群配置变更同步请求处理器
dump变更的配置， 客户端查询时，先从本地缓存查询
com.alibaba.nacos.config.server.remote.ConfigChangeClusterSyncRequestHandler


## 健康检查请求处理器
健康检查
com.alibaba.nacos.core.remote.HealthCheckRequestHandler




## 订阅服务请求处理器
com.alibaba.nacos.naming.remote.rpc.handler.SubscribeServiceRequestHandler

## 服务查询请求处理器

com.alibaba.nacos.naming.remote.rpc.handler.ServiceQueryRequestHandler

## 会员报告处理器
com.alibaba.nacos.core.cluster.remote.MemberReportHandler

