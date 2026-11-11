# 启动grpc服务

com.alibaba.nacos.core.remote.grpc.GrpcSdkServer
com.alibaba.nacos.core.remote.grpc.BaseGrpcServer.startServer
com.alibaba.nacos.core.remote.BaseRpcServer.start


com.alibaba.nacos.core.remote.grpc.GrpcConnectionInterceptor.interceptCall



com.alibaba.nacos.core.remote.grpc.AddressTransportFilter.transportReady
com.alibaba.nacos.core.remote.grpc.AddressTransportFilter.transportTerminated


com.alibaba.nacos.core.remote.grpc.BaseGrpcServer.addServices

## 处理连接请求
com.alibaba.nacos.core.remote.grpc.GrpcBiStreamRequestAcceptor
[处理客户端的grpc连接请求](./处理客户端的grpc连接请求.md)

## 处理其他配置变更等请求
com.alibaba.nacos.core.remote.grpc.GrpcRequestAcceptor

[处理客户端的grpc连接请求](./处理客户端的grpc连接请求.md)