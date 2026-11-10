

# 服务端路径
灰度的是在gray-data目录；
```sh
eptrusttry@EPdeMacBook-Pro COMMON_GROUP % ls
ROUTER_CONFIG
eptrusttry@EPdeMacBook-Pro COMMON_GROUP % cat ROUTER_CONFIG 
server:
  port: 8080
zone:
  partitions:12
  mode:"hash"%                                                                                                                                                                          eptrusttry@EPdeMacBook-Pro COMMON_GROUP % pwd
/System/Volumes/Data/resource/github/nacos/distribution/target/nacos-server-2.5.1/nacos/data/config-data/COMMON_GROUP
eptrusttry@EPdeMacBook-Pro COMMON_GROUP % 
```


# 客户端路径
每次服务重启的时候，会从本地加载配置到缓存；同时从服务端拉取配置，如果存在变更，则覆盖；
```sh
eptrusttry@EPdeMacBook-Pro config % cd Config-fixed-public-127.0.0.1_8848_nacos
eptrusttry@EPdeMacBook-Pro Config-fixed-public-127.0.0.1_8848_nacos % ls
encrypted-data-key	snapshot-tenant
eptrusttry@EPdeMacBook-Pro Config-fixed-public-127.0.0.1_8848_nacos % pwd
/Users/eptrusttry/nacos/config/Config-fixed-public-127.0.0.1_8848_nacos
eptrusttry@EPdeMacBook-Pro Config-fixed-public-127.0.0.1_8848_nacos % cd snapshot-tenant
eptrusttry@EPdeMacBook-Pro snapshot-tenant % ls
public
eptrusttry@EPdeMacBook-Pro snapshot-tenant % cd public 
eptrusttry@EPdeMacBook-Pro public % ls
COMMON_GROUP	DEFAULT_GROUP
eptrusttry@EPdeMacBook-Pro public % cd COMMON_GROUP 
eptrusttry@EPdeMacBook-Pro COMMON_GROUP % ls
ROUTER_CONFIG
eptrusttry@EPdeMacBook-Pro COMMON_GROUP % cat ROUTER_CONFIG 
server:
  port: 8080
zone:
  partitions:12
  mode:"hash"%                                                                  eptrusttry@EPdeMacBook-Pro COMMON_GROUP % pws
zsh: command not found: pws
eptrusttry@EPdeMacBook-Pro COMMON_GROUP % pwd
/Users/eptrusttry/nacos/config/Config-fixed-public-127.0.0.1_8848_nacos/snapshot-tenant/public/COMMON_GROUP
eptrusttry@EPdeMacBook-Pro COMMON_GROUP % 

```