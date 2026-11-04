我来读取这个文件并为你解析它的作用。

## 📦 ControllerMethodsCache 类解析

这是 Nacos 中的一个**控制器方法缓存组件**，用于管理和匹配 HTTP 请求到对应的 Controller 方法。它在 **Nacos 权限认证和请求路由** 中起到关键作用。

---

## 🎯 核心作用

### 1. **请求方法映射缓存**
- 缓存所有 Controller 中带有 `@RequestMapping` 及其派生注解（`@GetMapping`、`@PostMapping` 等）的方法
- 建立 URL 路径和 HTTP 方法到 Java 方法的映射关系

### 2. **请求匹配**
- 根据 `HttpServletRequest` 找到对应的 Controller 方法
- 支持路径匹配、HTTP 方法匹配、参数条件匹配
- 处理多个匹配时的优先级排序和歧义检测

### 3. **权限控制支持**
- 为 Nacos 的认证授权提供方法级别的路由信息
- 判断请求是否需要认证

---

## 📊 核心数据结构

```java
// 存储 RequestMappingInfo -> Method 的映射
private ConcurrentMap<RequestMappingInfo, Method> methods;

// 存储 URL -> List<RequestMappingInfo> 的查找表
private final ConcurrentMap<String, List<RequestMappingInfo>> urlLookup;

// 已扫描的类集合（避免重复扫描）
private final Set<Class> scannedClass;
```

---

## 🔧 主要方法解析

### 1. **`getMethod(HttpServletRequest request)`** - 获取请求对应的方法

**流程**：
```
1. 从请求中提取路径和 HTTP 方法
2. 构建 URL Key（格式：HTTP_METHOD/path）
3. 从 urlLookup 中查找候选的 RequestMappingInfo 列表
4. 通过参数条件过滤出匹配项
5. 如果有多个匹配项，使用 Comparator 排序
6. 检测歧义映射（抛出异常）
7. 返回最佳匹配的方法
```

**关键点**：
- 支持上下文路径（context path）处理
- 处理 URL 末尾斜杠的兼容（Issue #4701）
- 歧义检测：如果两个方法优先级相同，抛出 `IllegalStateException`

---

### 2. **`initClassMethod(String packageName)`** - 初始化包下所有 Controller 方法

**流程**：
```
1. 使用 DefaultPackageScan 扫描包
2. 查找所有带 @RequestMapping 注解的类
3. 对每个类调用 initClassMethod(Class)
```

---

### 3. **`initClassMethod(Class<?> clazz)`** - 初始化单个类的方法

**核心逻辑**：
```java
// 1. 获取类级别的 @RequestMapping 路径
RequestMapping requestMapping = clazz.getAnnotation(RequestMapping.class);

// 2. 遍历类的所有方法
for (Method method : clazz.getMethods()) {
    // 3. 如果没有 @RequestMapping，解析子注解（@GetMapping 等）
    if (!method.isAnnotationPresent(RequestMapping.class)) {
        parseSubAnnotations(method, classPath);
        continue;
    }
    
    // 4. 解析方法级别的 @RequestMapping
    // 5. 构建 URL Key 并存储映射关系
}
```

**支持的注解**：
- `@RequestMapping`（通用）
- `@GetMapping`（GET 请求）
- `@PostMapping`（POST 请求）
- `@PutMapping`（PUT 请求）
- `@DeleteMapping`（DELETE 请求）
- `@PatchMapping`（PATCH 请求）

---

### 4. **`addUrlAndMethodRelation`** - 添加 URL 和方法的关联

```java
private void addUrlAndMethodRelation(String urlKey, String[] requestParam, Method method) {
    // 1. 创建 RequestMappingInfo 对象
    RequestMappingInfo requestMappingInfo = new RequestMappingInfo();
    requestMappingInfo.setPathRequestCondition(new PathRequestCondition(urlKey));
    requestMappingInfo.setParamRequestCondition(new ParamRequestCondition(requestParam));
    
    // 2. 添加到 URL 查找表
    List<RequestMappingInfo> requestMappingInfos = urlLookup.get(urlKey);
    if (requestMappingInfos == null) {
        urlLookup.putIfAbsent(urlKey, new ArrayList<>());
        requestMappingInfos = urlLookup.get(urlKey);
        // 兼容 URL 末尾有斜杠的情况
        String urlKeyBackup = urlKey + "/";
        urlLookup.putIfAbsent(urlKeyBackup, requestMappingInfos);
    }
    
    // 3. 存储映射关系
    requestMappingInfos.add(requestMappingInfo);
    methods.put(requestMappingInfo, method);
}
```

---

## 🔍 使用场景

### 场景 1：权限认证
当请求到达 Nacos 时，权限拦截器需要：
1. 获取请求的 URL 和 HTTP 方法
2. 通过 `ControllerMethodsCache` 找到对应的 Controller 方法
3. 检查该方法是否需要认证
4. 执行相应的认证逻辑

### 场景 2：请求路由
帮助 Nacos 确定请求应该由哪个 Controller 的哪个方法处理。

---

## ⚙️ 工作流程图

```
HTTP Request
    ↓
1. 提取 path + method
    ↓
2. 构建 URL Key (如: GET/v1/ns/service/list)
    ↓
3. 从 urlLookup 查找候选 RequestMappingInfo 列表
    ↓
4. 参数条件匹配 (ParamRequestCondition)
    ↓
5. 排序并选择最佳匹配
    ↓
6. 从 methods Map 获取对应的 Method
    ↓
7. 反射调用 Controller 方法
```

---

## 🎨 设计亮点

1. **并发安全**：使用 `ConcurrentHashMap` 保证线程安全
2. **避免重复扫描**：通过 `scannedClass` 集合防止重复处理
3. **灵活的注解支持**：同时支持 `@RequestMapping` 和各种派生注解
4. **歧义检测**：发现多个方法匹配同一 URL 时主动报错
5. **斜杠兼容**：自动处理 URL 末尾有无斜杠的情况（Issue #4701）

---

## 📝 总结

`ControllerMethodsCache` 是 Nacos 请求处理链路中的**核心路由组件**，它：
- 在启动时扫描并缓存所有 Controller 的方法映射
- 在运行时根据请求快速定位对应的处理方法
- 为权限控制、参数校验等提供基础支持

这是一个典型的**空间换时间**的设计模式，通过预缓存提升请求处理效率。