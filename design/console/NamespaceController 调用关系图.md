我来帮你查找 `NamespaceController` 在哪里被调用。

让我搜索这个控制器的 API 路径在哪些地方被调用：

很好！让我查看几个具体的调用组件：

完美！现在我可以给你完整的调用关系了。让我整理一下：

## 📍 NamespaceController 调用关系图

`NamespaceController` 是 Nacos 控制台的 **V1 版本命名空间管理 API**，主要被前端 UI 组件调用。

---

## 🔄 调用链路

### **后端 Controller 层**
```
NamespaceController (V1版本)
    ↓
NamespaceOperationService (业务逻辑层)
    ↓
NamespacePersistService (数据持久化层)
```

### **前端调用关系**
```
console-ui/src/
├── reducers/namespace.js (Redux action)
│   └── getNamespaces() → GET /v1/console/namespaces
│
├── components/NewNameSpace/NewNameSpace.js (新建命名空间对话框)
│   ├── checkNamespaceIdExist() → GET /v1/console/namespaces?checkNamespaceIdExist=true
│   ├── createNamespace() → POST /v1/console/namespaces
│   └── refreshNameSpace() → GET /v1/console/namespaces
│
├── components/EditorNameSpace/EditorNameSpace.js (编辑命名空间对话框)
│   ├── getNamespaceDetail() → GET /v1/console/namespaces?show=all&namespaceId=xxx
│   ├── editNamespace() → PUT /v1/console/namespaces
│   └── refreshNameSpace() → GET /v1/console/namespaces
│
├── components/NameSpaceList/NameSpaceList.js (命名空间列表)
│   └── deleteNamespace() → DELETE /v1/console/namespaces
│
└── pages/AuthorityControl/PermissionsManagement/
    ├── NewPermissions.js (新建权限)
    │   └── getNamespaces() → 通过 Redux 调用 GET /v1/console/namespaces
    └── PermissionsManagement.js (权限管理页面)
        └── getNamespaces() → 通过 Redux 调用 GET /v1/console/namespaces
```

---

## 📝 具体调用示例

### 1️⃣ **获取命名空间列表**

**前端调用位置**：`console-ui/src/reducers/namespace.js` (第24-31行)

```javascript
const getNamespaces = params => dispatch =>
  request.get('v1/console/namespaces', { params }).then(response => {
    const { code, data } = response;
    dispatch({
      type: GET_NAMESPACES,
      data: code === 200 ? data : [],
    });
  });
```

**对应后端接口**：
```java
// NamespaceController.java (第71-74行)
@GetMapping
public RestResult<List<Namespace>> getNamespaces() {
    return RestResultUtils.success(namespaceOperationService.getNamespaceList());
}
```

---

### 2️⃣ **创建新命名空间**

**前端调用位置**：`console-ui/src/components/NewNameSpace/NewNameSpace.js` (第131-158行)

```javascript
// 步骤1：检查命名空间ID是否存在
request({
  type: 'get',
  url: 'v1/console/namespaces?checkNamespaceIdExist=true',  // ← 调用后端
  data: { customNamespaceId },
  success: res => {
    if (res === true) {
      Dialog.alert({ content: '命名空间ID已存在' });
    } else {
      // 步骤2：创建命名空间
      request({
        type: 'post',
        url: 'v1/console/namespaces',  // ← 调用后端
        data: {
          customNamespaceId,
          namespaceName: values.namespaceShowName,
          namespaceDesc: values.namespaceDesc,
        },
        success: res => {
          if (res === true) {
            this.props.getNameSpaces();  // 刷新列表
          }
        }
      });
    }
  }
});
```

**对应后端接口**：
```java
// NamespaceController.java

// 检查ID是否存在 (第131-137行)
@GetMapping(params = "checkNamespaceIdExist=true")
public Boolean checkNamespaceIdExist(@RequestParam("customNamespaceId") String namespaceId) {
    return (namespacePersistService.tenantInfoCountByTenantId(namespaceId) > 0);
}

// 创建命名空间 (第94-123行)
@PostMapping
@Secured(resource = AuthConstants.CONSOLE_RESOURCE_NAME_PREFIX + "namespaces", 
         action = ActionTypes.WRITE)
public Boolean createNamespace(
        @RequestParam("customNamespaceId") String namespaceId,
        @RequestParam("namespaceName") String namespaceName,
        @RequestParam(value = "namespaceDesc", required = false) String namespaceDesc) {
    // 参数校验 + 创建逻辑
    return namespaceOperationService.createNamespace(namespaceId, namespaceName, namespaceDesc);
}
```

---

### 3️⃣ **编辑命名空间**

**前端调用位置**：`console-ui/src/components/EditorNameSpace/EditorNameSpace.js`

```javascript
// 获取详情 (第70-91行)
getNamespaceDetail(record) {
  request({
    type: 'get',
    url: `v1/console/namespaces?show=all&namespaceId=${record.namespace}`,  // ← 调用后端
    success: res => {
      this.field.setValue('namespaceDesc', res.namespaceDesc);
    }
  });
}

// 提交修改 (第93-128行)
handleSubmit() {
  request({
    type: 'put',
    url: 'v1/console/namespaces',  // ← 调用后端
    data: {
      namespace: values.namespace,
      namespaceShowName: values.namespaceShowName,
      namespaceDesc: values.namespaceDesc,
    },
    success: res => {
      if (res === true) {
        this.props.getNameSpaces();  // 刷新列表
      }
    }
  });
}
```

**对应后端接口**：
```java
// NamespaceController.java

// 获取详情 (第82-85行)
@GetMapping(params = "show=all")
public Namespace getNamespace(@RequestParam("namespaceId") String namespaceId) {
    return namespaceOperationService.getNamespace(namespaceId);
}

// 编辑命名空间 (第147-157行)
@PutMapping
@Secured(resource = AuthConstants.CONSOLE_RESOURCE_NAME_PREFIX + "namespaces", 
         action = ActionTypes.WRITE)
public Boolean editNamespace(
        @RequestParam("namespace") String namespace,
        @RequestParam("namespaceShowName") String namespaceShowName,
        @RequestParam(value = "namespaceDesc", required = false) String namespaceDesc) {
    return namespaceOperationService.editNamespace(namespace, namespaceShowName, namespaceDesc);
}
```

---

### 4️⃣ **删除命名空间**

**前端调用位置**：`console-ui/src/components/NameSpaceList/NameSpaceList.js` (第121行附近)

```javascript
// 删除操作
request({
  type: 'delete',
  url: 'v1/console/namespaces',  // ← 调用后端
  data: { namespaceId: record.namespace },
  success: res => {
    if (res === true) {
      this.props.getNameSpaces();  // 刷新列表
    }
  }
});
```

**对应后端接口**：
```java
// NamespaceController.java (第165-169行)
@DeleteMapping
@Secured(resource = AuthConstants.CONSOLE_RESOURCE_NAME_PREFIX + "namespaces", 
         action = ActionTypes.WRITE)
public Boolean deleteNamespace(@RequestParam("namespaceId") String namespaceId) {
    return namespaceOperationService.removeNamespace(namespaceId);
}
```

---

### 5️⃣ **权限管理页面调用**

**前端调用位置**：`console-ui/src/pages/AuthorityControl/PermissionsManagement/NewPermissions.js`

```javascript
// 使用 Redux 获取命名空间列表
import { getNamespaces } from '../../../reducers/namespace';

@Component
@connect(state => ({ 
  namespaces: state.namespace.namespaces 
}), { getNamespaces, searchRoles })
class NewPermissions extends React.Component {
  componentDidMount() {
    this.props.getNamespaces();  // ← 触发 Redux action
  }
}
```

---

## 🆚 V1 vs V2 版本对比

Nacos 同时维护两个版本的命名空间控制器：

| 特性 | NamespaceController (V1) | NamespaceControllerV2 (V2) |
|------|-------------------------|---------------------------|
| **路径** | `/v1/console/namespaces` | `/v2/console/namespace` |
| **返回格式** | 直接返回对象/布尔值 | 统一 `Result<T>` 包装 |
| **调用方** | 前端 UI 组件 | 外部 API 调用 |
| **权限控制** | 有（部分接口） | 完善（所有接口） |
| **参数校验** | 简单 | 完善（正则、长度、唯一性） |
| **错误处理** | 返回 false | 抛出异常 + 错误码 |

---

## 📊 完整调用流程图

```
用户操作 (浏览器)
    ↓
┌─────────────────────────────────────────┐
│  前端 UI 组件                            │
│  ├─ NewNameSpace.js (新建)              │
│  ├─ EditorNameSpace.js (编辑)           │
│  ├─ NameSpaceList.js (列表/删除)        │
│  └─ PermissionsManagement.js (权限)     │
└─────────────────────────────────────────┘
    ↓ HTTP 请求
┌─────────────────────────────────────────┐
│  Redux Actions                           │
│  └─ namespace.js → getNamespaces()      │
└─────────────────────────────────────────┘
    ↓ dispatch
┌─────────────────────────────────────────┐
│  HTTP 请求                               │
│  GET    /v1/console/namespaces          │
│  POST   /v1/console/namespaces          │
│  PUT    /v1/console/namespaces          │
│  DELETE /v1/console/namespaces          │
└─────────────────────────────────────────┘
    ↓ Spring MVC 路由
┌─────────────────────────────────────────┐
│  NamespaceController (V1)               │
│  ├─ getNamespaces()                     │
│  ├─ createNamespace()                   │
│  ├─ editNamespace()                     │
│  ├─ deleteNamespace()                   │
│  └─ checkNamespaceIdExist()             │
└─────────────────────────────────────────┘
    ↓ @Autowired
┌─────────────────────────────────────────┐
│  NamespaceOperationService              │
│  ├─ getNamespaceList()                  │
│  ├─ createNamespace()                   │
│  ├─ editNamespace()                     │
│  ├─ removeNamespace()                   │
│  └─ getNamespace()                      │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  NamespacePersistService                │
│  └─ 数据库操作 (MySQL/Derby)            │
└─────────────────────────────────────────┘
```

---

## 🎯 总结

**NamespaceController (V1)** 主要被 **Nacos 控制台前端** 调用，用于：

1. ✅ 展示命名空间列表
2. ✅ 创建新命名空间
3. ✅ 编辑命名空间信息
4. ✅ 删除命名空间
5. ✅ 检查命名空间ID是否存在
6. ✅ 在权限管理页面提供命名空间选择

**注意**：虽然 V2 版本（`NamespaceControllerV2`）已经存在，但 V1 版本仍在使用中，两者并存。前端 UI 主要使用 V1 接口。