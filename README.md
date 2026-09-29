# appcache — 应用缓存运行库

纯 Go 库，不依赖外部缓存服务。组织键、版本与加载任务，提供热点缓存的
合并加载与失效控制。

## 设计

- `Store` 接口（`store.go`）：可替换的存储策略。默认 `NewMapStore()`；
  换成其他实现时，调用者通过 `Cache` 观察到的结果不变（见
  `TestAlternateStoreSameResults`）。
- `Cache`（`cache.go`）：在 `Store` 之上管理 LRU 顺序、每键版本号和
  在途加载任务（singleflight 合并）。

## 行为规则

- 合并加载：同一键的并发 `Get` 只共享一次 loader 调用。
- 取消隔离：加载用 `context.WithoutCancel` 脱离单个调用者；某个等待者
  取消只影响自己，不取消其他人的共同加载。
- 失效控制：`Invalidate` 递增键版本；失效前开始的加载在提交时比对版本，
  过期的旧结果直接丢弃（计入 `Stats.StaleDrops`），不覆盖新值。
- 刷新期读旧值：`AllowStale` 开启后，过期条目先返回旧值，同时后台刷新。
- 容量淘汰：超出 `Capacity` 时按 LRU 淘汰，`OnEvict` 回调与
  `Stats.Evictions` 提供可检查的淘汰行为。

## 验证

```sh
go test -race ./...
```

