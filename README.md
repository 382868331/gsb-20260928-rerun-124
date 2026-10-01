# appcache — 应用缓存运行库

纯 Go 库，不依赖外部缓存服务。组织键、版本与加载任务，提供热点缓存的
合并加载与失效控制。

## 设计

- `Store` 接口（`store.go`）：可替换的存储策略。默认 `NewMapStore()`；
  换成其他实现时，调用者通过 `Cache` 观察到的结果不变（见
  `TestAlternateStoreSameResults`）。
- `Cache`（`cache.go`）：在 `Store` 之上管理 LRU 顺序、每键本地代际、
  来源业务版本、跨键依赖和在途加载任务（singleflight 合并）。

## 行为规则

- 合并加载：同一键的并发 `Get` 只共享一次 loader 调用。
- 取消隔离：加载用 `context.WithoutCancel` 脱离单个调用者；某个等待者
  取消只影响自己，不取消其他人的共同加载。
- 两类版本严格分离：本地代际（`Version()`）只记录 `Invalidate` 次数，
  用于丢弃失效期间完成的加载；来源业务版本（`Result.Version`、
  `SourceVersion()`）由 loader 给出，用于判断数据新旧。
- 失效控制：`Invalidate` 递增键的本地代际；失效前开始的加载在提交时
  比对代际，过期的旧结果直接丢弃（计入 `Stats.StaleDrops`）。失效后
  新的 `Get` 会在新代际上重新加载，旧加载只为自己的等待者完成。
- 来源回退：提交时若结果的业务版本低于已知最高来源版本，该结果丢弃
  （计入 `Stats.VersionDrops`），较晚完成的旧来源读取不得覆盖较新结果。
- 跨键依赖：`Result.Deps` 声明结果依赖的键；`Invalidate` 会沿依赖反向
  传播到所有（传递）依赖方，visited 集合保证依赖环不会导致循环刷新。
  重新加载返回新的依赖列表时，旧的反向边随即清除。
- 刷新期读旧值：`AllowStale` 开启后，过期条目先返回旧值，同时后台刷新；
  但旧值的来源版本低于已知来源版本时不再提供，直接走合并重载。
- 事件记录：每次存储、丢弃、失效都记录 `Event`（含原因），可通过
  `Events()` 读取有界日志，或用 `Options.OnEvent` 回调逐条观察。
- 容量淘汰：超出 `Capacity` 时按 LRU 淘汰，`OnEvict` 回调与
  `Stats.Evictions` 提供可检查的淘汰行为。

## 验证

```sh
go test -race ./...
```
