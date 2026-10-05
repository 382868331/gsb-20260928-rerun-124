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
- 失效控制：`Invalidate` 递增键的本地代际（仅表示本地失效次数）；失效前
  开始的加载在提交时比对代际，过期的旧结果直接丢弃（计入
  `Stats.StaleDrops`），不覆盖新值。失效后的新 `Get` 不会加入失效前
  启动的在途加载，而是发起新加载。
- 来源业务版本：`SourceLoader` 返回 `SourceResult{Value, Version, Deps}`。
  缓存记录每个键已知最新的来源版本（`SourceVersion`），与本地代际严格
  区分；较晚完成但来源版本更旧的读取会被丢弃（计入
  `Stats.OldSourceDrops`），不得覆盖已知较新的结果。
- 跨键依赖：结果可声明依赖多个键（`Deps`）；`Invalidate` 沿依赖边传播到
  所有派生条目，依赖环由访问集合截断，不会形成循环刷新。
- 决策可观测：每次存储、丢弃、失效都通过 `OnEvent` 上报 `Event`（含
  `Reason`：`loaded` / `invalidated-during-load` /
  `older-source-version` / `invalidate`）。
- 刷新期读旧值：`AllowStale` 开启后，过期条目先返回旧值，同时后台刷新。
- 容量淘汰：超出 `Capacity` 时按 LRU 淘汰，`OnEvict` 回调与
  `Stats.Evictions` 提供可检查的淘汰行为。
- 组合读取视图（`view.go`）：`Snapshot` 把多个相互依赖的键在同一
  次加锁下捕获为可保留的 `View`，每项记录来源业务版本
  （`SourceVersion`），未完成或失败的部分以 `ItemPending` /
  `ItemFailed` 连同错误显式标出。已交付的视图不可变：后来的刷新
  不会改变它，重复读取恒定；新视图能拿到最新值，两个视图可按键
  对比来源版本来解释差异。
- 候选刷新：`View.Refresh` 产出可查看的 `Candidate`，其加载是
  “试探性加载”（probes）：结果只交给该候选，绝不写入公共存储。
  部分来源失败时原视图保留、候选中的新值仍可逐项比较，失败部分
  以 `ItemFailed` 连同错误单独说明；只有全部成员就绪且视图内依赖
  边全部满足时 `Adopt` 才在锁内复验代际/来源版本并把整组结果原子
  发布到公共缓存。等待者中途退出（`waiter-left`）或来源在候选
  加载期间再次变化（`superseded`）都让候选进入明确的
  `CandidateAborted` 去向。候选只是数据，不持有锁、加载任务或
  goroutine，丢弃后不留占用。
- 统一依赖一致性边界：普通组合读取 `Snapshot` 与候选采用遵守同一条
  边界——整组成员全部就绪、且落在视图内的每条依赖边都指向就绪成员，
  才一次性发布；任何部分失败/待定都不发布新值，因此失败窗口中的
  重复读取不会把新根节点/新分支与旧叶子拼在一起并谎称完整。读取
  入口不能绕过候选采用校验：未采用的新值只能在候选（或当次未完成
  视图）中查看，公共缓存始终只保留上一次已确认视图；来源恢复后
  的下一次完整读取或 `Adopt` 才发布新组合，新老视图可按键对比来源
   版本来解释采用了哪份来源版本。
- 确认视图与采用决定持久化（`journal.go`、`persist.go`）：设置
  `Options.Journal`（如 `NewFileJournal(path)`，逐行 CRC 校验的追加
  日志）后，每次 `Snapshot`/`Adopt` 发布完整组合都会追加一条
  `confirmed` 记录——含每个键的值、来源版本、本地代际、依赖边与确认
  依据（`snapshot`/`adopt`）；失败、被取代、等待者退出或被放弃的候选
  追加 `candidate` 记录，保留其拟采用的新值与失败原因仅供审计。
  `Restore(opts, journal)` 在重开时把最近一条完整可信的 confirmed
  记录恢复为公共存储内容（来源版本、依赖索引、代际一并复原），
  `Cache.Confirmed()` 可随时解释上次发布的是哪份组合；候选记录绝不
  回放进存储，失败候选的新值不会成为当前值，恢复出的旧视图只报告
  记录时的事实。日志损坏、截断或 confirmed 记录不完整时 `Restore`
  返回明确错误，同时缓存仍保留最后可信组合。

## 验证

```sh
go test -race ./...
```
