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
- 组合读取视图（`view.go`）：`Snapshot` 把多个相互依赖的键捕获为
  可保留的 `View`，每项记录来源业务版本（`SourceVersion`），未完成
  或失败的部分以 `ItemPending` / `ItemFailed` 连同错误显式标出。
  所有读取入口（`Snapshot` 与候选采用）共用同一条依赖一致性边界：
  视图内每条依赖边都必须落在就绪成员上，且两边来源业务版本必须
  一致；失败叶子会沿依赖边（含菱形）把父节点一并排除，任何读取
  都不会把新根/新分支与旧叶子拼成“完整”组合。已交付的视图不可
  变：后来的刷新不会改变它，重复读取恒定；新视图拿到新值后，两个
  视图可按键对比来源版本来解释差异。
- 候选与确认视图分离：组合刷新的每次加载都是探针（probe），结果
  只暂存在候选里，绝不自动写入公共缓存。`View.Refresh` 产出可查看
  的 `Candidate`：部分来源失败时原确认视图保留、候选中的新值仍可
  逐项查看（失败部分与可供比较的新值分别标注），但只有全部成员
  就绪且视图内依赖边全部满足时 `Adopt` 才会在缓存锁内复验（代际未
  变、未被更新的已知来源版本超越、依赖一致）并把整组值原子发布；
  等待者中途退出（`waiter-left`）或来源在候选加载期间再次变化
  （`superseded`）都让候选进入明确的 `CandidateAborted` 去向。未
  采用的候选不会在公共缓存留下任何值；候选不持有锁，探针加载与调
  用者取消脱钩，丢弃候选不占用在途任务表。

## 验证

```sh
go test -race ./...
```
