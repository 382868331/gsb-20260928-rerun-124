# 应用缓存运行库

这是一个不依赖外部缓存服务的 Go 进程内缓存运行库。缓存协调器负责键、版本、失效代际、加载等待者和容量淘汰；实际存储通过 Storage 接口接入，后续替换存储后端不需要改变调用方的 Get、Refresh、Put 和 Invalidate 语义。

## 核心语义

- 同键同失效代际的并发加载只启动一个来源任务，其他调用合并等待。
- 单个等待者的 context.Context 取消只退出该等待者，不取消共享加载。
- 加载结果必须匹配当前失效代际，且版本不能小于当前版本，旧结果返回 ErrStaleVersion，不能覆盖新值。
- Invalidate 将当前值显式标记为陈旧；普通 Get 等待重新加载。
- GetWith 配合 AllowStaleOnRefresh 可立即读取旧值，并在后台刷新。
- 容量大于 0 时按最近最少使用淘汰，淘汰事件通过 OnEviction 可检查，原因标记为 EvictionCapacity。
- 默认存储为内存 LRU，也可传入自定义 Storage；协调逻辑不只封装一个映射表。

## 快速示例

```go
cache := cacheruntime.New(cacheruntime.Options[string, []byte]{
    Capacity: 10000,
    Loader: func(ctx context.Context, key string) (cacheruntime.Loaded[[]byte], error) {
        return loadFromSource(ctx, key)
    },
    OnEviction: func(event cacheruntime.Eviction[string, []byte]) {
        metrics.Evict(string(event.Reason))
    },
})

value, version, err := cache.Get(ctx, key)
staleValue, oldVersion, staleErr := cache.GetWith(ctx, key, cacheruntime.GetOptions{
    AllowStaleOnRefresh: true,
})
cache.Invalidate(key)
```

## 存储替换

实现 Storage 接口即可替换默认内存后端。存储实现不需要理解加载合并、版本冲突或等待者取消；这些规则由 Cache 统一保证。若自定义存储有容量限制，仍应提供准确的 Len 和 Oldest，缓存协调器会继续执行容量策略。

## 验证

```sh
go test -race ./...
go vet ./...
```
