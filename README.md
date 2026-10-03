# VaultStream 增量同步清单库

只用 Python 标准库和 SQLite 实现的文件元数据增量同步清单库。

## API

```python
from vaultstream import VaultSync

vs = VaultSync("vault.db")                 # 可注入时钟: VaultSync(path, clock=my_clock)

seq = vs.commit("file-1", {"name": "a.txt", "content_hash": "abc"})
seq = vs.commit("file-1", None)            # 删除标记；返回严格递增的全局序号

page = vs.changes(checkpoint)              # int checkpoint 开启会话，封定终点 H
page = vs.changes(page.cursor)             # str cursor 继续翻页（可重放，结果相同）
# page.records: List[Change]               # 每页最多 100 条，非末页必满 100 条
# page.cursor: 非末页给出；page.checkpoint: 仅末页给出，值为 H

vs.compact(floor)                          # 推进新会话的最小 checkpoint 并清理历史
vs.close()
```

- 会话自开启起有效 600 秒，翻页不续期；过期游标抛 `CursorExpiredError`。
- checkpoint 低于当前下限抛 `CheckpointTooOldError`；此前开启且未过期的会话不受影响。
- 游标持久化在库中，关闭重开后未过期游标仍然有效。
- 异常类型：`VaultSyncError`、`CheckpointTooOldError`、`CursorExpiredError`、
  `InvalidCursorError`、`InvalidCheckpointError`。

## 运行测试

```sh
python3 -m unittest test_vaultstream -v
```

## 实现要点

- `commits` 表保存全部提交历史（`seq` 自增主键），`sessions` 表持久化会话
  （起点 C、终点 H、过期时间），`meta` 表保存当前下限。
- 翻页用 keyset 分页：按“文件截至 H 的最后一次提交序号”升序，游标记录上一页
  考察到的序号，不重放已越过的提交；仅对涉及的文件查询 C 之前的基线版本。
- `compact(F)` 令 R = min(F, 所有未过期会话的 C)，删除每个文件序号 ≤ R 的
  历史版本中除最新一条（基线）外的全部行，因此仍被允许的同步区间都能正确计算。
