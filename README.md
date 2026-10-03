# VaultStream 增量同步清单库

只用 Python 标准库 + SQLite 实现的文件元数据增量同步清单库。
只处理文件元数据（`name` / `content_hash`），内容哈希由上游保证有效。

## 环境要求

- Python 3.8+（仅标准库，无第三方依赖，无需任何外部服务）

## API

```python
from vaultstream import VaultStore, FileState

store = VaultStore("manifest.db")          # 可注入 clock=... 与 session_ttl=...

seq = store.commit("file-id", FileState(name="a.txt", content_hash="sha256:..."))
seq = store.commit("file-id", None)        # 删除标记；返回值严格递增

page = store.changes(0)                    # 传 checkpoint(int) 开启会话，封定终点 H
page = store.changes(page.cursor)          # 传 cursor(str) 继续翻页
# page.records: 净变化记录，按各文件截至 H 的最后提交序号升序
# 非末页：恰好 100 条 + cursor；末页：cursor=None 且 checkpoint=H

store.compact(10)                          # 推进新会话最小 checkpoint 并清理历史
store.close()
```

- 会话自开启起 600 秒有效（`session_ttl` 可调），翻页不续期；过期游标抛
  `CursorExpiredError`。会话行持久化在 SQLite 中，关闭重开后未过期游标仍有效。
- `compact(F)`：`F` 须单调不减且不超过当前最大提交序号。令 R 为 `F` 与所有
  未过期会话起点 C 的最小值（无有效会话时 R=F）；清理后每个文件至多保留一个
  序号 ≤ R 的历史版本，同时保留计算所有仍被允许同步区间所需的基线。
- 每个操作各自使用短事务，不跨调用保持事务、读快照或锁；分页按位置续查，
  常驻内存不随总文件数或历史版本数增长。

## 运行测试

```bash
python3 -m unittest discover -s tests -v
```

## 目录结构

- `vaultstream/store.py` — 核心实现（commit / changes / compact）
- `vaultstream/types.py` — `FileState` / `ChangeRecord` / `ChangesPage`
- `vaultstream/errors.py` — 异常类型
- `tests/test_vaultstream.py` — 自动化测试（含独立全量回放核对）
