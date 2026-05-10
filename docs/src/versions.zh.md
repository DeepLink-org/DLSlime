# 版本

DLSlime 文档使用 [mike](https://github.com/jimporter/mike) 做版本化发布，并接入
MkDocs Material 的版本选择器。

## 发布通道

| 通道     | 含义                                 | 发布方式                              |
| -------- | ------------------------------------ | ------------------------------------- |
| `dev`    | 来自 `main` 或 `master` 的开发版文档 | GitHub Actions 将分支构建发布为 `dev` |
| `latest` | 指向最新文档的别名                   | 分支和 tag 构建都会更新               |
| `x.y.z`  | 发行版文档                           | 例如 `v0.1.0` 会发布为版本 `0.1.0`    |

## 本地预览

普通写作预览：

```bash
cd docs
make serve
```

如果要预览和 GitHub Pages 一样的版本选择器：

```bash
cd docs
make deploy-dev
make serve-versioned
```

`make deploy-dev` 会在本地 `gh-pages` 分支写入 `dev/latest` 版本，不会 push。
