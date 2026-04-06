# GitHub Actions CI/CD 工作流说明

## 📁 文件结构

```
.github/
└── workflows/
    ├── ci.yml       # 持续集成：自动构建、编译、测试、打包
    └── deploy.yml   # 持续部署：自动部署到服务器（可选）
```

---

## 🔧 ci.yml — 持续集成工作流

### 触发条件
- **push** 到 `main` / `master` / `recover-branch` 分支
- **Pull Request** 到 `main` / `master`
- **手动触发** (workflow_dispatch)

### 包含的 Job

| Job | 名称 | 说明 |
|-----|------|------|
| 1 | Compile | Scala + Java 编译检查 |
| 2 | Test | 单元测试执行 |
| 3 | Package | Maven Shade 打包生成 uber-jar |
| 4 | Code Quality | 代码质量扫描 (Scalastyle) |
| 5 | Summary | 构建状态汇总报告 |

### 构建产物
- 自动上传 Shade JAR 到 GitHub Artifacts，保留 30 天
- 命名格式: `overmatch-jar-{commit-sha}`

---

## 🚀 deploy.yml — 持续部署工作流

### 触发条件
- push tag `v*` 时自动部署
- 手动触发

### 部署流程
1. CI 通过后自动触发
2. Maven 打包生成 JAR
3. SCP 上传到目标服务器
4. 可选：通过 SSH 重启 Spark 任务

> ⚠️ 部分配置需要根据实际情况修改：
> - `DEPLOY_HOST` / `DEPLOY_USER` / `DEPLOY_KEY` / `DEPLOY_PATH` Secrets
> - 服务器上的实际部署目录和启动脚本

---

## ⚙️ 配置指南

### 必需的 GitHub Secrets

在仓库 Settings → Secrets and variables → Actions 中添加：

| Secret 名 | 说明 | 示例 |
|-----------|------|------|
| `DEPLOY_HOST` | 目标服务器 IP 或域名 | `192.168.45.11` |
| `DEPLOY_USER` | SSH 登录用户名 | `root` |
| `DEPLOY_KEY` | SSH 私钥内容 | `-----BEGIN OPENSSH...` |
| `DEPLOY_PATH` | 服务器上 JAR 部署路径 | `/opt/realtime-engine/jars` |

### 环境变量调整

可在 `ci.yml` 中修改：
- `JAVA_VERSION`: 当前为 `8`，与项目 pom.xml 一致
- `MAVEN_OPTS`: Maven JVM 参数

---

## 📊 使用方式

### 查看构建状态
每次 push 后，GitHub 仓库的 Actions 页面会显示构建进度和结果。

### 下载构建产物
构建成功后，在 Actions → 对应 run → Artifacts 中下载打包好的 JAR 文件。

### 手动触发
1. 进入仓库 Actions 页面
2. 选择左侧 "CI - Build & Test" 或 "CD - Deploy"
3. 点击 "Run workflow"
