# KwikPaste 同步服务器

KwikPaste（快贴）远程同步的服务端：设备之间不在同一个局域网时，经这台服务器同步剪贴板记录和设置。

- **端到端加密**：服务器只存、只转发密文，拿不到组密钥，也看不到剪贴板内容。
- **没有账号**：第一台设备建组，其他设备输入 6 位配对码加入。
- **一个程序两种模式**：`selfhost`（自部署，默认）和 `official`（官方服务器 `https://paste.fastthree.com/sync`）。
- 单个二进制 + SQLite，没有其他依赖。协议见 [`docs/PROTOCOL.md`](docs/PROTOCOL.md)。

## 自部署

### 一条命令

```bash
docker run -d --name kwikpaste-sync --restart unless-stopped \
  -p 8686:8686 -v kwikpaste-sync-data:/data \
  ghcr.io/mansandadada/kwikpaste-sync-server:latest
```

国内拉不动 ghcr.io 时换成阿里云镜像 `registry.cn-hangzhou.aliyuncs.com/<命名空间>/kwikpaste-sync-server:latest`（以发布说明里给出的地址为准）。

### docker compose

仓库里的 [`docker-compose.yml`](docker-compose.yml) 可以直接用：

```bash
docker compose up -d
```

### 服务器令牌

自部署服务器的每个请求都要带服务器令牌，没有令牌的人连接不上。

- 不设 `KP_SYNC_TOKEN` 时，首次启动自动生成，保存在数据目录的 `server-token` 文件里，每次启动也会打印在日志里：

  ```bash
  docker logs kwikpaste-sync 2>&1 | grep "Server token"
  # 或者
  docker exec kwikpaste-sync cat /data/server-token
  ```

- 想自己指定就设环境变量 `KP_SYNC_TOKEN`（它优先于文件）。
- 在 KwikPaste 里选「自部署服务器」，填服务器地址（如 `https://sync.example.com`）和这个令牌。
- 换令牌：删掉 `server-token` 文件（或改 `KP_SYNC_TOKEN`）后重启，所有设备都要重新填。

### 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `KP_SYNC_MODE` | `selfhost` | `selfhost` 或 `official` |
| `KP_SYNC_LISTEN` | `0.0.0.0:8686` | 监听地址 |
| `KP_SYNC_DATA` | `./data`（镜像里是 `/data`） | 数据目录：`sync.sqlite3`、`blobs/`、`server-token` |
| `KP_SYNC_TOKEN` | 自动生成 | 自部署服务器令牌 |
| `KP_SYNC_MAX_IMAGE_MB` | `100` | 单张图片上限（MB，按密文算） |
| `KP_SYNC_MAX_DEVICES` | `20` | 每个同步组最多几台设备 |
| `KP_SYNC_TEXT_RETENTION_HOURS` | 不设 = 永久保留 | 文本记录保留多少小时 |
| `KP_SYNC_TRUST_PROXY` | 不设 | 设为 `1` 时从 `X-Real-IP` / `X-Forwarded-For` 取客户端 IP（只在反向代理后面用；官方模式的按 IP 限流靠它） |
| `RUST_LOG` | `info` | 日志级别 |

自部署模式的其余策略固定为：同步历史、收藏 / 置顶 / 备注 / 删除、设置都可用，图片存储可补齐，单条文本密文 ≤ 16 MiB。
日志不会记录设备令牌、配对码和任何条目内容。

### HTTPS

客户端走公网时请放在 HTTPS 后面。事件流是一条长连接，反向代理要关掉响应缓冲、放宽读超时。

Caddy（自动申请证书）：

```caddyfile
sync.example.com {
    reverse_proxy 127.0.0.1:8686 {
        flush_interval -1
    }
}
```

nginx：

```nginx
server {
    listen 443 ssl;
    server_name sync.example.com;
    # ssl_certificate / ssl_certificate_key ...

    location / {
        proxy_pass http://127.0.0.1:8686;
        proxy_http_version 1.1;
        proxy_buffering off;
        proxy_request_buffering off;
        proxy_read_timeout 600s;
        client_max_body_size 0;
    }
}
```

这时 Docker 端口可以只绑本机：`-p 127.0.0.1:8686:8686`。

### 数据与备份

所有数据都在 `/data` 卷里。备份时先停容器再复制整个卷；删掉卷就清空了服务器（客户端会重新上传）。
组里最后一台设备退出时，整组数据立即删除。

## 官方服务器（运维）

官方模式只做立即同步：文本留 24 小时，图片只实时转发不存储（单张 ≤ 2 MiB，每组每天 30 MiB、全站每天 1 GiB，北京时间零点重置），
不同步收藏等元数据，每组 10 台设备，每 IP 每天建组 10 次、每小时加入 30 次，90 天没有设备连接的组整组删除。

1. 把 Linux 版二进制放到 `/usr/local/bin/kwikpaste-sync-server`，安装 [`deploy/kwikpaste-sync.service`](deploy/kwikpaste-sync.service)：

   ```bash
   sudo cp deploy/kwikpaste-sync.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now kwikpaste-sync
   ```

   数据在 `/var/lib/kwikpaste-sync`，只监听 `127.0.0.1:8686`。

2. nginx（`paste.fastthree.com` 的 server 块里）：

   ```nginx
   location /sync/ {
       proxy_pass http://127.0.0.1:8686/;
       proxy_buffering off;
       proxy_read_timeout 600s;
       client_max_body_size 8m;
       proxy_set_header X-Real-IP $remote_addr;
   }
   ```

   `X-Real-IP` 这一行不能少：服务开了 `KP_SYNC_TRUST_PROXY=1`，按 IP 限流用的就是它。

## 本地运行（Windows）

```powershell
# 自部署模式（令牌写到 .\data\server-token）
cargo run --release

# 官方模式
$env:KP_SYNC_MODE = "official"; cargo run --release
```

测试与检查：

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

## 发布镜像

推送 `v*` 标签后 GitHub Actions 构建 `linux/amd64` + `linux/arm64` 镜像，推到
`ghcr.io/<owner>/kwikpaste-sync-server`；配置了仓库机密 `ACR_REGISTRY`（如 `registry.cn-hangzhou.aliyuncs.com/<命名空间>`）、
`ACR_USERNAME`、`ACR_PASSWORD` 时同时推到阿里云容器镜像服务。
