# KwikPaste 同步服务器

KwikPaste（快贴）远程同步的服务端：设备之间不在同一个局域网时，经这台服务器同步剪贴板记录和设置。

- **端到端加密**：服务器只存、只转发密文，拿不到组密钥，也看不到剪贴板内容。
- **没有账号**：第一台设备建组，其他设备输入 8 位配对码加入。
- **一个程序两种模式**：`selfhost`（自部署，默认）和 `official`（官方服务器 `https://sync.fastthree.com`）。
- 单个二进制 + SQLite，没有其他依赖。协议见 [`docs/PROTOCOL.md`](docs/PROTOCOL.md)。

## 自部署

### 一条命令

```bash
docker run -d --name kwikpaste-sync --restart unless-stopped \
  -p 8686:8686 -v kwikpaste-sync-data:/data \
  ghcr.io/mansandadada/kwikpaste-sync-server:latest
```

想把数据放在宿主机的某个目录（NAS 上常见），把 `-v kwikpaste-sync-data:/data` 换成 `-v /volume1/docker/kwikpaste-sync:/data`，
并先让这个目录归容器里的用户（uid 10001）所有，否则服务写不进数据，会反复重启：

```bash
mkdir -p /volume1/docker/kwikpaste-sync && chown 10001:10001 /volume1/docker/kwikpaste-sync
```

镜像同时支持 `linux/amd64` 和 `linux/arm64`。目前只发布在 ghcr.io，暂时没有国内镜像。

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
| `KP_SYNC_JOIN_FAILURES_PER_HOUR` | `1000` | 仅官方模式：全站每小时允许多少次配对码错误（所有 IP 合计），到顶后所有加入请求返回 429 |
| `KP_SYNC_GROUP_STORAGE_MB` | `20` | 仅官方模式：每组最多存多少 MB 条目 |
| `KP_SYNC_STORAGE_MB` | `2048` | 仅官方模式：全站最多存多少 MB 条目 |
| `KP_SYNC_GROUP_FORWARD_MB_PER_DAY` | `200` | 仅官方模式：每组每天实时转发多少 MB 非图片条目（按送达设备数计，北京时间零点重置） |
| `KP_SYNC_FRAMES_PER_DEVICE_PER_10MIN` | `1200` | 仅官方模式：每台设备每 10 分钟最多上传多少条 |
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

### 没有公网 IP

家里的 NAS 没有公网 IP 也能用：设备之间用 Tailscale、ZeroTier 这类组网工具连通，或者用内网穿透，
在 KwikPaste 里填设备能访问到的地址（如 `http://100.64.0.2:8686`）。只在同一个局域网里用的话，直接用局域网同步更省事。

### 数据与备份

所有数据都在 `/data` 卷里。备份时先停容器再复制整个卷；删掉卷就清空了服务器（客户端会重新上传）。
组里最后一台设备退出时，整组数据立即删除。

## 官方服务器（运维）

官方模式只做立即同步：文本留 24 小时，图片只实时转发不存储（单张 ≤ 2 MiB，每组每天 30 MiB、全站每天 1 GiB，北京时间零点重置），
不同步收藏等元数据，每组 10 台设备，每 IP 每天建组 10 次、每小时加入 30 次，90 天没有设备连接的组整组删除。
配对码是 8 位数字，全站每小时最多 1000 次配对码错误，超过后所有加入请求暂停到最早一次错误滑出一小时窗口。
每组最多存 20 MiB、全站最多存 2 GiB，每组每天实时转发 200 MiB 非图片条目，每台设备每 10 分钟最多上传 1200 条；存储与转发超限的条目单条返回 `quota` 和 `retryAt`，上传频率超限整个请求返回 429。以上数值都可用上面的环境变量调整。
另外固定：每条按密文加 512 字节计入存储和转发；只收文本、图片和设置三种条目，单条设置 ≤ 64 KiB，每组永久保留的条目（设置）合计 ≤ 1 MiB；
不读数据的事件流积压超过 4 MiB 就断开；nginx 限每个 IP 最多 16 个并发连接。

官方服务器是一个独立容器，放在那台机器已有的 `nginx` 容器后面。容器在一个单独的内部网络里：不对外发布端口，
没有外网出口，也访问不到宿主机上的任何服务，只有 nginx 能连进来。

1. 拉镜像（那台机器拉不了 Docker Hub，但能访问 ghcr.io）：

   ```bash
   docker pull ghcr.io/mansandadada/kwikpaste-sync-server:1.2.0
   ```

2. 内部网络（只建一次）。`--internal` 去掉了外网出口；宿主机在这个网桥上仍有一个地址，所以再用一条防火墙规则挡住
   从网桥进宿主机的流量（Docker 28 之前的版本不支持 `gateway_mode_ipv4=isolated`），规则由一个开机自启的 systemd 单元保证：

   ```bash
   docker network create --internal --subnet 172.31.255.0/28 \
     -o com.docker.network.bridge.name=kpsync0 kwikpaste-sync-net
   docker network connect kwikpaste-sync-net nginx
   cat > /etc/systemd/system/kwikpaste-sync-guard.service <<'UNIT'
   [Unit]
   Description=Block the KwikPaste sync container network from reaching this host
   After=docker.service
   Wants=docker.service

   [Service]
   Type=oneshot
   RemainAfterExit=yes
   ExecStart=/bin/sh -c 'iptables -C INPUT -i kpsync0 -j DROP 2>/dev/null || iptables -I INPUT -i kpsync0 -j DROP'

   [Install]
   WantedBy=multi-user.target
   UNIT
   systemctl daemon-reload && systemctl enable --now kwikpaste-sync-guard
   ```

3. 运行（数据目录要属于容器里的 uid 10001）：

   ```bash
   mkdir -p /home/kwikpaste-sync && chown 10001:10001 /home/kwikpaste-sync
   docker run -d --name kwikpaste-sync --restart unless-stopped \
     --memory 256m --cpus 1 --log-opt max-size=10m --log-opt max-file=3 \
     --network kwikpaste-sync-net --ip 172.31.255.10 -v /home/kwikpaste-sync:/data \
     -e KP_SYNC_MODE=official -e KP_SYNC_TRUST_PROXY=1 \
     ghcr.io/mansandadada/kwikpaste-sync-server:1.2.0
   ```

   升级：`docker pull` 新版本后 `docker rm -f kwikpaste-sync`，再用同样的参数 `docker run`（数据在卷里，不受影响；
   表结构有变化时要写迁移）。nginx 容器如果被重建，要重新 `docker network connect kwikpaste-sync-net nginx`。

4. nginx：[`deploy/nginx-official.conf`](deploy/nginx-official.conf) 放进 nginx 的 `conf.d/`，证书用 acme.sh（webroot）单独签发：

   ```bash
   acme.sh --issue -d sync.fastthree.com -w /home/nginx/html --keylength ec-256 --server letsencrypt
   acme.sh --install-cert -d sync.fastthree.com --ecc \
     --key-file /home/nginx/certs/sync.fastthree.com/privkey.pem \
     --fullchain-file /home/nginx/certs/sync.fastthree.com/fullchain.pem \
     --reloadcmd "docker exec nginx nginx -s reload"
   ```

   `X-Real-IP` 不能少：服务开了 `KP_SYNC_TRUST_PROXY=1`，按 IP 限流用的就是它。`proxy_buffering off` 也不能少，否则事件流会被攒着不发。

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
