# 基于 PyFlink 的实时电商订单监控与异常交易预警系统

本项目实现一个本地可运行的实时电商监控闭环：

```text
Python 订单流生成器 -> Socket/Kafka 实时流 -> PyFlink 事件时间窗口与异常检测 -> SQLite -> FastAPI -> Vue + ECharts 大屏
```

## 功能

- Python 订单生成器支持 socket 和 Kafka 两种实时输出模式
- PyFlink DataStream 支持 socket 或 Kafka 实时数据源
- 基于订单 `event_time` 分配事件时间，并使用 Watermark 处理轻微乱序数据
- 使用事件时间滚动窗口统计全局、商品、城市指标
- 异常检测覆盖支付失败、风险设备/IP、高频下单、连续失败、设备跳变、异地跳变、同 IP/同设备关联多用户
- SQLite 保存原始订单、销售窗口、商品窗口、城市窗口和异常告警
- FastAPI 提供监控接口和内置演示页
- Vue + ECharts 展示实时趋势、告警分布、风险趋势、热门商品、城市销售分布、最新订单和异常告警

## 目录结构

```text
realtime-ecommerce-flink/
  backend/                  FastAPI 后端和 SQLite 初始化
  data/                     SQLite 数据库存放目录
  flink_jobs/               PyFlink 实时处理任务
  generator/                实时订单流生成器，支持 socket/Kafka 输出
  scripts/                  初始化数据库和写入演示数据脚本
  frontend/                 Vue/Vite 前端和 standalone 演示页
  docs/                     报告正文与运行截图
  requirements.txt          FastAPI 后端依赖
  requirements-flink.txt    PyFlink 依赖范围
```

## 环境准备

建议使用：

- Python 3.10 或 3.11
- JDK 11 或 17
- Node.js 18+，完整 Vue/Vite 前端需要
- Kafka 4.3，本机 Homebrew 安装路径通常是 `/opt/homebrew/opt/kafka`
- SQLite 使用 Python 标准库，无需单独安装

所有终端都先进入项目目录；下面命令使用你当前项目的本机路径：

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
```

首次运行或换新环境时安装依赖：

```bash
python -m pip install -r requirements.txt
python -m pip install -r requirements-flink.txt
python -c "import pyflink; print(pyflink.__file__)"
```

`requirements-flink.txt` 固定使用 `apache-flink==2.0.0`，因为当前 PyFlink 2.2.x 暂无可用的 Kafka connector JAR。Kafka 链路还需要下载 Flink Kafka connector：

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python -m pip install --force-reinstall -r requirements-flink.txt
scripts/install_flink_kafka_connector.sh
```

前端首次运行安装 Node 依赖：

```bash
cd /YOURPATH/realtime-ecommerce-flink/frontend
npm install
```

## 只启动后端和前端

这条链路不启动实时 PyFlink 作业，适合先确认 FastAPI、SQLite 和 Vue 页面可以打开。演示数据由 `scripts/seed_demo_data.py` 写入。

终端 1：初始化数据库，写入演示数据，启动 FastAPI 后端。

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python scripts/init_db.py
python scripts/seed_demo_data.py
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

终端 2：启动 Vue/Vite 前端。

```bash
cd /YOURPATH/realtime-ecommerce-flink/frontend
npm install
npm run dev
```

访问地址：

- 前端页面：[http://127.0.0.1:5173](http://127.0.0.1:5173)
- 后端健康检查：[http://127.0.0.1:8000/api/health](http://127.0.0.1:8000/api/health)
- FastAPI 内置页面：[http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard)

如果后端端口不是 `8000`，前端可以这样指定 API 地址：

```bash
cd /YOURPATH/realtime-ecommerce-flink/frontend
VITE_API_BASE_URL=http://127.0.0.1:8000 npm run dev
```

## Socket 实时链路

这条链路是：

```text
Python 订单生成器 -> Socket 9999 -> PyFlink 作业 -> SQLite -> FastAPI 后端 -> Vue 前端
```

终端 1：启动 socket 订单流生成器。

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python generator/order_stream_generator.py \
  --sink socket \
  --host 127.0.0.1 \
  --port 9999 \
  --interval 0.5
```

如果不想在终端持续打印每条订单，可以追加 `--quiet`。

终端 2：启动 PyFlink 作业，从 socket 读取订单并写入 SQLite。

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python scripts/init_db.py
python flink_jobs/order_stream_job.py \
  --source socket \
  --host 127.0.0.1 \
  --port 9999 \
  --window-seconds 10 \
  --watermark-delay-seconds 3 \
  --frequency-threshold 6
```

终端 3：启动 FastAPI 后端。

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

终端 4：启动 Vue/Vite 前端。

```bash
cd /YOURPATH/realtime-ecommerce-flink/frontend
npm install
npm run dev
```

访问：[http://127.0.0.1:5173](http://127.0.0.1:5173)

## Kafka 4.3 实时链路

这条链路是：

```text
Python 订单生成器 -> Kafka ecommerce-orders -> PyFlink KafkaSource -> SQLite -> FastAPI 后端 -> Vue 前端
```

本项目提供了 `scripts/kafka_demo.sh` 帮助本地 Kafka 4.x/KRaft 演示：发现 Kafka 命令、格式化 KRaft 存储、启动 broker、创建 topic、检查连通性。你的 Kafka 4.3 如果是 Homebrew 安装，使用下面的 `KAFKA_HOME`：

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
export KAFKA_HOME=/opt/homebrew/opt/kafka
scripts/kafka_demo.sh info
```

如果是手动解压安装，`KAFKA_HOME` 改成你的 Kafka 解压目录，例如：

```bash
export KAFKA_HOME=/path/to/kafka_2.13-4.3.0
```

如果你的 Kafka 配置文件不在默认位置，可以追加：

```bash
export KAFKA_CONFIG=/path/to/server.properties
```

首次运行 Kafka 4.x/KRaft 时，先格式化本地存储；同一份 `server.properties` 只需要做一次：

```bash
cd /YOURPATH/realtime-ecommerce-flink
export KAFKA_HOME=/opt/homebrew/opt/kafka
scripts/kafka_demo.sh format
```

终端 1：启动 Kafka broker，保持这个终端运行。

```bash
cd /YOURPATH/realtime-ecommerce-flink
export KAFKA_HOME=/opt/homebrew/opt/kafka
scripts/kafka_demo.sh start
```

终端 2：确认 Kafka 可连接，并创建演示 topic。

```bash
cd /YOURPATH/realtime-ecommerce-flink
export KAFKA_HOME=/opt/homebrew/opt/kafka
scripts/kafka_demo.sh status
scripts/kafka_demo.sh topic
```

终端 3：启动 Kafka 生产模式，持续把订单 JSON 写入 `ecommerce-orders`。

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python generator/order_stream_generator.py \
  --sink kafka \
  --kafka-bootstrap-servers 127.0.0.1:9092 \
  --kafka-topic ecommerce-orders \
  --interval 0.5
```

如果想确认 Kafka topic 中有消息，可以临时开一个消费者观察前 5 条消息：

```bash
cd /YOURPATH/realtime-ecommerce-flink
export KAFKA_HOME=/opt/homebrew/opt/kafka
scripts/kafka_demo.sh consume
```

终端 4：启动 Kafka 输入模式的 PyFlink 作业。

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python scripts/init_db.py
python flink_jobs/order_stream_job.py \
  --source kafka \
  --kafka-bootstrap-servers 127.0.0.1:9092 \
  --kafka-topic ecommerce-orders \
  --kafka-group-id ecommerce-monitor \
  --kafka-offset latest \
  --window-seconds 10 \
  --watermark-delay-seconds 3
```

如果运行时报 Kafka connector 类不存在，说明当前 PyFlink 环境只有 Python API，没有把 Flink Kafka connector JAR 放进 Java classpath。先安装项目推荐的 PyFlink 版本并下载 connector：

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python -m pip install --force-reinstall -r requirements-flink.txt
scripts/install_flink_kafka_connector.sh
```

脚本会下载到 `lib/flink-sql-connector-kafka-4.0.0-2.0.jar`，作业启动时会自动加载它。注意不要使用 `flink-connector-kafka-*.jar` 薄 JAR，否则会缺少 Kafka Java client 依赖。也可以显式把路径传给作业：

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python flink_jobs/order_stream_job.py \
  --source kafka \
  --kafka-bootstrap-servers 127.0.0.1:9092 \
  --kafka-topic ecommerce-orders \
  --kafka-group-id ecommerce-monitor \
  --kafka-offset latest \
  --window-seconds 10 \
  --watermark-delay-seconds 3 \
  --kafka-connector-jar /YOURPATH/realtime-ecommerce-flink/lib/flink-sql-connector-kafka-4.0.0-2.0.jar
```

终端 5：启动 FastAPI 后端。

```bash
cd /YOURPATH/realtime-ecommerce-flink
conda activate pyflink
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

终端 6：启动 Vue/Vite 前端。

```bash
cd /YOURPATH/realtime-ecommerce-flink/frontend
npm install
npm run dev
```

访问：

- 完整前端页面：[http://127.0.0.1:5173](http://127.0.0.1:5173)
- 后端内置页面：[http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard)

## 数据格式

实时生成器每行输出一条 JSON 订单，例如：

```json
{
  "order_id": "O20260612103000000001",
  "user_id": "U1234",
  "product_id": "P1001",
  "product_name": "无线耳机",
  "category": "数码",
  "city": "上海",
  "amount": 899.0,
  "quantity": 1,
  "payment_status": "SUCCESS",
  "device_id": "D1234",
  "ip_address": "192.168.1.8",
  "event_time": "2026-06-12 10:30:00",
  "scenario": "NORMAL"
}
```

生成器会随机或按序列混入以下异常场景：

- `PAYMENT_FAILED`：支付失败订单
- `RISKY_DEVICE_IP`：风险设备或风险 IP
- `BURST_USER`：同一用户短时间密集下单
- `REPEATED_PAYMENT_FAILURES`：同一用户窗口内连续支付失败
- `DEVICE_HOPPING`：同一用户窗口内频繁切换设备
- `GEO_VELOCITY`：同一用户窗口内跨多个城市下单
- `SHARED_IP_USERS`：同一 IP 在窗口内关联多个用户
- `SHARED_DEVICE_USERS`：同一设备在窗口内关联多个用户
- 每 37 条订单会生成一条 1 到 3 秒乱序的 `event_time`，用于验证 Watermark 处理能力

## PyFlink 计算逻辑

`flink_jobs/order_stream_job.py` 使用 DataStream API：

- `--source socket` 使用 `socket_text_stream` 接入订单 JSON Lines
- 兼容 PyFlink 2.x：如果当前版本没有 `socket_text_stream`，自动回退到 Flink Java `SocketTextStreamFunction`
- `--source kafka` 使用 `KafkaSource` 消费 Kafka topic
- `set_python_executable(sys.executable)` 让 Python worker 使用当前 conda 环境解释器
- `map(parse_order)` 将 JSON 转为 Flink tuple
- `WatermarkStrategy.for_bounded_out_of_orderness` 按 `event_time` 提取时间戳并生成 Watermark
- `TumblingEventTimeWindows` 做滚动事件时间窗口
- 按全局、商品、城市三个维度输出窗口统计
- 订单级规则实时写入异常告警
- 用户、IP、设备维度窗口检测复合异常并写入告警

默认异常规则：

- 支付失败：`payment_status == FAILED`
- 风险设备：`device_id` 命中内置风险设备列表
- 风险 IP：`ip_address` 命中内置风险 IP 列表
- 高频用户：同一用户在一个窗口内订单数达到 `--frequency-threshold`
- 连续失败：同一用户窗口内失败支付数达到 `--failed-payment-threshold`
- 设备跳变：同一用户窗口内设备数达到 `--device-hop-threshold`
- 异地跳变：同一用户窗口内城市数达到 `--city-hop-threshold`
- 共享 IP：同一 IP 窗口内关联用户数达到 `--shared-ip-user-threshold`
- 共享设备：同一设备窗口内关联用户数达到 `--shared-device-user-threshold`

## API

- `GET /api/health`
- `GET /api/metrics/summary`
- `GET /api/metrics/sales-trend?limit=24`
- `GET /api/metrics/top-products?limit=8`
- `GET /api/metrics/city-distribution`
- `GET /api/metrics/alert-distribution`
- `GET /api/metrics/risk-trend?limit=24`
- `GET /api/orders/latest?limit=10`
- `GET /api/alerts?limit=10&risk_level=HIGH&alert_type=SHARED_IP_USERS`
- `GET /api/alerts/options`
- `GET /dashboard`

## 截图与报告

- 桌面运行截图：[docs/screenshots/dashboard-running-desktop-chrome.png](docs/screenshots/dashboard-running-desktop-chrome.png)
- 移动端运行截图：[docs/screenshots/dashboard-running-mobile.png](docs/screenshots/dashboard-running-mobile.png)

## 常见问题

如果 `import pyflink` 失败，先确认是否激活了正确环境：

```bash
conda env list
conda activate pyflink
python -m pip show apache-flink
```

如果 PyFlink 启动时报 Java 相关错误，检查 JDK：

```bash
java -version
```

如果 Flink 作业连接 socket 失败，先启动订单流生成器，再启动 Flink 作业。

如果 Kafka source 报 connector 类不存在，先执行：

```bash
python -m pip install --force-reinstall -r requirements-flink.txt
scripts/install_flink_kafka_connector.sh
```

然后重新启动 `flink_jobs/order_stream_job.py`。

如果 Vue 前端启动失败但后端已运行，可以先访问 `/dashboard` 使用内置演示页。
