# 基于 PyFlink 的实时电商订单监控与异常交易预警系统

本项目实现一个本地可运行的实时电商监控闭环：

```text
Python 订单流生成器 -> PyFlink 实时窗口计算与异常检测 -> SQLite -> FastAPI -> Vue + ECharts 大屏
```

## 功能

- Python socket 数据生成器持续输出 JSON Lines 订单流
- PyFlink DataStream 读取实时流，执行处理时间滚动窗口统计
- SQLite 保存原始订单、销售窗口、商品窗口、城市窗口和异常告警
- FastAPI 提供监控接口和内置演示页
- Vue + ECharts 展示实时销售额、热门商品、城市销售分布、最新订单和异常告警

## 目录结构

```text
realtime-ecommerce-flink/
  backend/                  FastAPI 后端和 SQLite 初始化
  data/                     SQLite 数据库存放目录
  flink_jobs/               PyFlink 实时处理任务
  generator/                实时订单流生成器
  scripts/                  初始化数据库和写入演示数据脚本
  frontend/                 Vue/Vite 前端和 standalone 演示页
  requirements.txt          FastAPI 后端依赖
  requirements-flink.txt    PyFlink 依赖范围
```

## 环境

建议使用：

- Python 3.10 或 3.11
- JDK 11 或 17
- Node.js 18+，仅完整 Vue/Vite 前端需要
- SQLite 使用 Python 标准库，无需单独安装

如果已经有 `pyflink` conda 环境，可以直接进入项目目录：

```bash
cd /Users/amo/Documents/Coding/Code/BigData/realtime-ecommerce-flink
conda activate pyflink
python -c "import pyflink; print(pyflink.__file__)"
```

安装后端依赖：

```bash
pip install -r requirements.txt
```

如果环境里还没有 PyFlink：

```bash
pip install -r requirements-flink.txt
```

## 快速演示

这一步不需要启动 PyFlink，适合先验证后端、数据库和页面。

```bash
cd /Users/amo/Documents/Coding/Code/BigData/realtime-ecommerce-flink
conda activate pyflink
pip install -r requirements.txt
python scripts/init_db.py
python scripts/seed_demo_data.py
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

访问：

- 后端健康检查：[http://127.0.0.1:8000/api/health](http://127.0.0.1:8000/api/health)
- 内置 Vue 演示页：[http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard)

`seed_demo_data.py` 默认生成 120 条订单、24 个窗口统计，并写入多种异常告警。数据来自项目内的 Python 生成器，不依赖外部数据集。

## 真实实时链路

打开多个终端，均先进入项目目录并激活 conda 环境：

```bash
cd /Users/amo/Documents/Coding/Code/BigData/realtime-ecommerce-flink
conda activate pyflink
```

终端 1：启动订单流生成器。

```bash
python generator/order_stream_generator.py --host 127.0.0.1 --port 9999 --interval 0.5
```

如果不想在终端持续打印每条订单，可以追加 `--quiet`。

终端 2：启动 PyFlink 作业。

```bash
python flink_jobs/order_stream_job.py --host 127.0.0.1 --port 9999 --window-seconds 10 --frequency-threshold 6
```

终端 3：启动 FastAPI。

```bash
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

终端 4：启动完整 Vue/Vite 前端。

```bash
cd frontend
npm install
npm run dev
```

访问：[http://127.0.0.1:5173](http://127.0.0.1:5173)

如果本机暂时没有 Node.js/npm，可以继续使用 FastAPI 内置页面：

```text
http://127.0.0.1:8000/dashboard
```

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

生成器会随机混入以下异常场景：

- `HIGH_AMOUNT`：单笔大额订单
- `PAYMENT_FAILED`：支付失败订单
- `RISKY_DEVICE_IP`：风险设备或风险 IP
- `BURST_USER`：同一用户短时间密集下单

## PyFlink 计算逻辑

`flink_jobs/order_stream_job.py` 使用 DataStream API：

- `socket_text_stream` 接入订单 JSON Lines
- 兼容 PyFlink 2.x：如果当前版本没有 `socket_text_stream`，自动回退到 Flink Java `SocketTextStreamFunction`
- `set_python_executable(sys.executable)` 让 Python worker 使用当前 conda 环境解释器
- `map(parse_order)` 将 JSON 转为 Flink tuple
- `TumblingProcessingTimeWindows` 做滚动处理时间窗口
- 按全局、商品、城市三个维度输出窗口统计
- 订单级规则实时写入异常告警
- 用户维度窗口检测高频下单并写入告警

默认异常规则：

- 大额订单：`amount >= 5000`
- 支付失败：`payment_status == FAILED`
- 风险设备：`device_id` 命中内置风险设备列表
- 风险 IP：`ip_address` 命中内置风险 IP 列表
- 高频用户：同一用户在一个窗口内订单数达到 `--frequency-threshold`

## API

- `GET /api/health`
- `GET /api/metrics/summary`
- `GET /api/metrics/sales-trend?limit=24`
- `GET /api/metrics/top-products?limit=8`
- `GET /api/metrics/city-distribution`
- `GET /api/orders/latest?limit=10`
- `GET /api/alerts?limit=10`
- `GET /dashboard`

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

如果 Vue 前端启动失败但后端已运行，可以先访问 `/dashboard` 使用内置演示页。
