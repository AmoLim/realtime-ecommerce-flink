# API 接口参考文档

## 概述

本系统提供 RESTful API 接口，基于 FastAPI 框架构建。所有接口返回 JSON 格式数据，基础路径为 `http://127.0.0.1:8000`。

## 数据模型

### 订单 (Order)

| 字段             | 类型   | 说明                       |
| ---------------- | ------ | -------------------------- |
| order_id         | string | 订单唯一标识               |
| user_id          | string | 用户 ID                    |
| product_id       | string | 商品 ID                    |
| product_name     | string | 商品名称                   |
| category         | string | 商品分类                   |
| city             | string | 下单城市                   |
| amount           | float  | 订单金额                   |
| quantity         | int    | 购买数量                   |
| payment_status   | string | 支付状态 (SUCCESS / FAILED) |
| device_id        | string | 设备 ID                    |
| ip_address       | string | IP 地址                    |
| event_time       | string | 事件时间 (yyyy-MM-dd HH:mm:ss) |
| scenario         | string | 场景标签                   |

### 告警 (Alert)

| 字段        | 类型   | 说明                                |
| ----------- | ------ | ----------------------------------- |
| id          | int    | 告警唯一 ID                         |
| alert_type  | string | 告警类型                            |
| risk_level  | string | 风险等级 (LOW / MEDIUM / HIGH)       |
| order_id    | string | 关联订单 ID                         |
| user_id     | string | 关联用户 ID                         |
| description | string | 告警描述                            |
| created_at  | string | 创建时间                            |

---

## 接口列表

### 1. 健康检查

**GET** `/api/health`

检查后端服务是否正常运行。

**响应示例：**

```json
{
  "status": "ok",
  "timestamp": "2026-06-12 10:30:00"
}
```

---

### 2. 指标汇总

**GET** `/api/metrics/summary`

获取系统核心指标汇总，包括总订单数、总销售额、告警数量等。

**响应示例：**

```json
{
  "total_orders": 15234,
  "total_revenue": 1289600.50,
  "total_alerts": 127,
  "active_users": 892,
  "avg_order_amount": 84.65
}
```

---

### 3. 销售趋势

**GET** `/api/metrics/sales-trend?limit=24`

获取最近 N 个时间窗口的销售趋势数据，用于折线图展示。

**请求参数：**

| 参数  | 类型 | 默认值 | 说明               |
| ----- | ---- | ------ | ------------------ |
| limit | int  | 24     | 返回最近 N 条记录  |

**响应示例：**

```json
[
  {
    "window_start": "2026-06-12 10:00:00",
    "window_end": "2026-06-12 10:00:10",
    "total_amount": 12800.50,
    "order_count": 45
  },
  {
    "window_start": "2026-06-12 09:59:50",
    "window_end": "2026-06-12 10:00:00",
    "total_amount": 11500.00,
    "order_count": 42
  }
]
```

---

### 4. 热门商品

**GET** `/api/metrics/top-products?limit=8`

获取销售额最高的前 N 个商品排行。

**请求参数：**

| 参数  | 类型 | 默认值 | 说明               |
| ----- | ---- | ------ | ------------------ |
| limit | int  | 8      | 返回前 N 个商品    |

**响应示例：**

```json
[
  {
    "product_id": "P1001",
    "product_name": "无线耳机",
    "category": "数码",
    "total_amount": 125000.00,
    "order_count": 230
  },
  {
    "product_id": "P2005",
    "product_name": "运动鞋",
    "category": "服饰",
    "total_amount": 98000.00,
    "order_count": 185
  }
]
```

---

### 5. 城市销售分布

**GET** `/api/metrics/city-distribution`

获取各城市的销售统计数据，用于地图或饼图展示。

**响应示例：**

```json
[
  {
    "city": "上海",
    "total_amount": 450000.00,
    "order_count": 1200
  },
  {
    "city": "北京",
    "total_amount": 380000.00,
    "order_count": 1050
  },
  {
    "city": "深圳",
    "total_amount": 320000.00,
    "order_count": 890
  }
]
```

---

### 6. 告警分布

**GET** `/api/metrics/alert-distribution`

获取各类告警的数量分布统计。

**响应示例：**

```json
[
  {
    "alert_type": "PAYMENT_FAILED",
    "count": 45
  },
  {
    "alert_type": "BURST_USER",
    "count": 32
  },
  {
    "alert_type": "DEVICE_HOPPING",
    "count": 18
  },
  {
    "alert_type": "GEO_VELOCITY",
    "count": 12
  }
]
```

---

### 7. 风险趋势

**GET** `/api/metrics/risk-trend?limit=24`

获取最近 N 个时间窗口的风险趋势数据。

**请求参数：**

| 参数  | 类型 | 默认值 | 说明               |
| ----- | ---- | ------ | ------------------ |
| limit | int  | 24     | 返回最近 N 条记录  |

**响应示例：**

```json
[
  {
    "window_start": "2026-06-12 10:00:00",
    "window_end": "2026-06-12 10:00:10",
    "high_risk_count": 3,
    "medium_risk_count": 8,
    "low_risk_count": 15
  }
]
```

---

### 8. 最新订单

**GET** `/api/orders/latest?limit=10`

获取最近的 N 条订单记录。

**请求参数：**

| 参数  | 类型 | 默认值 | 说明               |
| ----- | ---- | ------ | ------------------ |
| limit | int  | 10     | 返回最近 N 条订单  |

**响应示例：**

```json
[
  {
    "order_id": "O20260612103000000001",
    "user_id": "U1234",
    "product_name": "无线耳机",
    "category": "数码",
    "city": "上海",
    "amount": 899.00,
    "payment_status": "SUCCESS",
    "event_time": "2026-06-12 10:30:00",
    "scenario": "NORMAL"
  }
]
```

---

### 9. 告警列表

**GET** `/api/alerts?limit=10&risk_level=HIGH&alert_type=SHARED_IP_USERS`

获取告警记录，支持按风险等级和告警类型筛选。

**请求参数：**

| 参数       | 类型   | 默认值 | 说明                                        |
| ---------- | ------ | ------ | ------------------------------------------- |
| limit      | int    | 10     | 返回最近 N 条告警                           |
| risk_level | string | 无     | 按风险等级筛选 (LOW / MEDIUM / HIGH)         |
| alert_type | string | 无     | 按告警类型筛选                              |

**响应示例：**

```json
[
  {
    "id": 1,
    "alert_type": "SHARED_IP_USERS",
    "risk_level": "HIGH",
    "order_id": "O20260612103000000042",
    "user_id": "U5678",
    "description": "IP 192.168.1.100 在窗口内关联了 3 个不同用户",
    "created_at": "2026-06-12 10:30:10"
  }
]
```

---

### 10. 告警选项

**GET** `/api/alerts/options`

获取可用的告警类型和风险等级列表，用于前端筛选下拉框。

**响应示例：**

```json
{
  "alert_types": [
    "PAYMENT_FAILED",
    "RISKY_DEVICE_IP",
    "BURST_USER",
    "REPEATED_PAYMENT_FAILURES",
    "DEVICE_HOPPING",
    "GEO_VELOCITY",
    "SHARED_IP_USERS",
    "SHARED_DEVICE_USERS"
  ],
  "risk_levels": ["LOW", "MEDIUM", "HIGH"]
}
```

---

## 错误码

| HTTP 状态码 | 说明                        |
| ----------- | --------------------------- |
| 200         | 请求成功                    |
| 400         | 请求参数错误                |
| 404         | 资源不存在                  |
| 500         | 服务器内部错误，需查看日志  |

## 异常检测规则说明

系统在 PyFlink DataStream 作业中实时检测以下异常模式：

| 异常类型                  | 检测逻辑                                                       | 默认阈值 |
| ------------------------- | -------------------------------------------------------------- | -------- |
| PAYMENT_FAILED            | `payment_status == "FAILED"`                                   | —        |
| RISKY_DEVICE_IP           | 设备 ID 或 IP 命中内置风险列表                                   | —        |
| BURST_USER                | 同一用户在时间窗口内下单数 ≥ 阈值                                | 6        |
| REPEATED_PAYMENT_FAILURES | 同一用户在时间窗口内支付失败数 ≥ 阈值                            | 3        |
| DEVICE_HOPPING            | 同一用户在时间窗口内使用设备数 ≥ 阈值                            | 3        |
| GEO_VELOCITY              | 同一用户在时间窗口内出现城市数 ≥ 阈值                            | 3        |
| SHARED_IP_USERS           | 同一 IP 在时间窗口内关联用户数 ≥ 阈值                            | 3        |
| SHARED_DEVICE_USERS       | 同一设备在时间窗口内关联用户数 ≥ 阈值                            | 3        |

> 以上阈值可通过 PyFlink 作业的命令行参数进行调整。详见 [README.md](../README.md) 中的 PyFlink 计算逻辑章节。
