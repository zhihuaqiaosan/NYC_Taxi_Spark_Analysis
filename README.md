# 🚕 NYC Taxi Trip Data Analysis with Spark

## 基于Spark的纽约出租车行程数据性能调优与可视化分析

<div align="center">

![Python](https://img.shields.io/badge/Python-3.9-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.3.2-orange.svg)
![Flask](https://img.shields.io/badge/Flask-2.3.0-green.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

</div>

## 📊 项目简介

本项目基于纽约市出租车与豪华轿车委员会（TLC）开放的2024年第一季度黄色出租车行程数据，使用PySpark进行大规模数据处理和性能调优，并通过Flask + ECharts构建可视化分析仪表板。

**数据规模**：955万行行程记录 | 约160MB（Parquet格式）

**核心成果**：通过AQE + Kryo序列化组合优化，**性能提升19.6%**（9.83秒 → 7.90秒）

## 🎯 项目亮点

| 亮点 | 说明 |
|:---|:---|
| ✅ **数据处理** | 处理955万行纽约出租车行程数据 |
| ✅ **性能调优** | 三组对比实验，验证AQE+Kryo有效性 |
| ✅ **性能提升** | **19.6%** 性能提升（9.83秒 → 7.90秒） |
| ✅ **数据可视化** | Flask + ECharts 完整仪表板 |
| ✅ **业务洞察** | 小费规律、热点区域、时段特征分析 |

## 📁 项目结构

```
NYC_Taxi_Spark_Analysis/
│
├── baseline.py                     # 基线版本（无优化）
├── export_for_web.py               # 导出JSON数据
├── optimized_v2.py                 # 优化版本V2（AQE+Kryo+缓存 → OOM）
├── optimized_v3_no_cache.py        # 优化版本V3（AQE+Kryo ✅）
│
├── web/
│   ├── app.py                      # Flask后端服务
│   ├── static/
│   │   ├── hour_data.json          # 小时统计数据
│   │   ├── location_data.json      # 热点区域数据
│   │   ├── passenger_data.json     # 乘客数小费数据
│   │   └── summary_data.json       # 汇总指标数据
│   └── templates/
│       └── index.html              # 前端页面
│
├── requirements.txt                # 依赖包列表
└── README.md                       # 项目说明
```

## 🚀 快速开始

### 环境要求

| 组件 | 版本 |
|:---|:---|
| Python | 3.9+ |
| JDK | 1.8+ |
| 内存 | 8GB+（推荐） |
| 操作系统 | Windows / Linux / macOS |

### 安装依赖

```bash
pip install -r requirements.txt
```

### 下载数据

从 [NYC TLC官网](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) 下载2024年1-3月的Parquet文件：

- `yellow_tripdata_2024-01.parquet`
- `yellow_tripdata_2024-02.parquet`
- `yellow_tripdata_2024-03.parquet`

将文件放入项目根目录。

### 运行实验

```bash
# 1. 基线版本（无优化）
python scripts/baseline.py

# 2. 优化版本V3（AQE + Kryo，推荐）
python scripts/optimized_v3_no_cache.py

# 3. 导出可视化数据
python scripts/export_for_web.py

# 4. 启动可视化服务
cd web
python app.py
```

打开浏览器访问：`http://localhost:5000`

## 📊 实验结果

### 性能对比

| 版本 | 优化策略 | 耗时 | 提升 | 状态 |
|:---:|:---|:---:|:---:|:---:|
| **V1** | 无优化（基线） | 9.83秒 | — | ✅ |
| **V2** | AQE + Kryo + 缓存 | OOM | ❌ | ❌ 失败 |
| **V3** | AQE + Kryo（无缓存） | **7.90秒** | **↑19.6%** | ✅ |

### 性能分析

```
耗时对比
12s ┤
10s ┤ ████████ 9.83s
 8s ┤ ██████ 7.90s ⬅ 快19.6%
 6s ┤
 4s ┤
 2s ┤
 0s ┴────── V1 ────── V3
```

**V2失败原因**：8GB内存无法同时容纳多个分区的缓存数据，导致内存溢出

**V3提升原因**：
- Kryo序列化比Java序列化快3-5倍
- AQE动态合并小分区，减少调度开销
- 两者组合产生协同效应

## 📈 数据分析结果

### 1. 乘客数与平均小费

| 乘客数 | 平均小费 | 行程数 | 占比 |
|:---:|:---:|:---:|:---:|
| 1人 | $3.42 | 6,819,814 | 71.4% |
| **2人** | **$3.76** | 1,262,673 | 13.2% |
| 3人 | $3.59 | 285,983 | 3.0% |
| 4人 | $3.56 | 164,590 | 1.7% |

**洞察**：2人出行时小费最高，可能与社交压力有关

### 2. 时段与平均小费

| 时段 | 平均小费 | 平均车费 | 行程数 |
|:---:|:---:|:---:|:---:|
| 0时 | **$3.68** | $19.50 | 592,224 |
| 1时 | $3.46 | $18.17 | 653,781 |
| 7时 | $3.36 | $19.62 | 385,368 |
| 13时 | $3.64 | **$25.94** | 59,884 |
| 23时 | $3.43 | $19.46 | 580,541 |

**洞察**：凌晨小费最高，可能与夜间服务溢价有关

### 3. Top5热门上车区域

| 排名 | 区域ID | 行程数 | 累计占比 |
|:---:|:---:|:---:|:---:|
| 1 | 161 | 453,826 | 4.75% |
| 2 | 237 | 439,139 | 9.35% |
| 3 | 132 | 429,746 | 13.85% |
| 4 | 236 | 416,509 | 18.21% |
| 5 | 162 | 336,460 | **21.73%** |

**洞察**：Top5区域集中在曼哈顿核心区，占总量21.73%

## 🎨 可视化展示

启动服务后访问 `http://localhost:5000` 查看：

| 模块 | 图表类型 | 展示内容 |
|:---|:---|:---|
| 统计卡片 | 数值 | 总行程数、平均小费、平均车费、平均距离 |
| 趋势分析 | 折线图+柱状图 | 24小时小费、车费、行程数 |
| 热门区域 | 柱状图 | Top10上车区域 |
| 乘客分析 | 柱状图 | 不同乘客数平均小费 |
| 关系分析 | 散点图 | 小费与车费关系 |

## 🔧 调优策略说明

| 策略 | 配置 | 作用 |
|:---|:---|:---|
| **AQE** | `spark.sql.adaptive.enabled=true` | 动态合并分区，优化执行计划 |
| **Kryo** | `spark.serializer=KryoSerializer` | 比Java序列化快3-5倍 |
| **分区优化** | `spark.sql.shuffle.partitions=8` | 减少小分区开销 |
| **内存配置** | `spark.executor.memory=1g` | 单机环境谨慎设置 |

## 📝 实验总结

### 完成情况

| 任务 | 状态 |
|:---|:---:|
| 数据处理 | ✅ 完成955万行数据读取与清洗 |
| 性能优化 | ✅ 完成三组对比实验 |
| 结果分析 | ✅ 得出有效分析结论 |
| 可视化 | ✅ 完成仪表板搭建 |

### 主要收获

1. 掌握了PySpark处理大规模数据的基本流程
2. 理解了AQE、Kryo、缓存等优化策略的适用场景
3. 认识到性能优化必须结合实际资源配置
4. 学会了使用Flask + ECharts构建可视化仪表板

### 改进方向

1. 部署到集群环境，测试分布式场景下的优化效果
2. 扩展到全年数据，分析季节性规律
3. 加入机器学习模型预测小费或行程需求

## 📚 参考资料

- [NYC TLC Open Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Flask Documentation](https://flask.palletsprojects.com/)
- [ECharts Documentation](https://echarts.apache.org/)
```

