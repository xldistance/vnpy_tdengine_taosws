
# 使用 `taos-ws-py` 库及导出 CSV 数据

## 安装 `taos-ws-py`
首先，使用以下命令安装 `taos-ws-py` 库：

```bash
pip install taos-ws-py
````

## 在 Windows 上使用

该库在 Windows 上可以正常使用。

## 导出 CSV 数据

可以使用以下命令将数据导出为 CSV 文件：

```bash
taos -h localhost -P 6030 -u root -pxxx -s "select * from vnpy.s_bar where datetime >= '2025-11-10' >> H:/Desktop/tddata_1m.csv;"
```

**注意**：

* 密码需要紧跟在 `-p` 后面，且无空格。

