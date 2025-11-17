from datetime import datetime
from collections.abc import Callable
from typing import cast, Iterator, Sequence, Any
import taosws
import pandas as pd
import re
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    TZ_INFO,
)
from vnpy.trader.setting import SETTINGS
from .tdengine_script import (
    CREATE_DATABASE_SCRIPT,
    CREATE_BAR_TABLE_SCRIPT,
    CREATE_TICK_TABLE_SCRIPT,
)

def sanitize_symbol(symbol: str) -> str:
    """
    修改symbol用于表名
    """
    symbol = re.sub(r"[-./]", "_", symbol)
    return symbol

def parse_datetime(dt_value) -> datetime:
    """解析datetime值，统一转换为datetime对象"""
    if dt_value is None:
        return None
    if isinstance(dt_value, datetime):
        return dt_value
    if isinstance(dt_value, str):
        try:
            # 尝试解析ISO格式的时间字符串
            return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
        except:
            try:
                # 尝试解析其他常见格式
                return pd.to_datetime(dt_value).to_pydatetime()
            except:
                return None
    if hasattr(dt_value, 'to_pydatetime'):
        return dt_value.to_pydatetime()
    return dt_value

class TdEngineDatabase(BaseDatabase):
    """TDengine数据库接口（使用taosws）"""
    
    def __init__(self) -> None:
        """构造函数"""
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = 6041 #SETTINGS["database.port"]
        self.database: str = SETTINGS["database.database"]
        
        # 连接数据库
        self.conn: taosws.Connection = taosws.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
        )
        
        # 初始化创建数据库和数据表
        self.conn.execute(CREATE_DATABASE_SCRIPT.format(self.database))
        self.conn.execute(f"use {self.database}")
        self.conn.execute(CREATE_BAR_TABLE_SCRIPT)
        self.conn.execute(CREATE_TICK_TABLE_SCRIPT)

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存k线数据"""
        # 缓存字段参数
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval = bar.interval
        count: int = 0
        table_name: str = "_".join(["bar", sanitize_symbol(symbol), exchange.value, interval.value])
        
        # 以超级表为模版创建表
        create_table_script: str = (
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            "USING s_bar(symbol, exchange, interval_, count_) "
            f"TAGS('{symbol}', '{exchange.value}', '{interval.value}', '{count}')"
        )
        self.conn.execute(create_table_script)
        
        # 写入k线数据
        self.insert_in_batch(table_name, bars, 1000)
        
        # 查询汇总信息
        try:
            result = self.conn.query(f"SELECT start_time, end_time, count_ FROM {table_name}")
            results: list[tuple] = list(result)
            if results:
                overview: tuple = results[0]
                overview_start = parse_datetime(overview[0])
                overview_end = parse_datetime(overview[1])
                overview_count: int = int(overview[2]) if overview[2] is not None else 0
            else:
                overview_start = None
                overview_end = None
                overview_count = 0
        except Exception:
            # 如果查询失败，说明表为空或没有汇总信息
            overview_start = None
            overview_end = None
            overview_count = 0
        
        # 没有该合约或首次写入
        if not overview_count or overview_start is None:
            overview_start = bars[0].datetime
            overview_end = bars[-1].datetime
            overview_count = len(bars)
        # 已有该合约
        elif stream:
            overview_end = bars[-1].datetime
            overview_count += len(bars)
        else:
            overview_start = min(overview_start, bars[0].datetime)
            overview_end = max(overview_end, bars[-1].datetime)
            try:
                result = self.conn.query(f"select count(*) from {table_name}")
                results = list(result)
                bar_count: int = int(results[0][0])
                overview_count = bar_count
            except Exception:
                overview_count = len(bars)
        
        # 更新汇总信息
        self.conn.execute(f"ALTER TABLE {table_name} SET TAG start_time='{overview_start}';")
        self.conn.execute(f"ALTER TABLE {table_name} SET TAG end_time='{overview_end}';")
        self.conn.execute(f"ALTER TABLE {table_name} SET TAG count_='{overview_count}';")
        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存tick数据"""
        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange
        count: int = 0
        table_name: str = "_".join(["tick", sanitize_symbol(symbol), exchange.value])
        
        # 以超级表为模版创建表
        create_table_script: str = (
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            "USING s_tick(symbol, exchange, count_) "
            f"TAGS ( '{symbol}', '{exchange.value}', '{count}')"
        )
        self.conn.execute(create_table_script)
        
        # 写入tick数据
        self.insert_in_batch(table_name, ticks, 1000)
        
        # 查询汇总信息
        try:
            result = self.conn.query(f"SELECT start_time, end_time, count_ FROM {table_name}")
            results: list[tuple] = list(result)
            if results:
                overview: tuple = results[0]
                overview_start = parse_datetime(overview[0])
                overview_end = parse_datetime(overview[1])
                overview_count: int = int(overview[2]) if overview[2] is not None else 0
            else:
                overview_start = None
                overview_end = None
                overview_count = 0
        except Exception:
            overview_start = None
            overview_end = None
            overview_count = 0
        
        # 没有该合约或首次写入
        if not overview_count or overview_start is None:
            overview_start = ticks[0].datetime
            overview_end = ticks[-1].datetime
            overview_count = len(ticks)
        # 已有该合约
        elif stream:
            overview_end = ticks[-1].datetime
            overview_count += len(ticks)
        else:
            overview_start = min(overview_start, ticks[0].datetime)
            overview_end = max(overview_end, ticks[-1].datetime)
            try:
                result = self.conn.query(f"select count(*) from {table_name}")
                results = list(result)
                tick_count: int = int(results[0][0])
                overview_count = tick_count
            except Exception:
                overview_count = len(ticks)
        
        # 更新汇总信息
        self.conn.execute(f"ALTER TABLE {table_name} SET TAG start_time='{overview_start}';")
        self.conn.execute(f"ALTER TABLE {table_name} SET TAG end_time='{overview_end}';")
        self.conn.execute(f"ALTER TABLE {table_name} SET TAG count_='{overview_count}';")
        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """读取K线数据"""
        # 生成数据表名
        table_name: str = "_".join(["bar", sanitize_symbol(symbol), exchange.value, interval.value])
        
        # 从数据库读取数据
        sql = f"select *, interval_ from {table_name} WHERE datetime BETWEEN '{start}' AND '{end}'"
        try:
            result = self.conn.query(sql)
            data = list(result)
        except Exception:
            return []
        
        if not data:
            return []
        
        # 返回BarData列表
        bars: list[BarData] = []
        for row in data:
            try:
                # 数据顺序为: datetime, volume, open_interest, open_price, high_price, low_price, close_price, interval_
                dt = parse_datetime(row[0])
                if dt and hasattr(dt, 'astimezone'):
                    dt = dt.astimezone(TZ_INFO)
                
                bar: BarData = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    datetime=dt,
                    interval=Interval(row[7]),  # interval_字段
                    volume=row[1],
                    open_interest=row[2],
                    open_price=row[3],
                    high_price=row[4],
                    low_price=row[5],
                    close_price=row[6],
                    gateway_name="DATABASE"
                )
                bars.append(bar)
            except Exception as e:
                print(f"Error processing bar data row: {row}, error: {e}")
                continue
        
        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TickData]:
        """读取tick数据"""
        # 生成数据表名
        table_name: str = "_".join(["tick", sanitize_symbol(symbol), exchange.value])
        
        # 从数据库读取数据
        sql = f"select * from {table_name} WHERE datetime BETWEEN '{start}' AND '{end}'"
        try:
            result = self.conn.query(sql)
            data = list(result)
        except Exception:
            return []
        
        if not data:
            return []
        
        # 返回TickData列表
        ticks: list[TickData] = []
        for row in data:
            try:
                dt = parse_datetime(row[0])
                if dt and hasattr(dt, 'astimezone'):
                    dt = dt.astimezone(TZ_INFO)
                
                tick: TickData = TickData(
                    symbol=symbol,
                    exchange=exchange,
                    datetime=dt,
                    name=row[1],
                    volume=row[2],
                    open_interest=row[3],
                    last_price=row[4],
                    last_volume=row[5],
                    limit_up=row[6],
                    limit_down=row[7],
                    open_price=row[8],
                    high_price=row[9],
                    low_price=row[10],
                    pre_close=row[11],
                    bid_price_1=row[12],
                    bid_price_2=row[13],
                    bid_price_3=row[14],
                    bid_price_4=row[15],
                    bid_price_5=row[16],
                    ask_price_1=row[17],
                    ask_price_2=row[18],
                    ask_price_3=row[19],
                    ask_price_4=row[20],
                    ask_price_5=row[21],
                    bid_volume_1=row[22],
                    bid_volume_2=row[23],
                    bid_volume_3=row[24],
                    bid_volume_4=row[25],
                    bid_volume_5=row[26],
                    ask_volume_1=row[27],
                    ask_volume_2=row[28],
                    ask_volume_3=row[29],
                    ask_volume_4=row[30],
                    ask_volume_5=row[31],
                    gateway_name="DATABASE"
                )
                ticks.append(tick)
            except Exception as e:
                print(f"Error processing tick data row: {row}, error: {e}")
                continue
        
        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        end: "datetime"
    ) -> int:
        """删除K线数据"""
        # 生成数据表名
        table_name: str = "_".join(["bar", sanitize_symbol(symbol), exchange.value, interval.value])
        
        # 查询数据条数
        try:
            result = self.conn.query(f"select count(*) from {table_name} where datetime < '{end}'")
            result_list: list = list(result)
            count: int = int(result_list[0][0])
        except Exception:
            count = 0
        
        # 执行K线删除
        try:
            self.conn.execute(f"DELETE FROM {table_name} WHERE datetime < '{end}'")
        except Exception:
            pass
        
        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        end: "datetime"
    ) -> int:
        """删除tick数据"""
        # 生成数据表名
        table_name: str = "_".join(["tick", sanitize_symbol(symbol), exchange.value])
        
        # 查询数据条数
        try:
            result = self.conn.query(f"select count(*) from {table_name} where datetime < '{end}'")
            result_list: list = list(result)
            count: int = int(result_list[0][0])
        except Exception:
            count = 0
        
        # 删除tick数据
        try:
            self.conn.execute(f"DELETE FROM {table_name} WHERE datetime < '{end}'")
        except Exception:
            pass
        
        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """查询K线汇总信息"""
        try:
            # 从数据库读取数据
            result = self.conn.query("SHOW TABLE TAGS FROM s_bar")
            data = list(result)
            
            # 返回BarOverview列表
            overviews: list[BarOverview] = []
            for row in data:
                try:
                    # 根据实际的SHOW TAGS输出调整索引
                    overview: BarOverview = BarOverview(
                        symbol=row[1],  # symbol
                        exchange=Exchange(row[2]),  # exchange
                        interval=Interval(row[3]),  # interval_
                        start=parse_datetime(row[4]),  # start_time
                        end=parse_datetime(row[5]),  # end_time
                        count=int(row[6]) if row[6] is not None else 0,  # count_
                    )
                    if overview.start:
                        overview.start = overview.start.astimezone(TZ_INFO)
                    if overview.end:
                        overview.end = overview.end.astimezone(TZ_INFO)
                    overviews.append(overview)
                except Exception as e:
                    print(f"Error processing bar overview row: {row}, error: {e}")
                    continue
            
            return overviews
        except Exception:
            return []

    def get_tick_overview(self) -> list[TickOverview]:
        """查询Tick汇总信息"""
        try:
            # 从数据库读取数据
            result = self.conn.query("SHOW TABLE TAGS FROM s_tick")
            data = list(result)
            
            # 返回TickOverview列表
            overviews: list[TickOverview] = []
            for row in data:
                try:
                    overview: TickOverview = TickOverview(
                        symbol=row[1],  # symbol
                        exchange=Exchange(row[2]),  # exchange
                        start=parse_datetime(row[3]),  # start_time
                        end=parse_datetime(row[4]),  # end_time
                        count=int(row[5]) if row[5] is not None else 0,  # count_
                    )
                    if overview.start:
                        overview.start = overview.start.astimezone(TZ_INFO)
                    if overview.end:
                        overview.end = overview.end.astimezone(TZ_INFO)
                    overviews.append(overview)
                except Exception as e:
                    print(f"Error processing tick overview row: {row}, error: {e}")
                    continue
            
            return overviews
        except Exception:
            return []

    def insert_in_batch(self, table_name: str, data_set: list, batch_size: int) -> None:
        """数据批量插入数据库"""
        if table_name.split("_")[0] == "bar":
            generate: Callable = generate_bar
        else:
            generate = generate_tick
        
        data: list[str] = [f"insert into {table_name} values"]
        count: int = 0
        for d in data_set:
            data.append(generate(d))
            count += 1
            if count == batch_size:
                try:
                    self.conn.execute(" ".join(data))
                except Exception as e:
                    print(f"Error executing batch insert: {e}")
                data = [f"insert into {table_name} values"]
                count = 0
        if count != 0:
            try:
                self.conn.execute(" ".join(data))
            except Exception as e:
                print(f"Error executing final batch insert: {e}")

    def execute_sql(self, sql: str) -> dict:
        """执行SQL并返回结果字典格式"""
        try:
            result = self.conn.query(sql)
            data = list(result)
            return {"data": data}
        except Exception:
            return {"data": []}

    def stream_query_rows(
        self,
        sql: str,
        batch_size: int = 10000,
    ) -> Iterator[Sequence[Any]]:
        """
        以流式方式执行 SELECT 语句，按批量分页返回 rows。
        注意：
        - sql 不要包含 LIMIT / OFFSET，自带的会在这里统一加上
        - 只适用于 SELECT/SHOW 这类有结果集的语句
        - 内部使用 LIMIT ... OFFSET 分页，不会一次性把所有 rows 全拉进内存
        """
        base_sql = sql.strip().rstrip(";")   # 去掉多余空格和末尾分号
        offset = 0
        while True:
            page_sql = f"{base_sql} LIMIT {batch_size} OFFSET {offset}"
            data = self.execute_sql(page_sql)
            if not data:
                break
            rows = data.get("data") or []
            if not rows:
                break
            for row in rows:
                yield row
            # 如果本批次不足 batch_size，说明已经到最后一页了
            if len(rows) < batch_size:
                break
            offset += len(rows)

    def close(self) -> None:
        """关闭数据库连接"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def __del__(self):
        """析构函数"""
        self.close()

def generate_bar(bar: BarData) -> str:
    """将BarData转换为可存储的字符串"""
    result: str = (f"('{bar.datetime}', {bar.volume}, {bar.open_interest},"
                   + f"{bar.open_price}, {bar.high_price}, {bar.low_price}, {bar.close_price})")
    return result

def generate_tick(tick: TickData) -> str:
    """将TickData转换为可存储的字符串"""
    result: str = (f"('{tick.datetime}', '{tick.name}', {tick.volume}, "
                   + f"{tick.open_interest}, {tick.last_price}, {tick.last_volume}, "
                   + f"{tick.limit_up}, {tick.limit_down}, {tick.open_price}, {tick.high_price}, {tick.low_price}, {tick.pre_close}, "
                   + f"{tick.bid_price_1}, {tick.bid_price_2}, {tick.bid_price_3}, {tick.bid_price_4}, {tick.bid_price_5}, "
                   + f"{tick.ask_price_1}, {tick.ask_price_2}, {tick.ask_price_3}, {tick.ask_price_4}, {tick.ask_price_5}, "
                   + f"{tick.bid_volume_1}, {tick.bid_volume_2}, {tick.bid_volume_3}, {tick.bid_volume_4}, {tick.bid_volume_5}, "
                   + f"{tick.ask_volume_1}, {tick.ask_volume_2}, {tick.ask_volume_3}, {tick.ask_volume_4}, {tick.ask_volume_5})")
    return result
