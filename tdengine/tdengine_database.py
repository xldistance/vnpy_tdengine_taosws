from datetime import datetime
from collections.abc import Callable
from typing import Iterator, Sequence, Any
from collections import defaultdict
import taosws
import pandas as pd
import re
import threading
import time
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
    """修改symbol用于表名"""
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
            return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
        except:
            try:
                return pd.to_datetime(dt_value).to_pydatetime()
            except:
                return None
    if hasattr(dt_value, 'to_pydatetime'):
        return dt_value.to_pydatetime()
    return dt_value


class TdEngineDatabase(BaseDatabase):
    """TDengine数据库接口（使用taosws）- 优化版"""
    
    def __init__(self) -> None:
        """构造函数"""
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = 6041
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
        
        # 用于缓存已创建的表，避免重复CREATE TABLE检查
        self._table_cache = set()
        self._table_cache_lock = threading.Lock()
        
        # 需要更新统计信息的表队列
        self._pending_bar_updates = set()
        self._pending_tick_updates = set()
        self._update_lock = threading.Lock()
        
        # 启动定时更新线程
        self._stop_update_thread = False
        self._update_thread = threading.Thread(
            target=self._update_overview_loop,
            daemon=True
        )
        self._update_thread.start()
        
    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存k线数据（高性能多表批量写入）"""
        if not bars:
            return True
        
        # 按(symbol, exchange, interval)分组
        grouped_bars = defaultdict(list)
        for bar in bars:
            key = (bar.symbol, bar.exchange, bar.interval)
            grouped_bars[key].append(bar)
        
        # 确保所有表已创建
        self._ensure_bar_tables_exist(grouped_bars.keys())
        
        # 使用多表插入语法批量写入
        success = self._batch_insert_bars(grouped_bars)
        
        # 标记需要更新统计信息的表
        if success:
            with self._update_lock:
                self._pending_bar_updates.update(grouped_bars.keys())
        
        return success
    
    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存tick数据（高性能多表批量写入）"""
        if not ticks:
            return True
        
        # 按(symbol, exchange)分组
        grouped_ticks = defaultdict(list)
        for tick in ticks:
            key = (tick.symbol, tick.exchange)
            grouped_ticks[key].append(tick)
        
        # 确保所有表已创建
        self._ensure_tick_tables_exist(grouped_ticks.keys())
        
        # 使用多表插入语法批量写入
        success = self._batch_insert_ticks(grouped_ticks)
        
        # 标记需要更新统计信息的表
        if success:
            with self._update_lock:
                self._pending_tick_updates.update(grouped_ticks.keys())
        
        return success
    
    def _ensure_bar_tables_exist(self, table_keys: set) -> None:
        """批量创建bar表（带缓存优化）"""
        tables_to_create = []
        
        with self._table_cache_lock:
            for symbol, exchange, interval in table_keys:
                table_name = self._get_bar_table_name(symbol, exchange, interval)
                if table_name not in self._table_cache:
                    tables_to_create.append((table_name, symbol, exchange, interval))
                    self._table_cache.add(table_name)
        
        # 批量创建表（修复：使用NOW()作为初始时间戳）
        for table_name, symbol, exchange, interval in tables_to_create:
            try:
                sql = (
                    f"CREATE TABLE IF NOT EXISTS {table_name} "
                    f"USING s_bar TAGS("
                    f"'{symbol}', '{exchange.value}', '{interval.value}', "
                    f"NOW(), NOW(), 0)"
                )
                self.conn.execute(sql)
            except Exception as e:
                print(f"创建表失败 {table_name}: {e}")
    
    def _ensure_tick_tables_exist(self, table_keys: set) -> None:
        """批量创建tick表（带缓存优化）"""
        tables_to_create = []
        
        with self._table_cache_lock:
            for symbol, exchange in table_keys:
                table_name = self._get_tick_table_name(symbol, exchange)
                if table_name not in self._table_cache:
                    tables_to_create.append((table_name, symbol, exchange))
                    self._table_cache.add(table_name)
        
        # 批量创建表
        for table_name, symbol, exchange in tables_to_create:
            try:
                sql = (
                    f"CREATE TABLE IF NOT EXISTS {table_name} "
                    f"USING s_tick TAGS("
                    f"'{symbol}', '{exchange.value}', "
                    f"NOW(), NOW(), 0)"
                )
                self.conn.execute(sql)
            except Exception as e:
                print(f"创建表失败 {table_name}: {e}")
    
    def _batch_insert_bars(self, grouped_bars: dict) -> bool:
        """使用TDengine多表插入语法批量写入bar数据"""
        max_sql_size = 800000
        max_tables_per_batch = 100
        
        try:
            current_sql_parts = []
            current_size = 0
            table_count = 0
            
            for (symbol, exchange, interval), bars in grouped_bars.items():
                table_name = self._get_bar_table_name(symbol, exchange, interval)
                
                # 生成VALUES部分
                values_list = []
                for bar in bars:
                    value_str = (
                        f"('{bar.datetime}',{bar.volume},{bar.open_interest},"
                        f"{bar.open_price},{bar.high_price},{bar.low_price},{bar.close_price})"
                    )
                    values_list.append(value_str)
                
                table_sql = f"{table_name} VALUES {' '.join(values_list)}"
                table_sql_size = len(table_sql)
                
                if (current_size + table_sql_size > max_sql_size or 
                    table_count >= max_tables_per_batch) and current_sql_parts:
                    self._execute_multi_insert(current_sql_parts)
                    current_sql_parts = []
                    current_size = 0
                    table_count = 0
                
                current_sql_parts.append(table_sql)
                current_size += table_sql_size
                table_count += 1
            
            if current_sql_parts:
                self._execute_multi_insert(current_sql_parts)
            
            return True
            
        except Exception as e:
            print(f"批量插入bar数据失败: {e}")
            return False
    
    def _batch_insert_ticks(self, grouped_ticks: dict) -> bool:
        """使用TDengine多表插入语法批量写入tick数据"""
        max_sql_size = 800000
        max_tables_per_batch = 100
        
        try:
            current_sql_parts = []
            current_size = 0
            table_count = 0
            
            for (symbol, exchange), ticks in grouped_ticks.items():
                table_name = self._get_tick_table_name(symbol, exchange)
                
                values_list = []
                for tick in ticks:
                    value_str = (
                        f"('{tick.datetime}','{tick.name}',{tick.volume},"
                        f"{tick.open_interest},{tick.last_price},{tick.last_volume},"
                        f"{tick.limit_up},{tick.limit_down},{tick.open_price},{tick.high_price},"
                        f"{tick.low_price},{tick.pre_close},"
                        f"{tick.bid_price_1},{tick.bid_price_2},{tick.bid_price_3},{tick.bid_price_4},{tick.bid_price_5},"
                        f"{tick.ask_price_1},{tick.ask_price_2},{tick.ask_price_3},{tick.ask_price_4},{tick.ask_price_5},"
                        f"{tick.bid_volume_1},{tick.bid_volume_2},{tick.bid_volume_3},{tick.bid_volume_4},{tick.bid_volume_5},"
                        f"{tick.ask_volume_1},{tick.ask_volume_2},{tick.ask_volume_3},{tick.ask_volume_4},{tick.ask_volume_5})"
                    )
                    values_list.append(value_str)
                
                table_sql = f"{table_name} VALUES {' '.join(values_list)}"
                table_sql_size = len(table_sql)
                
                if (current_size + table_sql_size > max_sql_size or 
                    table_count >= max_tables_per_batch) and current_sql_parts:
                    self._execute_multi_insert(current_sql_parts)
                    current_sql_parts = []
                    current_size = 0
                    table_count = 0
                
                current_sql_parts.append(table_sql)
                current_size += table_sql_size
                table_count += 1
            
            if current_sql_parts:
                self._execute_multi_insert(current_sql_parts)
            
            return True
            
        except Exception as e:
            print(f"批量插入tick数据失败: {e}")
            return False
    
    def _execute_multi_insert(self, sql_parts: list[str]) -> None:
        """执行多表插入SQL"""
        if not sql_parts:
            return
        
        sql = "INSERT INTO " + " ".join(sql_parts)
        try:
            self.conn.execute(sql)
        except Exception as e:
            print(f"执行多表插入失败: {e}")
            self._fallback_insert(sql_parts)
    
    def _fallback_insert(self, sql_parts: list[str]) -> None:
        """降级策略：逐表插入"""
        for part in sql_parts:
            try:
                sql = "INSERT INTO " + part
                self.conn.execute(sql)
            except Exception as e:
                print(f"单表插入失败: {e}")
    
    def _update_overview_loop(self) -> None:
        """定时更新统计信息的后台线程"""
        while not self._stop_update_thread:
            try:
                time.sleep(5)
                
                with self._update_lock:
                    bar_updates = list(self._pending_bar_updates)
                    tick_updates = list(self._pending_tick_updates)
                    self._pending_bar_updates.clear()
                    self._pending_tick_updates.clear()
                
                if bar_updates:
                    self._batch_update_bar_overviews(bar_updates)
                
                if tick_updates:
                    self._batch_update_tick_overviews(tick_updates)
                    
            except Exception as e:
                print(f"统计信息更新线程异常: {e}")
    
    def _batch_update_bar_overviews(self, table_keys: list) -> None:
        """批量更新bar统计信息（修复版）"""
        for symbol, exchange, interval in table_keys:
            table_name = self._get_bar_table_name(symbol, exchange, interval)
            try:
                # 先检查表是否有数据
                count_result = self.conn.query(f"SELECT count(*) FROM {table_name}")
                count_data = list(count_result)
                
                if not count_data or count_data[0][0] == 0:
                    # 表为空，跳过
                    continue
                
                count = int(count_data[0][0])
                
                # 分别查询最小和最大时间（避免聚合函数问题）
                min_result = self.conn.query(
                    f"SELECT datetime FROM {table_name} ORDER BY datetime ASC LIMIT 1"
                )
                max_result = self.conn.query(
                    f"SELECT datetime FROM {table_name} ORDER BY datetime DESC LIMIT 1"
                )
                
                min_data = list(min_result)
                max_data = list(max_result)
                
                if min_data and max_data:
                    start_time = parse_datetime(min_data[0][0])
                    end_time = parse_datetime(max_data[0][0])
                    
                    # 更新tags
                    self.conn.execute(
                        f"ALTER TABLE {table_name} SET TAG start_time='{start_time}';"
                    )
                    self.conn.execute(
                        f"ALTER TABLE {table_name} SET TAG end_time='{end_time}';"
                    )
                    self.conn.execute(
                        f"ALTER TABLE {table_name} SET TAG count_={count};"
                    )
                    
            except Exception as e:
                print(f"更新bar统计信息失败 {table_name}: {e}")
    
    def _batch_update_tick_overviews(self, table_keys: list) -> None:
        """批量更新tick统计信息（修复版）"""
        for symbol, exchange in table_keys:
            table_name = self._get_tick_table_name(symbol, exchange)
            try:
                # 先检查表是否有数据
                count_result = self.conn.query(f"SELECT count(*) FROM {table_name}")
                count_data = list(count_result)
                
                if not count_data or count_data[0][0] == 0:
                    continue
                
                count = int(count_data[0][0])
                
                # 分别查询最小和最大时间
                min_result = self.conn.query(
                    f"SELECT datetime FROM {table_name} ORDER BY datetime ASC LIMIT 1"
                )
                max_result = self.conn.query(
                    f"SELECT datetime FROM {table_name} ORDER BY datetime DESC LIMIT 1"
                )
                
                min_data = list(min_result)
                max_data = list(max_result)
                
                if min_data and max_data:
                    start_time = parse_datetime(min_data[0][0])
                    end_time = parse_datetime(max_data[0][0])
                    
                    self.conn.execute(
                        f"ALTER TABLE {table_name} SET TAG start_time='{start_time}';"
                    )
                    self.conn.execute(
                        f"ALTER TABLE {table_name} SET TAG end_time='{end_time}';"
                    )
                    self.conn.execute(
                        f"ALTER TABLE {table_name} SET TAG count_={count};"
                    )
                    
            except Exception as e:
                print(f"更新tick统计信息失败 {table_name}: {e}")
    
    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """读取K线数据"""
        table_name = self._get_bar_table_name(symbol, exchange, interval)
        
        sql = (
            f"SELECT datetime, volume, open_interest, open_price, high_price, "
            f"low_price, close_price FROM {table_name} "
            f"WHERE datetime BETWEEN '{start}' AND '{end}' ORDER BY datetime"
        )
        
        try:
            result = self.conn.query(sql)
            data = list(result)
        except Exception as e:
            print(f"查询bar数据失败: {e}")
            return []
        
        if not data:
            return []
        
        bars: list[BarData] = []
        for row in data:
            try:
                dt = parse_datetime(row[0])
                if dt and hasattr(dt, 'astimezone'):
                    dt = dt.astimezone(TZ_INFO)
                
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    datetime=dt,
                    interval=interval,
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
                print(f"解析bar数据失败: {row}, {e}")
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
        table_name = self._get_tick_table_name(symbol, exchange)
        
        sql = f"SELECT * FROM {table_name} WHERE datetime BETWEEN '{start}' AND '{end}' ORDER BY datetime"
        
        try:
            result = self.conn.query(sql)
            data = list(result)
        except Exception as e:
            print(f"查询tick数据失败: {e}")
            return []
        
        if not data:
            return []
        
        ticks: list[TickData] = []
        for row in data:
            try:
                dt = parse_datetime(row[0])
                if dt and hasattr(dt, 'astimezone'):
                    dt = dt.astimezone(TZ_INFO)
                
                tick = TickData(
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
                print(f"解析tick数据失败: {row}, {e}")
                continue
        
        return ticks
    
    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        end: datetime
    ) -> int:
        """删除K线数据"""
        table_name = self._get_bar_table_name(symbol, exchange, interval)
        
        try:
            result = self.conn.query(
                f"SELECT count(*) FROM {table_name} WHERE datetime < '{end}'"
            )
            result_list = list(result)
            count = int(result_list[0][0])
        except Exception:
            count = 0
        
        try:
            self.conn.execute(f"DELETE FROM {table_name} WHERE datetime < '{end}'")
            with self._update_lock:
                self._pending_bar_updates.add((symbol, exchange, interval))
        except Exception as e:
            print(f"删除bar数据失败: {e}")
        
        return count
    
    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        end: datetime
    ) -> int:
        """删除tick数据"""
        table_name = self._get_tick_table_name(symbol, exchange)
        
        try:
            result = self.conn.query(
                f"SELECT count(*) FROM {table_name} WHERE datetime < '{end}'"
            )
            result_list = list(result)
            count = int(result_list[0][0])
        except Exception:
            count = 0
        
        try:
            self.conn.execute(f"DELETE FROM {table_name} WHERE datetime < '{end}'")
            with self._update_lock:
                self._pending_tick_updates.add((symbol, exchange))
        except Exception as e:
            print(f"删除tick数据失败: {e}")
        
        return count
    
    def get_bar_overview(self) -> list[BarOverview]:
        """查询K线汇总信息"""
        try:
            result = self.conn.query(
                "SELECT symbol, exchange, interval_, start_time, end_time, count_ "
                "FROM s_bar GROUP BY symbol, exchange, interval_, start_time, end_time, count_"
            )
            data = list(result)
            
            overviews: list[BarOverview] = []
            seen = set()
            
            for row in data:
                try:
                    key = (row[0], row[1], row[2])
                    if key in seen:
                        continue
                    seen.add(key)
                    
                    overview = BarOverview(
                        symbol=row[0],
                        exchange=Exchange(row[1]),
                        interval=Interval(row[2]),
                        start=parse_datetime(row[3]),
                        end=parse_datetime(row[4]),
                        count=int(row[5]) if row[5] is not None else 0,
                    )
                    if overview.start:
                        overview.start = overview.start.astimezone(TZ_INFO)
                    if overview.end:
                        overview.end = overview.end.astimezone(TZ_INFO)
                    overviews.append(overview)
                except Exception as e:
                    print(f"解析bar overview失败: {row}, {e}")
                    continue
            
            return overviews
            
        except Exception as e:
            print(f"查询bar overview失败: {e}")
            return []
    
    def get_tick_overview(self) -> list[TickOverview]:
        """查询Tick汇总信息"""
        try:
            result = self.conn.query(
                "SELECT symbol, exchange, start_time, end_time, count_ "
                "FROM s_tick GROUP BY symbol, exchange, start_time, end_time, count_"
            )
            data = list(result)
            
            overviews: list[TickOverview] = []
            seen = set()
            
            for row in data:
                try:
                    key = (row[0], row[1])
                    if key in seen:
                        continue
                    seen.add(key)
                    
                    overview = TickOverview(
                        symbol=row[0],
                        exchange=Exchange(row[1]),
                        start=parse_datetime(row[2]),
                        end=parse_datetime(row[3]),
                        count=int(row[4]) if row[4] is not None else 0,
                    )
                    if overview.start:
                        overview.start = overview.start.astimezone(TZ_INFO)
                    if overview.end:
                        overview.end = overview.end.astimezone(TZ_INFO)
                    overviews.append(overview)
                except Exception as e:
                    print(f"解析tick overview失败: {row}, {e}")
                    continue
            
            return overviews
            
        except Exception as e:
            print(f"查询tick overview失败: {e}")
            return []
    
    def execute_sql(self, sql: str) -> dict:
        """执行SQL并返回结果字典格式"""
        try:
            result = self.conn.query(sql)
            data = list(result)
            return {"data": data}
        except Exception as e:
            print(f"执行SQL失败: {sql}, {e}")
            return {"data": []}
    
    def stream_query_rows(
        self,
        sql: str,
        batch_size: int = 10000,
    ) -> Iterator[Sequence[Any]]:
        """以流式方式执行SELECT语句，按批量分页返回rows"""
        base_sql = sql.strip().rstrip(";")
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
            
            if len(rows) < batch_size:
                break
            
            offset += len(rows)
    
    def force_update_all_overviews(self) -> None:
        """强制立即更新所有表的统计信息（用于维护）"""
        print("开始强制更新所有统计信息...")
        
        # 获取所有bar表
        try:
            result = self.conn.query("SHOW TABLES LIKE 'bar_%'")
            bar_tables = [row[0] for row in result]
            
            for table_name in bar_tables:
                try:
                    # 检查表是否有数据
                    count_result = self.conn.query(f"SELECT count(*) FROM {table_name}")
                    count_data = list(count_result)
                    
                    if not count_data or count_data[0][0] == 0:
                        continue
                    
                    count = int(count_data[0][0])
                    
                    # 获取时间范围
                    min_result = self.conn.query(
                        f"SELECT datetime FROM {table_name} ORDER BY datetime ASC LIMIT 1"
                    )
                    max_result = self.conn.query(
                        f"SELECT datetime FROM {table_name} ORDER BY datetime DESC LIMIT 1"
                    )
                    
                    min_data = list(min_result)
                    max_data = list(max_result)
                    
                    if min_data and max_data:
                        start_time = parse_datetime(min_data[0][0])
                        end_time = parse_datetime(max_data[0][0])
                        
                        self.conn.execute(
                            f"ALTER TABLE {table_name} SET TAG start_time='{start_time}';"
                        )
                        self.conn.execute(
                            f"ALTER TABLE {table_name} SET TAG end_time='{end_time}';"
                        )
                        self.conn.execute(
                            f"ALTER TABLE {table_name} SET TAG count_={count};"
                        )
                        print(f"更新bar表统计: {table_name} - {count}条记录")
                        
                except Exception as e:
                    print(f"更新bar表 {table_name} 失败: {e}")
                    
        except Exception as e:
            print(f"获取bar表列表失败: {e}")
        
        # 获取所有tick表
        try:
            result = self.conn.query("SHOW TABLES LIKE 'tick_%'")
            tick_tables = [row[0] for row in result]
            
            for table_name in tick_tables:
                try:
                    count_result = self.conn.query(f"SELECT count(*) FROM {table_name}")
                    count_data = list(count_result)
                    
                    if not count_data or count_data[0][0] == 0:
                        continue
                    
                    count = int(count_data[0][0])
                    
                    min_result = self.conn.query(
                        f"SELECT datetime FROM {table_name} ORDER BY datetime ASC LIMIT 1"
                    )
                    max_result = self.conn.query(
                        f"SELECT datetime FROM {table_name} ORDER BY datetime DESC LIMIT 1"
                    )
                    
                    min_data = list(min_result)
                    max_data = list(max_result)
                    
                    if min_data and max_data:
                        start_time = parse_datetime(min_data[0][0])
                        end_time = parse_datetime(max_data[0][0])
                        
                        self.conn.execute(
                            f"ALTER TABLE {table_name} SET TAG start_time='{start_time}';"
                        )
                        self.conn.execute(
                            f"ALTER TABLE {table_name} SET TAG end_time='{end_time}';"
                        )
                        self.conn.execute(
                            f"ALTER TABLE {table_name} SET TAG count_={count};"
                        )
                        print(f"更新tick表统计: {table_name} - {count}条记录")
                        
                except Exception as e:
                    print(f"更新tick表 {table_name} 失败: {e}")
                    
        except Exception as e:
            print(f"获取tick表列表失败: {e}")
        
        print("统计信息更新完成")
    
    def _get_bar_table_name(self, symbol: str, exchange: Exchange, interval: Interval) -> str:
        """生成bar表名"""
        return "_".join(["bar", sanitize_symbol(symbol), exchange.value, interval.value])
    
    def _get_tick_table_name(self, symbol: str, exchange: Exchange) -> str:
        """生成tick表名"""
        return "_".join(["tick", sanitize_symbol(symbol), exchange.value])
    
    def get_newest_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> BarData:
        """获取最新的一条K线数据"""
        table_name = self._get_bar_table_name(symbol, exchange, interval)
        
        sql = (
            f"SELECT datetime, volume, open_interest, open_price, high_price, "
            f"low_price, close_price FROM {table_name} "
            f"ORDER BY datetime DESC LIMIT 1"
        )
        
        try:
            result = self.conn.query(sql)
            data = list(result)
            
            if not data:
                return None
            
            row = data[0]
            dt = parse_datetime(row[0])
            if dt and hasattr(dt, 'astimezone'):
                dt = dt.astimezone(TZ_INFO)
            
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=row[1],
                open_interest=row[2],
                open_price=row[3],
                high_price=row[4],
                low_price=row[5],
                close_price=row[6],
                gateway_name="DATABASE"
            )
            return bar
            
        except Exception as e:
            print(f"获取最新bar数据失败: {e}")
            return None
    
    def get_newest_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> TickData:
        """获取最新的一条tick数据"""
        table_name = self._get_tick_table_name(symbol, exchange)
        
        sql = f"SELECT * FROM {table_name} ORDER BY datetime DESC LIMIT 1"
        
        try:
            result = self.conn.query(sql)
            data = list(result)
            
            if not data:
                return None
            
            row = data[0]
            dt = parse_datetime(row[0])
            if dt and hasattr(dt, 'astimezone'):
                dt = dt.astimezone(TZ_INFO)
            
            tick = TickData(
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
            return tick
            
        except Exception as e:
            print(f"获取最新tick数据失败: {e}")
            return None
    
    def get_bar_data_statistics(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> dict:
        """获取K线数据统计信息（不依赖tags，实时计算）"""
        table_name = self._get_bar_table_name(symbol, exchange, interval)
        
        try:
            # 先检查是否有数据
            count_result = self.conn.query(f"SELECT count(*) FROM {table_name}")
            count_data = list(count_result)
            
            if not count_data or count_data[0][0] == 0:
                return {}
            
            # 获取统计数据
            result = self.conn.query(
                f"SELECT count(*), "
                f"min(low_price), max(high_price), avg(volume) "
                f"FROM {table_name}"
            )
            results = list(result)
            
            # 获取时间范围
            min_time = self.conn.query(
                f"SELECT datetime FROM {table_name} ORDER BY datetime ASC LIMIT 1"
            )
            max_time = self.conn.query(
                f"SELECT datetime FROM {table_name} ORDER BY datetime DESC LIMIT 1"
            )
            
            min_time_data = list(min_time)
            max_time_data = list(max_time)
            
            if results and results[0][0] and min_time_data and max_time_data:
                return {
                    "count": int(results[0][0]),
                    "start_time": parse_datetime(min_time_data[0][0]),
                    "end_time": parse_datetime(max_time_data[0][0]),
                    "min_price": float(results[0][1]) if results[0][1] else 0,
                    "max_price": float(results[0][2]) if results[0][2] else 0,
                    "avg_volume": float(results[0][3]) if results[0][3] else 0
                }
            return {}
            
        except Exception as e:
            print(f"获取bar统计信息失败: {e}")
            return {}
    
    def clean_empty_tables(self) -> int:
        """清理空表（维护功能）"""
        cleaned_count = 0
        
        try:
            # 清理空的bar表
            result = self.conn.query("SHOW TABLES LIKE 'bar_%'")
            bar_tables = [row[0] for row in result]
            
            for table_name in bar_tables:
                try:
                    result = self.conn.query(f"SELECT count(*) FROM {table_name}")
                    count = int(list(result)[0][0])
                    
                    if count == 0:
                        self.conn.execute(f"DROP TABLE {table_name}")
                        with self._table_cache_lock:
                            self._table_cache.discard(table_name)
                        cleaned_count += 1
                        print(f"删除空表: {table_name}")
                except Exception as e:
                    print(f"检查表 {table_name} 失败: {e}")
            
            # 清理空的tick表
            result = self.conn.query("SHOW TABLES LIKE 'tick_%'")
            tick_tables = [row[0] for row in result]
            
            for table_name in tick_tables:
                try:
                    result = self.conn.query(f"SELECT count(*) FROM {table_name}")
                    count = int(list(result)[0][0])
                    
                    if count == 0:
                        self.conn.execute(f"DROP TABLE {table_name}")
                        with self._table_cache_lock:
                            self._table_cache.discard(table_name)
                        cleaned_count += 1
                        print(f"删除空表: {table_name}")
                except Exception as e:
                    print(f"检查表 {table_name} 失败: {e}")
            
            print(f"共清理 {cleaned_count} 个空表")
            return cleaned_count
            
        except Exception as e:
            print(f"清理空表失败: {e}")
            return cleaned_count
    
    def get_database_info(self) -> dict:
        """获取数据库信息"""
        info = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "table_count": 0,
            "bar_table_count": 0,
            "tick_table_count": 0,
            "total_bar_count": 0,
            "total_tick_count": 0
        }
        
        try:
            # 统计bar表
            result = self.conn.query("SHOW TABLES LIKE 'bar_%'")
            bar_tables = list(result)
            info["bar_table_count"] = len(bar_tables)
            
            # 统计tick表
            result = self.conn.query("SHOW TABLES LIKE 'tick_%'")
            tick_tables = list(result)
            info["tick_table_count"] = len(tick_tables)
            
            info["table_count"] = info["bar_table_count"] + info["tick_table_count"]
            
            # 统计总记录数（可选，可能较慢）
            # for table in bar_tables:
            #     result = self.conn.query(f"SELECT count(*) FROM {table[0]}")
            #     info["total_bar_count"] += int(list(result)[0][0])
            
            # for table in tick_tables:
            #     result = self.conn.query(f"SELECT count(*) FROM {table[0]}")
            #     info["total_tick_count"] += int(list(result)[0][0])
            
        except Exception as e:
            print(f"获取数据库信息失败: {e}")
        
        return info
    
    def close(self) -> None:
        """关闭数据库连接"""
        # 停止更新线程
        self._stop_update_thread = True
        if hasattr(self, '_update_thread') and self._update_thread.is_alive():
            self._update_thread.join(timeout=2)
        
        # 关闭数据库连接
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
                print("TDengine数据库连接已关闭")
            except Exception as e:
                print(f"关闭数据库连接失败: {e}")
    
    def __del__(self):
        """析构函数"""
        self.close()
    
    def __repr__(self) -> str:
        """字符串表示"""
        return f"<TdEngineDatabase {self.host}:{self.port}/{self.database}>"


# 以下函数已废弃，保留仅为兼容性
def generate_bar(bar: BarData) -> str:
    """将BarData转换为可存储的字符串（已废弃）"""
    result = (
        f"('{bar.datetime}', {bar.volume}, {bar.open_interest}, "
        f"{bar.open_price}, {bar.high_price}, {bar.low_price}, {bar.close_price})"
    )
    return result


def generate_tick(tick: TickData) -> str:
    """将TickData转换为可存储的字符串（已废弃）"""
    result = (
        f"('{tick.datetime}', '{tick.name}', {tick.volume}, "
        f"{tick.open_interest}, {tick.last_price}, {tick.last_volume}, "
        f"{tick.limit_up}, {tick.limit_down}, {tick.open_price}, {tick.high_price}, "
        f"{tick.low_price}, {tick.pre_close}, "
        f"{tick.bid_price_1}, {tick.bid_price_2}, {tick.bid_price_3}, {tick.bid_price_4}, {tick.bid_price_5}, "
        f"{tick.ask_price_1}, {tick.ask_price_2}, {tick.ask_price_3}, {tick.ask_price_4}, {tick.ask_price_5}, "
        f"{tick.bid_volume_1}, {tick.bid_volume_2}, {tick.bid_volume_3}, {tick.bid_volume_4}, {tick.bid_volume_5}, "
        f"{tick.ask_volume_1}, {tick.ask_volume_2}, {tick.ask_volume_3}, {tick.ask_volume_4}, {tick.ask_volume_5})"
    )
    return result
