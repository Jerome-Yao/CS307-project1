import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, date
import os
from typing import List, Tuple, Optional

# 数据库连接池配置
DB_CONFIG = {
    "host": "localhost",
    "database": "project1",
    "user": "normaluser",
    "password": "123",
    "port": 5432,
    "min_size": 5,
    "max_size": 10,
    "command_timeout": 60
}

file_path = "../../resources/output25S.csv"
abs_file_path = os.path.abspath(file_path)

def clean_date(date_str: str) -> Optional[date]:
    """增强型日期清洗函数"""
    if pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip()
    formats = [
        "%Y/%m/%d", "%Y%m%d", 
        "%Y-%m-%d", "%d/%m/%Y",
        "%m/%d/%Y", "%d-%m-%Y"
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.split(' ')[0], fmt)
            return dt.date()
        except ValueError:
            continue
    
    print(f"无法解析的日期格式: {date_str}")
    return None

async def create_connection_pool() -> asyncpg.pool.Pool:
    """创建数据库连接池"""
    return await asyncpg.create_pool(**DB_CONFIG)

async def copy_with_conflict_handling(
    conn: asyncpg.Connection,
    table: str,
    columns: List[str],
    data: List[tuple],
    conflict_columns: List[str] = None
) -> int:
    """安全批量插入"""
    if not data:
        return 0
    
    temp_table = f"temp_{table}"
    
    async with conn.transaction():
        # 创建临时表
        await conn.execute(f"""
            CREATE TEMP TABLE {temp_table} 
            (LIKE {table} INCLUDING DEFAULTS)
            ON COMMIT DROP
        """)
        
        # 执行COPY
        await conn.copy_records_to_table(temp_table, records=data, columns=columns)
        
        # 构建插入语句
        insert_sql = f"""
            INSERT INTO {table} 
            SELECT * FROM {temp_table}
            {f'ON CONFLICT ({", ".join(conflict_columns)}) DO NOTHING' 
                if conflict_columns else ''}
        """
        
        result = await conn.execute(insert_sql)
        return int(result.split()[-1])

async def parallel_data_import(
    pool: asyncpg.pool.Pool,
    tasks: List[Tuple[str, List[str], List[tuple], List[str]]]
) -> None:
    """增强型并行导入"""
    async def worker(task):
        try:
            async with pool.acquire() as conn:
                table, cols, data, conflict = task
                start = datetime.now()
                affected = await copy_with_conflict_handling(conn, table, cols, data, conflict)
                print(f"{table.ljust(15)} 插入 {affected} 行 | 耗时: {datetime.now()-start}")
        except Exception as e:
            print(f"\n处理任务 {table} 时出错: {str(e)}")
            if data:
                print(f"首条问题数据示例: {data[0]}")
            raise

    # 分批次处理避免内存溢出
    batch_size = 50000
    all_tasks = []
    for task in tasks:
        table, cols, data, conflict = task
        for i in range(0, len(data), batch_size):
            batched_data = data[i:i+batch_size]
            all_tasks.append((table, cols, batched_data, conflict))
    
    # 并行执行
    await asyncio.gather(*[worker(t) for t in all_tasks])

async def main():
    """主函数"""
    total_start = datetime.now()
    print(f"{'='*40}\n数据导入开始 @ {total_start}\n{'='*40}")
    
    # 检查文件
    if not os.path.exists(abs_file_path):
        raise FileNotFoundError(f"CSV文件不存在: {abs_file_path}")
    
    # 阶段1：数据加载和清洗
    print("\n[阶段1] 数据读取和预处理...")
    start_load = datetime.now()
    
    # 明确指定数据类型
    dtype_spec = {
        'salesman number': 'str',
        'mobile phone': 'str',
        'contract number': 'str',
        'product code': 'str',
        'product model': 'str',
        'quantity': 'Int64',
        'unit price': 'float64'
    }
    
    df = pd.read_csv(abs_file_path, dtype=dtype_spec)
    
    # 清洗关键字段
    numeric_cols = ['quantity', 'unit price']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].fillna(0) if 'price' in col else df[col].fillna(0).astype(int)
    
    # 日期字段处理
    date_cols = ['contract date', 'estimated delivery date', 'lodgement date']
    for col in date_cols:
        df[col] = df[col].apply(clean_date)
    
    # 处理文本字段的NaN
    text_cols = ['client enterprise', 'country', 'supply center', 'city', 'industry']
    df[text_cols] = df[text_cols].fillna('').astype(str)
    
    print(f"数据加载完成 | 总记录数: {len(df):,} | 耗时: {datetime.now()-start_load}")
    
    # 阶段2：数据准备
    print("\n[阶段2] 数据准备...")
    start_prepare = datetime.now()
    
    # 确保salesman number转换为整数
    df['salesman number'] = pd.to_numeric(
        df['salesman number'], 
        errors='coerce'
    ).fillna(0).astype(int)
    
    # 构建数据集
    supply_data = df[['supply center', 'director']].drop_duplicates().to_numpy().tolist()
    client_data = df[['client enterprise', 'country', 'supply center', 'city', 'industry']].drop_duplicates().to_numpy().tolist()
    product_data = df[['product code', 'product name']].drop_duplicates().to_numpy().tolist()
    model_data = df[['product code', 'product model', 'unit price']].drop_duplicates().to_numpy().tolist()
    sales_data = df[['salesman number', 'salesman', 'gender', 'mobile phone', 'age']].drop_duplicates().to_numpy().tolist()
    contract_data = df[['contract number', 'client enterprise', 'contract date']].drop_duplicates().to_numpy().tolist()
    
    # 明确转换order数据类型
    order_data = []
    for _, row in df.iterrows():
        order_data.append((
            str(row['contract number']),
            str(row['product code']),
            str(row['product model']),
            int(row['quantity']),
            row['estimated delivery date'],
            row['lodgement date'],
            int(row['salesman number'])
        ))
    
    print(f"数据准备完成 | 耗时: {datetime.now()-start_prepare}")
    
    # 阶段3：并行导入
    print("\n[阶段3] 并行数据导入...")
    pool = await create_connection_pool()
    
    try:
        # 分阶段任务配置
        stage_config = [
            {
                'name': '基础表',
                'tasks': [
                    ('supply_center', ['center_name', 'director'], supply_data, ['center_name']),
                    ('product', ['product_code', 'product_name'], product_data, ['product_code']),
                    ('sales', ['salesman_number', 'salesman_name', 'gender', 'mobile_number'], sales_data, ['salesman_number'])
                ]
            },
            {
                'name': '关联表',
                'tasks': [
                    ('client', ['client_name', 'country', 'supply_center', 'city', 'industry'], client_data, ['client_name']),
                    ('product_model', ['product_code', 'product_model', 'unit_price'], model_data, ['product_code', 'product_model'])
                ]
            },
            {
                'name': '合同数据',
                'tasks': [
                    ('contract', ['contract_number', 'client_name', 'contract_date'], contract_data, ['contract_number'])
                ]
            },
            {
                'name': '订单详情',
                'tasks': [
                    ('order_detail', [
                        'contract_number', 'product_code', 'product_model', 
                        'quantity', 'estimated_delivery_date', 'lodgement_date', 'salesman_number'
                    ], order_data, None)
                ]
            }
        ]
        
        # 分阶段执行
        total_import_start = datetime.now()
        for stage in stage_config:
            stage_start = datetime.now()
            print(f"\n--- 正在导入 {stage['name']} ---")
            await parallel_data_import(pool, stage['tasks'])
            print(f"阶段完成 | 耗时: {datetime.now()-stage_start}")
        
        print(f"\n所有数据导入完成 | 总导入耗时: {datetime.now()-total_import_start}")
        
    except Exception as e:
        print(f"\n致命错误: {type(e).__name__}: {str(e)}")
    finally:
        await pool.close()
    
    total_end = datetime.now()
    print(f"\n{'='*40}\n数据导入结束 @ {total_end}")
    print(f"总运行时间: {total_end - total_start}\n{'='*40}")

if __name__ == "__main__":
    asyncio.run(main())