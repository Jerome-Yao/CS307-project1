import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, date
import os
from typing import List, Optional

# MySQL数据库配置
DB_CONFIG = {
    "host": "localhost",
    "database": "public",
    "user": "root",
    "password": "wgx20050109",
    "port": 3306,
    "cursorclass": pymysql.cursors.DictCursor
}

file_path = "../../resources/output25S.csv"
abs_file_path = os.path.abspath(file_path)

def clean_date(date_str: str) -> Optional[str]:
    """MySQL兼容日期清洗（返回字符串格式）"""
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
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    print(f"无法解析的日期格式: {date_str}")
    return None

def create_connection():
    """创建MySQL数据库连接"""
    return pymysql.connect(**DB_CONFIG)

def mysql_batch_insert(
    conn: pymysql.Connection,
    table: str,
    columns: List[str],
    data: List[tuple],
    batch_size: int = 5000,
    conflict_cols: List[str] = None
) -> int:
    """增强型MySQL批量插入"""
    if not data:
        return 0
    
    placeholders = ", ".join(["%s"] * len(columns))
    cols_str = ", ".join(columns)
    
    # 构建ON DUPLICATE子句
    update_clause = ""
    if conflict_cols:
        update_fields = [f"{col}=VALUES({col})" for col in conflict_cols]
        update_clause = f"ON DUPLICATE KEY UPDATE {', '.join(update_fields)}"
    
    sql = f"""
        INSERT INTO {table} ({cols_str})
        VALUES ({placeholders})
        {update_clause}
    """
    
    total = 0
    try:
        with conn.cursor() as cursor:
            # 分批提交防止内存溢出
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                cursor.executemany(sql, batch)
                total += cursor.rowcount
            conn.commit()
    except pymysql.Error as e:
        conn.rollback()
        print(f"批量插入失败: {str(e)}")
        raise
    
    return total

def main():
    total_start = datetime.now()
    print(f"{'='*40}\n数据导入开始 @ {total_start}\n{'='*40}")
    
    # 阶段1：数据加载
    if not os.path.exists(abs_file_path):
        raise FileNotFoundError(f"CSV文件不存在: {abs_file_path}")
    
    print("\n[阶段1] 数据读取和预处理...")
    df = pd.read_csv(abs_file_path, dtype={
        'salesman number': 'str',
        'mobile phone': 'str',
        'contract number': 'str',
        'product code': 'str',
        'product model': 'str'
    })
    
    # 数值清洗
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    df['unit price'] = df['unit price'].round(4)  # 适配DECIMAL(10,4)
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(np.int32)
    
    # 日期处理
    date_cols = ['contract date', 'estimated delivery date', 'lodgement date']
    for col in date_cols:
        df[col] = df[col].apply(clean_date)
    
    # 文本处理
    text_cols = ['client enterprise', 'country', 'supply center', 'city', 'industry']
    df[text_cols] = df[text_cols].fillna('').astype(str)
    
    # 阶段2：数据准备
    print("\n[阶段2] 数据结构化处理...")
    supply_data = df[['supply center', 'director']].drop_duplicates().values.tolist()
    client_data = df[['client enterprise', 'country', 'supply center', 'city', 'industry']].drop_duplicates().values.tolist()
    product_data = df[['product code', 'product name']].drop_duplicates().values.tolist()
    model_data = df[['product code', 'product model', 'unit price']].drop_duplicates().values.tolist()
    
    # 销售数据转换
    sales_data = df[['salesman number', 'salesman', 'gender', 'mobile phone', 'age']]
    sales_data = sales_data.drop_duplicates()
    sales_data['salesman number'] = sales_data['salesman number'].astype(np.int32)
    sales_data = sales_data.values.tolist()
    
    # 合同数据
    contract_data = df[['contract number', 'client enterprise', 'contract date']].drop_duplicates().values.tolist()
    
    # 订单数据转换
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
    
    # 阶段3：数据导入
    print("\n[阶段3] 分阶段数据导入...")
    conn = create_connection()
    
    try:
        # 按表依赖顺序导入
        tables = [
            ('supply_center', ['center_name', 'director'], supply_data, ['center_name']),
            ('product', ['product_code', 'product_name'], product_data, ['product_code']),
            ('sales', ['salesman_number', 'salesman_name', 'gender', 'mobile_number', 'age'], sales_data, ['salesman_number']),
            ('client', ['client_name', 'country', 'supply_center', 'city', 'industry'], client_data, ['client_name']),
            ('product_model', ['product_code', 'product_model', 'unit_price'], model_data, ['product_code', 'product_model']),
            ('contract', ['contract_number', 'client_name', 'contract_date'], contract_data, ['contract_number']),
            ('order_detail', [
                'contract_number', 'product_code', 'product_model',
                'quantity', 'estimated_delivery_date', 'lodgement_date', 'salesman_number'
            ], order_data, None)
        ]
        
        for table, cols, data, conflict in tables:
            start = datetime.now()
            print(f"正在导入 {table.ljust(15)}...", end=' ')
            count = mysql_batch_insert(conn, table, cols, data, conflict_cols=conflict)
            print(f"影响 {count} 行 | 耗时: {datetime.now()-start}")
            
    except Exception as e:
        print(f"\n发生错误: {str(e)}")
    finally:
        conn.close()
    
    total_end = datetime.now()
    print(f"\n{'='*40}\n数据导入完成 @ {total_end}")
    print(f"总运行时间: {total_end - total_start}\n{'='*40}")

if __name__ == "__main__":
    # Windows系统设置
    if os.name == 'nt':
        from asyncio import WindowsSelectorEventLoopPolicy
        asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    
    main()