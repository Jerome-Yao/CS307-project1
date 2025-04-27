import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, date
import os
from psycopg2.extras import execute_batch

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "gaussdb",
    "password": "Wgx@20050109",
    "port": 8888
}

file_path = "../../resources/output25S.csv"
abs_file_path = os.path.abspath(file_path)

def clean_date(date_str: str) -> date:
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
    return None

def create_connection():
    """创建数据库连接"""
    return psycopg2.connect(**DB_CONFIG)

def safe_batch_insert(conn, table: str, columns: list, data: list, conflict_cols: list = None):
    """安全批量插入（带冲突处理）"""
    if not data:
        return 0
    
    temp_table = f"temp_{table.replace('.', '_')}"
    cols_str = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    
    try:
        with conn.cursor() as cursor:
            # 创建临时表
            cursor.execute(f"""
                CREATE TEMP TABLE {temp_table} 
                (LIKE {table} INCLUDING DEFAULTS)
                ON COMMIT DELETE ROWS
            """)
            
            # 批量插入临时表
            execute_batch(cursor,
                f"INSERT INTO {temp_table} ({cols_str}) VALUES ({placeholders})",
                data
            )
            
            # 主表插入
            conflict_clause = ""
            if conflict_cols:
                conflict_cols_str = ', '.join(conflict_cols)
                conflict_clause = f"ON CONFLICT ({conflict_cols_str}) DO NOTHING"
                
            insert_sql = f"""
                INSERT INTO {table} ({cols_str})
                SELECT {cols_str} FROM {temp_table}
                {conflict_clause}
            """
            
            cursor.execute(insert_sql)
            return cursor.rowcount
    except Exception as e:
        conn.rollback()
        raise

def main():
    total_start = datetime.now()
    print(f"{'='*40}\n数据导入开始 @ {total_start}\n{'='*40}")
    
    # 阶段1：数据加载
    print("\n[阶段1] 数据读取和预处理...")
    df = pd.read_csv(abs_file_path, dtype={
        'salesman number': 'str',
        'mobile phone': 'str',
        'contract number': 'str',
        'product code': 'str',
        'product model': 'str'
    })
    
    # 处理数值字段
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    df['unit price'] = pd.to_numeric(df['unit price'], errors='coerce').fillna(0.0)
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)  # 新增age处理
    
    # 处理日期字段
    date_cols = ['contract date', 'estimated delivery date', 'lodgement date']
    for col in date_cols:
        df[col] = df[col].apply(clean_date)
    
    # 处理文本字段
    text_cols = ['client enterprise', 'country', 'supply center', 'city', 'industry']
    df[text_cols] = df[text_cols].fillna('').astype(str)
    
    # 阶段2：数据准备
    print("\n[阶段2] 数据准备...")
    supply_data = df[['supply center', 'director']].drop_duplicates().values.tolist()
    client_data = df[['client enterprise', 'country', 'supply center', 'city', 'industry']].drop_duplicates().values.tolist()
    product_data = df[['product code', 'product name']].drop_duplicates().values.tolist()
    model_data = df[['product code', 'product model', 'unit price']].drop_duplicates().values.tolist()
    
    # 修复sales表数据（包含age字段）
    sales_data = df[['salesman number', 'salesman', 'gender', 'mobile phone', 'age']].drop_duplicates()
    sales_data = sales_data.astype({
        'salesman number': 'int32',
        'age': 'int32'
    }).values.tolist()
    
    contract_data = df[['contract number', 'client enterprise', 'contract date']].drop_duplicates().values.tolist()
    
    # 订单数据
    order_data = df[[
        'contract number', 'product code', 'product model', 
        'quantity', 'estimated delivery date', 'lodgement date', 'salesman number'
    ]].copy()
    order_data['salesman number'] = order_data['salesman number'].astype(int)
    order_data = order_data.values.tolist()
    
    # 阶段3：顺序导入
    print("\n[阶段3] 数据导入...")
    conn = create_connection()
    try:
        # 按依赖顺序导入
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
            count = safe_batch_insert(conn, table, cols, data, conflict)
            print(f"插入 {count} 行 | 耗时: {datetime.now()-start}")
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"\n错误发生: {str(e)}")
    finally:
        conn.close()
    
    total_end = datetime.now()
    print(f"\n{'='*40}\n数据导入结束 @ {total_end}")
    print(f"总运行时间: {total_end - total_start}\n{'='*40}")

if __name__ == "__main__":
    main()