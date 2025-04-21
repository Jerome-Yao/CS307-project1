import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import os

# 配置数据库连接
DB_CONFIG = {
    "host": "localhost",
    "database": "project1",
    "user": "postgres",
    "password": "123",
    "port": "5432"
}

file_path = "/media/wgx/Ventoy/learn/25spring/database/CS307-project1/resources/output25S.csv"
abs_file_path = os.path.abspath(file_path)

# 数据清洗函数
def clean_date(date_str):
    date_str = str(date_str).strip()
    if not date_str or date_str.lower() == 'nan':
        return None
    try:
        return datetime.strptime(date_str, "%Y/%m/%d").date()
    except ValueError:
        try:
            return datetime.strptime(date_str.split('/')[0], "%Y%m%d").date()
        except Exception as e:
            print(f"Error parsing date: {date_str}. Error: {e}")
            return None

# 主程序
def main():
    # 记录脚本开始时间
    start_time = datetime.now()
    print(f"脚本开始执行时间: {start_time}")

    if not os.path.isfile(abs_file_path):
        print(f"文件不存在: {abs_file_path}")
        return

    # 记录读取文件开始时间
    read_file_start = datetime.now()
    df = pd.read_csv(abs_file_path, encoding='utf-8')
    # 记录读取文件结束时间并计算耗时
    read_file_end = datetime.now()
    print(f"读取文件耗时: {read_file_end - read_file_start}")
    print("CSV文件列名:", df.columns.tolist())  # 调试用，确认实际列名

    try:
        # 记录数据库连接开始时间
        connect_db_start = datetime.now()
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # 尝试执行一个简单的查询来验证连接
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        # 记录数据库连接结束时间并计算耗时
        connect_db_end = datetime.now()
        if result:
            print("数据库连接成功")
            print(f"数据库连接耗时: {connect_db_end - connect_db_start}")
        else:
            print("数据库连接异常，查询无结果")
            return

        # 记录插入数据开始时间
        insert_data_start = datetime.now()

        # 插入supply_center
        supply_centers = df[['supply center', 'director']].drop_duplicates()
        for _, row in supply_centers.iterrows():
            cursor.execute("""
                INSERT INTO supply_center (center_name, director) 
                VALUES (%s, %s) 
                ON CONFLICT (center_name) DO NOTHING""",
                (row['supply center'], row['director']))

        # 插入client（添加supply_center字段）
        clients = df[['client enterprise', 'country', 'supply center', 'city', 'industry']].drop_duplicates()
        for _, row in clients.iterrows():
            cursor.execute("""
                INSERT INTO client (client_name, country, supply_center, city, industry) 
                VALUES (%s, %s, %s, %s, %s) 
                ON CONFLICT (client_name) DO NOTHING""",
                (row['client enterprise'], 
                 row['country'],
                 row['supply center'],
                 row['city'],
                 row['industry']))

        # 插入product
        products = df[['product code', 'product name', 'product model', 'unit price']].drop_duplicates()
        for _, row in products.iterrows():
            cursor.execute("""
                INSERT INTO product (product_code, product_name)
                VALUES (%s, %s)
                ON CONFLICT (product_code) DO NOTHING""",
                (row['product code'], row['product name']))
            
            cursor.execute("""
                INSERT INTO product_model (product_code, product_model, unit_price)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_code, product_model) DO NOTHING""",
                (row['product code'], row['product model'], row['unit price']))

        # 插入sales（修正列名）
        sales_people = df[['salesman number', 'salesman', 'gender', 'mobile phone']].drop_duplicates()
        for _, row in sales_people.iterrows():
            cursor.execute("""
                INSERT INTO sales (salesman_number, salesman_name, gender, mobile_number)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (salesman_number) DO NOTHING""",
                (row['salesman number'], 
                 row['salesman'],
                 row['gender'],
                 row['mobile phone']))

        # 插入contract（使用client_name）
        contracts = []
        for _, row in df.iterrows():
            contracts.append((
                row['contract number'],
                row['client enterprise'],  # 直接使用client_name
                clean_date(row['contract date'])
            ))
        
        cursor.executemany("""
            INSERT INTO contract (contract_number, client_name, contract_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (contract_number) DO NOTHING""", contracts)

        # 插入order_detail（修正表名和字段）
        orders = []
        for _, row in df.iterrows():
            orders.append((
                row['contract number'],    # contract_number
                row['product code'],
                row['product model'],
                row['quantity'],
                clean_date(row['estimated delivery date']),
                clean_date(row['lodgement date']),
                row['salesman number']
            ))
        
        cursor.executemany("""
            INSERT INTO order_detail 
            (contract_number, product_code, product_model, quantity, estimated_delivery_date, lodgement_date, salesman_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""", orders)

        conn.commit()
        # 记录插入数据结束时间并计算耗时
        insert_data_end = datetime.now()
        print(f"数据插入耗时: {insert_data_end - insert_data_start}")
        print("数据导入成功！")
    except Exception as e:
        print(f"操作失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    # 记录脚本结束时间并计算总耗时
    end_time = datetime.now()
    print(f"脚本结束执行时间: {end_time}")
    print(f"脚本总耗时: {end_time - start_time}")

if __name__ == "__main__":
    main()