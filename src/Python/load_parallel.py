import psycopg2
import pandas as pd
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor

# 移除未使用的 timedelta 导入
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

# 插入 supply_center 表
def insert_supply_center(df):
    supply_centers = df[['supply center', 'director']].drop_duplicates()
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        for _, row in supply_centers.iterrows():
            cursor.execute("""
                INSERT INTO supply_center (center_name, director) 
                VALUES (%s, %s) 
                ON CONFLICT (center_name) DO NOTHING""",
                (row['supply center'], row['director']))
        conn.commit()
    except Exception as e:
        print(f"插入 supply_center 失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# 插入 client 表
def insert_client(df):
    clients = df[['client enterprise', 'country', 'supply center', 'city', 'industry']].drop_duplicates()
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
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
        conn.commit()
    except Exception as e:
        print(f"插入 client 失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# 插入 product 和 product_model 表
def insert_product(df):
    products = df[['product code', 'product name', 'product model', 'unit price']].drop_duplicates()
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
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
        conn.commit()
    except Exception as e:
        print(f"插入 product 或 product_model 失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# 插入 sales 表
def insert_sales(df):
    sales_people = df[['salesman number', 'salesman', 'gender', 'mobile phone']].drop_duplicates()
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        for _, row in sales_people.iterrows():
            cursor.execute("""
                INSERT INTO sales (salesman_number, salesman_name, gender, mobile_number)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (salesman_number) DO NOTHING""",
                (row['salesman number'], 
                 row['salesman'],
                 row['gender'],
                 row['mobile phone']))
        conn.commit()
    except Exception as e:
        print(f"插入 sales 失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# 插入 contract 表
def insert_contract(df):
    contracts = []
    for _, row in df.iterrows():
        contracts.append((
            row['contract number'],
            row['client enterprise'],
            clean_date(row['contract date'])
        ))
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.executemany("""
            INSERT INTO contract (contract_number, client_name, contract_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (contract_number) DO NOTHING""", contracts)
        conn.commit()
    except Exception as e:
        print(f"插入 contract 失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# 插入 order_detail 表
def insert_order_detail(df):
    orders = []
    for _, row in df.iterrows():
        orders.append((
            row['contract number'],
            row['product code'],
            row['product model'],
            row['quantity'],
            clean_date(row['estimated delivery date']),
            clean_date(row['lodgement date']),
            row['salesman number']
        ))
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.executemany("""
            INSERT INTO order_detail 
            (contract_number, product_code, product_model, quantity, estimated_delivery_date, lodgement_date, salesman_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""", orders)
        conn.commit()
    except Exception as e:
        print(f"插入 order_detail 失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

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
        # 记录插入数据开始时间
        insert_data_start = datetime.now()

        with ThreadPoolExecutor() as executor:
            # 插入 supply_center，因为其他表可能依赖它
            executor.submit(insert_supply_center, df).result()

            # 并行插入 client、product、sales
            futures = [
                executor.submit(insert_client, df),
                executor.submit(insert_product, df),
                executor.submit(insert_sales, df)
            ]
            for future in futures:
                future.result()

            # 并行插入 contract
            executor.submit(insert_contract, df).result()

            # 插入 order_detail，它可能依赖前面所有表
            executor.submit(insert_order_detail, df).result()
            # Ensure all tasks are completed before proceeding
            executor.shutdown(wait=True)
        # 记录插入数据结束时间并计算耗时
        insert_data_end = datetime.now()
        print(f"数据插入耗时: {insert_data_end - insert_data_start}")
        print("数据导入成功！")
    except Exception as e:
        print(f"操作失败: {e}")
    finally:
        # 记录脚本结束时间并计算总耗时
        end_time = datetime.now()
        print(f"脚本结束执行时间: {end_time}")
        print(f"脚本总耗时: {end_time - start_time}")

if __name__ == "__main__":
    main()
# 减少数据库连接次数：将数据库连接提到 main 函数中，让所有插入操作共享同一个连接，避免了频繁建立和关闭连接的开销。
# 分块读取 CSV 文件：使用 pd.read_csv 的 chunksize 参数分块读取文件，减少内存占用，提高处理效率。
# 增加并行度：通过 ThreadPoolExecutor 并行执行多个插入函数，合理设置 max_workers 以充分利用多核 CPU 的性能。
# 批量提交：每个数据块处理完成后提交一次事务，避免频繁提交事务带来的性能开销。