-- 移除create schema语句，因部分数据库版本可能不支持if not exists语法
-- 创建供应中心表
CREATE TABLE IF NOT EXISTS supply_center (
    center_name VARCHAR(50) PRIMARY KEY,
    director VARCHAR(50) NOT NULL
);

-- 创建客户表（移除外键约束）
CREATE TABLE IF NOT EXISTS client (
    client_id SERIAL PRIMARY KEY,
    client_name VARCHAR(50) NOT NULL UNIQUE,
    country VARCHAR(50) NOT NULL,
    supply_center VARCHAR(50),
    city VARCHAR(50),
    industry VARCHAR(50)
);

-- 创建合同表（移除外键约束）
CREATE TABLE IF NOT EXISTS contract (
    contract_number VARCHAR(20) PRIMARY KEY,
    client_name VARCHAR(50) NOT NULL,
    contract_date DATE NOT NULL
);

-- 创建销售人员表
CREATE TABLE IF NOT EXISTS sales (
    salesman_number INTEGER PRIMARY KEY,
    salesman_name VARCHAR(50) NOT NULL,
    gender VARCHAR(50),
    mobile_number VARCHAR(20),
    age INTEGER
);

-- 创建产品表
CREATE TABLE IF NOT EXISTS product (
    product_code VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(80) NOT NULL
);

-- 创建产品型号表（移除外键约束）
CREATE TABLE IF NOT EXISTS product_model (
    product_code VARCHAR(20),
    product_model VARCHAR(80),
    unit_price INTEGER NOT NULL,
    PRIMARY KEY (product_code, product_model)
);

-- 创建订单明细表（移除外键约束）
CREATE TABLE IF NOT EXISTS order_detail (
    order_id SERIAL PRIMARY KEY,
    contract_number VARCHAR(20),
    product_code VARCHAR(20) NOT NULL,
    product_model VARCHAR(80) NOT NULL,
    quantity INTEGER NOT NULL,
    estimated_delivery_date DATE,
    lodgement_date DATE,
    salesman_number INTEGER NOT NULL
);