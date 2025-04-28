create schema if not exists public;
-- 1. 创建供应中心表
CREATE TABLE IF NOT EXISTS supply_center (
    center_name VARCHAR(50) PRIMARY KEY,
    director VARCHAR(50) NOT NULL
);

-- 2. 创建客户表（添加表级外键约束）
CREATE TABLE IF NOT EXISTS client (
    client_id SERIAL PRIMARY KEY,
    client_name VARCHAR(50) NOT NULL UNIQUE,
    country VARCHAR(50) NOT NULL,
    supply_center VARCHAR(50),
    city VARCHAR(50),
    industry VARCHAR(50),
    CONSTRAINT fk_supply_center 
        FOREIGN KEY (supply_center) 
        REFERENCES supply_center(center_name)
);

-- 3. 创建合同表（使用表级外键约束）
CREATE TABLE IF NOT EXISTS contract (
    contract_number VARCHAR(20) PRIMARY KEY,
    client_name VARCHAR(50) NOT NULL,
    contract_date DATE NOT NULL,
    CONSTRAINT fk_client 
        FOREIGN KEY (client_name) 
        REFERENCES client(client_name)
);

-- 4. 创建销售人员表
CREATE TABLE IF NOT EXISTS sales (
    salesman_number INTEGER PRIMARY KEY,
    salesman_name VARCHAR(50) NOT NULL,
    gender VARCHAR(50),
    mobile_number VARCHAR(20),
    age INTEGER
);

-- 5. 创建产品表
CREATE TABLE IF NOT EXISTS product (
    product_code VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(80) NOT NULL
);

-- 6. 创建产品型号表（使用表级外键）
CREATE TABLE IF NOT EXISTS product_model (
    product_code VARCHAR(20),
    product_model VARCHAR(80),
    unit_price INTEGER NOT NULL,
    PRIMARY KEY (product_code, product_model),
    CONSTRAINT fk_product 
        FOREIGN KEY (product_code) 
        REFERENCES product(product_code)
);

-- 7. 创建订单明细表（全部使用表级外键）
CREATE TABLE IF NOT EXISTS order_detail (
    order_id SERIAL PRIMARY KEY,
    contract_number VARCHAR(20),
    product_code VARCHAR(20) NOT NULL,
    product_model VARCHAR(80) NOT NULL,
    quantity INTEGER NOT NULL,
    estimated_delivery_date DATE,
    lodgement_date DATE,
    salesman_number INTEGER NOT NULL,
    
    CONSTRAINT fk_contract 
        FOREIGN KEY (contract_number) 
        REFERENCES contract(contract_number),
        
    CONSTRAINT fk_product_model 
        FOREIGN KEY (product_code, product_model) 
        REFERENCES product_model(product_code, product_model),
        
    CONSTRAINT fk_salesman 
        FOREIGN KEY (salesman_number) 
        REFERENCES sales(salesman_number)
);