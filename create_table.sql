create schema if not exists public;
--1
create table if not exists supply_center
(
    center_name varchar(50) primary key,
    director    varchar(50) not null
);

--2
create table if not exists client
(
    client_id     serial primary key,
    client_name   varchar(50) not null
        constraint uq unique,
    country       varchar(50) not null,
    supply_center varchar(50)
        constraint client_area references supply_center (center_name),
    city          varchar(50),
    industry      varchar(50)
);

--3
create table if not exists contract
(
    contract_number varchar(20) not null primary key,
    client_name     varchar(50) not null
        constraint ordered_from references client (client_name),
    contract_date   date        not null
);

--4
create table if not exists sales
(
    salesman_number integer     not null primary key,
    salesman_name   varchar(50) not null,
    gender          varchar(50),
    mobile_number   varchar(20),
    age             integer
);

--5
create table if not exists product
(
    product_code varchar(20) not null primary key,
    product_name varchar(80) not null
);

--6
create table if not exists product_model
(
    product_code  varchar(20)
        constraint product_has_name references product (product_code),
    product_model varchar(80),
    primary key (product_code, product_model),
    unit_price    integer not null
);

--7
create table if not exists order_detail
(
    order_id                serial primary key,
    contract_number         varchar(20)
        constraint order_belong_to references contract (contract_number),
    product_code            varchar(20) not null,
    product_model           varchar(80) not null,
    foreign key (product_code, product_model)
        references product_model (product_code, product_model),
    quantity                integer     not null,
    estimated_delivery_date date,
    lodgement_date          date,
    salesman_number         integer     not null
        constraint order_sale_by references sales (salesman_number)
);

