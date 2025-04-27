CREATE DATABASE IF NOT EXISTS `public`;
USE `public`;

--1
CREATE TABLE IF NOT EXISTS `supply_center`
(
    `center_name` VARCHAR(50) PRIMARY KEY,
    `director`    VARCHAR(50) NOT NULL
) ENGINE=InnoDB;

--2
CREATE TABLE IF NOT EXISTS `client`
(
    `client_id`     BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    `client_name`   VARCHAR(50) NOT NULL UNIQUE,
    `country`       VARCHAR(50) NOT NULL,
    `supply_center` VARCHAR(50),
    `city`          VARCHAR(50),
    `industry`      VARCHAR(50),
    CONSTRAINT `client_area` FOREIGN KEY (`supply_center`) REFERENCES `supply_center` (`center_name`)
) ENGINE=InnoDB;

--3
CREATE TABLE IF NOT EXISTS `contract`
(
    `contract_number` VARCHAR(20) NOT NULL PRIMARY KEY,
    `client_name`     VARCHAR(50) NOT NULL,
    `contract_date`   DATE        NOT NULL,
    CONSTRAINT `ordered_from` FOREIGN KEY (`client_name`) REFERENCES `client` (`client_name`)
) ENGINE=InnoDB;

--4
CREATE TABLE IF NOT EXISTS `sales`
(
    `salesman_number` INT NOT NULL PRIMARY KEY,
    `salesman_name`   VARCHAR(50) NOT NULL,
    `gender`          VARCHAR(50),
    `mobile_number`   VARCHAR(20),
    `age`             INT
) ENGINE=InnoDB;

--5
CREATE TABLE IF NOT EXISTS `product`
(
    `product_code` VARCHAR(20) NOT NULL PRIMARY KEY,
    `product_name` VARCHAR(80) NOT NULL
) ENGINE=InnoDB;

--6
CREATE TABLE IF NOT EXISTS `product_model`
(
    `product_code`  VARCHAR(20),
    `product_model` VARCHAR(80),
    `unit_price`    INT NOT NULL,
    PRIMARY KEY (`product_code`, `product_model`),
    CONSTRAINT `product_has_name` FOREIGN KEY (`product_code`) REFERENCES `product` (`product_code`)
) ENGINE=InnoDB;

--7
CREATE TABLE IF NOT EXISTS `order_detail`
(
    `order_id`                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    `contract_number`         VARCHAR(20),
    `product_code`            VARCHAR(20) NOT NULL,
    `product_model`           VARCHAR(80) NOT NULL,
    `quantity`                INT NOT NULL,
    `estimated_delivery_date` DATE,
    `lodgement_date`          DATE,
    `salesman_number`         INT NOT NULL,
    CONSTRAINT `order_belong_to` FOREIGN KEY (`contract_number`) REFERENCES `contract` (`contract_number`),
    FOREIGN KEY (`product_code`, `product_model`) REFERENCES `product_model` (`product_code`, `product_model`),
    CONSTRAINT `order_sale_by` FOREIGN KEY (`salesman_number`) REFERENCES `sales` (`salesman_number`)
) ENGINE=InnoDB;