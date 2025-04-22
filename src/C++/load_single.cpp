#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <ctime>
#include <pqxx/pqxx>
#include <iomanip> 
#include "csv.h"

// 配置数据库连接
const std::string DB_CONFIG = "host=localhost dbname=project1 user=postgres password=123 port=5432";

// 数据清洗函数
// 修改 clean_date 函数中的空值处理
std::string clean_date(const std::string& date_str) {
    std::string cleaned = date_str;
    // 去除首尾空格
    size_t start = cleaned.find_first_not_of(" \t");
    if (start != std::string::npos) {
        cleaned = cleaned.substr(start);
    }
    size_t end = cleaned.find_last_not_of(" \t");
    if (end != std::string::npos) {
        cleaned = cleaned.substr(0, end + 1);
    }

    if (cleaned.empty() || cleaned == "nan") {
        return "1970-01-01"; // 改为返回默认日期而不是空字符串
    }

    // 尝试解析日期
    try {
        // 格式: %Y/%m/%d
        std::tm tm = {};
        std::istringstream ss(cleaned);
        ss >> std::get_time(&tm, "%Y/%m/%d");
        if (!ss.fail()) {
            char buffer[11];
            std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &tm);
            return buffer;
        }

        // 格式: %Y%m%d
        ss.clear();
        ss.str(cleaned.substr(0, cleaned.find('/')));
        ss >> std::get_time(&tm, "%Y%m%d");
        if (!ss.fail()) {
            char buffer[11];
            std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &tm);
            return buffer;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error parsing date: " << cleaned << ". Error: " << e.what() << std::endl;
    }

    return "1970-01-01"; // 确保最终返回有效日期
}

int main() {
    auto start_time = std::chrono::system_clock::now();
    // std::cout << "脚本开始执行时间: " << std::chrono::system_clock::to_time_t(start_time) << std::endl;

    std::string file_path = "/home/wgx/database/CS307-project1/resources/output25S.csv";
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "文件不存在: " << file_path << std::endl;
        return 1;
    }

    auto read_file_start = std::chrono::system_clock::now();

    io::CSVReader<20, io::trim_chars<>, io::double_quote_escape<',', '"'>> reader(file_path);
    reader.read_header(io::ignore_extra_column, 
        "contract number", "client enterprise", "supply center", "country", "city", 
        "industry", "product code", "product name", "product model", "unit price", 
        "quantity", "contract date", "estimated delivery date", "lodgement date", 
        "director", "salesman", "salesman number", "gender", "age", "mobile phone");
    
    std::string supply_center, director, client_enterprise, country, city, industry, product_code, product_name, product_model, age;
    int unit_price, salesman_number;
    std::string salesman, gender, mobile_phone, contract_number, contract_date, quantity, estimated_delivery_date, lodgement_date;

    std::vector<std::tuple<std::string, std::string>> supply_centers;
    std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string>> clients;
    std::vector<std::tuple<std::string, std::string, std::string, int>> products;
    std::vector<std::tuple<int, std::string, std::string, std::string, int>> sales_people;  
    std::vector<std::tuple<std::string, std::string, std::string>> contracts;
    std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string, std::string, std::string>> orders;

    while (reader.read_row(contract_number, client_enterprise, supply_center, country, city, 
        industry, product_code, product_name, product_model, unit_price, 
        quantity, contract_date, estimated_delivery_date, lodgement_date, 
        director, salesman, salesman_number, gender, age, mobile_phone)) {
            supply_centers.emplace_back(supply_center, director);
            clients.emplace_back(client_enterprise, country, supply_center, city, industry);
            products.emplace_back(product_code, product_name, product_model, unit_price);
            int age_int = 0;
            try {
                if (!age.empty()) {
                    age_int = std::stoi(age);
                }
            } catch (const std::exception&) {
                age_int = 0;
            }
            sales_people.emplace_back(salesman_number, salesman, gender, mobile_phone, age_int);
            contracts.emplace_back(contract_number, client_enterprise, clean_date(contract_date));
            orders.emplace_back(contract_number, product_code, product_model, quantity, clean_date(estimated_delivery_date), clean_date(lodgement_date), std::to_string(salesman_number));
    }

    auto read_file_end = std::chrono::system_clock::now();
    std::cout << "读取文件耗时: " << std::chrono::duration_cast<std::chrono::milliseconds>(read_file_end - read_file_start).count() << "ms" << std::endl;

    try {
        pqxx::connection conn(DB_CONFIG);
        pqxx::work txn(conn);

        auto connect_db_start = std::chrono::system_clock::now();
        pqxx::result res = txn.exec("SELECT 1");
        auto connect_db_end = std::chrono::system_clock::now();

        if (res.size() > 0) {
            std::cout << "数据库连接成功" << std::endl;
            std::cout << "数据库连接耗时: " << std::chrono::duration_cast<std::chrono::milliseconds>(connect_db_end - connect_db_start).count() << "ms" << std::endl;
        } else {
            std::cerr << "数据库连接异常，查询无结果" << std::endl;
            return 1;
        }

        auto insert_data_start = std::chrono::system_clock::now();

        // 插入 supply_center
        for (const auto& sc : supply_centers) {
            txn.exec_params("INSERT INTO supply_center (center_name, director) VALUES ($1, $2) ON CONFLICT (center_name) DO NOTHING",
                            std::get<0>(sc), std::get<1>(sc));
        }

        // 插入 client
        for (const auto& c : clients) {
            txn.exec_params("INSERT INTO client (client_name, country, supply_center, city, industry) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (client_name) DO NOTHING",
                            std::get<0>(c), std::get<1>(c), std::get<2>(c), std::get<3>(c), std::get<4>(c));
        }

        // 插入 product 和 product_model
        for (const auto& p : products) {
            txn.exec_params("INSERT INTO product (product_code, product_name) VALUES ($1, $2) ON CONFLICT (product_code) DO NOTHING",
                            std::get<0>(p), std::get<1>(p));
            txn.exec_params("INSERT INTO product_model (product_code, product_model, unit_price) VALUES ($1, $2, $3) ON CONFLICT (product_code, product_model) DO NOTHING",
                            std::get<0>(p), std::get<2>(p), std::get<3>(p));
        }

        // 插入 sales
        for (const auto& s : sales_people) {
            txn.exec_params("INSERT INTO sales (salesman_number, salesman_name, gender, mobile_number, age) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (salesman_number) DO NOTHING",
                            std::get<0>(s), std::get<1>(s), std::get<2>(s), std::get<3>(s), std::get<4>(s));
        }

        // 插入 contract
        for (const auto& ct : contracts) {
            txn.exec_params("INSERT INTO contract (contract_number, client_name, contract_date) VALUES ($1, $2, $3) ON CONFLICT (contract_number) DO NOTHING",
                            std::get<0>(ct), std::get<1>(ct), std::get<2>(ct));
        }

        // 插入 order_detail
        for (const auto& o : orders) {
            txn.exec_params("INSERT INTO order_detail (contract_number, product_code, product_model, quantity, estimated_delivery_date, lodgement_date, salesman_number) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                            std::get<0>(o), std::get<1>(o), std::get<2>(o), std::get<3>(o), std::get<4>(o), std::get<5>(o), std::get<6>(o));
        }

        txn.commit();

        auto insert_data_end = std::chrono::system_clock::now();
        std::cout << "数据插入耗时: " << std::chrono::duration_cast<std::chrono::milliseconds>(insert_data_end - insert_data_start).count() << "ms" << std::endl;
        std::cout << "数据导入成功！" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "操作失败: " << e.what() << std::endl;
    }

    auto end_time = std::chrono::system_clock::now();
    // std::cout << "脚本结束执行时间: " << std::chrono::system_clock::to_time_t(end_time) << std::endl;
    std::cout << "脚本总耗时: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << "ms" << std::endl;

    return 0;
}