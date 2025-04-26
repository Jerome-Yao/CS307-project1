#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_set>
#include <chrono>
#include <pqxx/pqxx>
#include <iomanip>
#include "csv.h"

// 配置数据库连接
const std::string DB_CONFIG = "host=localhost dbname=project1 user=postgres password=123 port=5432";

// 自定义哈希函数用于tuple
namespace std {
    template<typename... T>
    struct hash<tuple<T...>> {
        size_t operator()(const tuple<T...>& key) const {
            size_t seed = 0;
            apply([&seed](const T&... args) {
                ((seed ^= hash<T>{}(args) + 0x9e3779b9 + (seed << 6) + (seed >> 2)), ...);
            }, key);
            return seed;
        }
    };
}

// 数据清洗函数
std::string clean_date(const std::string& date_str) {
    std::string cleaned = date_str;
    // 去除首尾空格
    cleaned.erase(0, cleaned.find_first_not_of(" \t"));
    cleaned.erase(cleaned.find_last_not_of(" \t") + 1);

    if (cleaned.empty() || cleaned == "nan") 
        return "1970-01-01";

    struct tm tm = {};
    const char* formats[] = {
        "%Y/%m/%d", "%Y%m%d", "%Y-%m-%d", 
        "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"
    };

    for (const auto& fmt : formats) {
        if (strptime(cleaned.c_str(), fmt, &tm)) {
            char buffer[11];
            strftime(buffer, sizeof(buffer), "%Y-%m-%d", &tm);
            return buffer;
        }
    }

    return "1970-01-01";
}

template <typename T>
void batch_insert(pqxx::work& txn, 
                 const std::string& table_name,
                 std::initializer_list<std::string_view> columns,
                 const std::unordered_set<T>& data) {
    if (data.empty()) return;

    pqxx::table_path tablePath{table_name};
    // 创建流式写入器
    auto stream = pqxx::stream_to::table(txn, tablePath, columns);

    // 批量写入数据
    auto write_row = [&stream](const auto&... args) {
        stream.write_values(args...);
    };

    // 批量写入数据
    for (const auto& item : data) {
        std::apply(write_row, item);
    }

    // 完成流操作
    stream.complete();
}

int main() {
    auto total_start = std::chrono::high_resolution_clock::now();
    
    try {
        // 阶段1: 数据加载
        auto load_start = std::chrono::high_resolution_clock::now();
        io::CSVReader<20, io::trim_chars<>, io::double_quote_escape<',','\"'>> in("/home/wgx/database/CS307-project1/resources/output25S.csv");
        in.read_header(io::ignore_extra_column, 
            "contract number", "client enterprise", "supply center", "country", "city", 
            "industry", "product code", "product name", "product model", "unit price", 
            "quantity", "contract date", "estimated delivery date", "lodgement date", 
            "director", "salesman", "salesman number", "gender", "age", "mobile phone");

        // 使用无序集合去重
        std::unordered_set<std::tuple<std::string, std::string>> supply_centers;
        std::unordered_set<std::tuple<std::string, std::string, std::string, std::string, std::string>> clients;
        std::unordered_set<std::tuple<std::string, std::string>> products;
        std::unordered_set<std::tuple<std::string, std::string, int>> product_models;
        std::unordered_set<std::tuple<int, std::string, std::string, std::string, int>> sales;
        std::unordered_set<std::tuple<std::string, std::string, std::string>> contracts;
        std::vector<std::tuple<std::string, std::string, std::string, int, std::string, std::string, int>> orders;

        std::string contract_num, client_ent, supply_ctr, country, city, industry, 
                    product_cd, product_nm, product_mdl, contract_dt, est_deliv_dt, lodge_dt,
                    director, salesman, gender, mobile;
        int unit_price, quantity, salesman_num, age;

        while (in.read_row(contract_num, client_ent, supply_ctr, country, city, industry,
                          product_cd, product_nm, product_mdl, unit_price, quantity,
                          contract_dt, est_deliv_dt, lodge_dt, director, salesman,
                          salesman_num, gender, age, mobile)) {
            // 数据类型转换

            // 去重数据准备
            supply_centers.emplace(supply_ctr, director);
            clients.emplace(client_ent, country, supply_ctr, city, industry);
            products.emplace(product_cd, product_nm);
            product_models.emplace(product_cd, product_mdl, unit_price);
            sales.emplace(salesman_num, salesman, gender, mobile, age);
            contracts.emplace(contract_num, client_ent, clean_date(contract_dt));
            orders.emplace_back(contract_num, product_cd, product_mdl, quantity, 
                              clean_date(est_deliv_dt), clean_date(lodge_dt), salesman_num);
        }

        auto load_end = std::chrono::high_resolution_clock::now();
        std::cout << "数据加载完成 | 记录数: " 
                << supply_centers.size() << " supply centers, "
                << orders.size() << " orders | 耗时: "
                << std::chrono::duration<double>(load_end - load_start).count() << "s\n";

        // 阶段2: 数据库操作
        pqxx::connection conn(DB_CONFIG);
        pqxx::work txn(conn);

        auto db_start = std::chrono::high_resolution_clock::now();

        // 批量插入（保持外键顺序）
        batch_insert(txn, "supply_center", {"center_name", "director"}, supply_centers);
        batch_insert(txn, "client", {"client_name", "country", "supply_center", "city", "industry"}, clients);
        batch_insert(txn, "product", {"product_code", "product_name"}, products);
        batch_insert(txn, "product_model", {"product_code", "product_model", "unit_price"}, product_models);
        batch_insert(txn, "sales", {"salesman_number", "salesman_name", "gender", "mobile_number", "age"}, sales);
        batch_insert(txn, "contract", {"contract_number", "client_name", "contract_date"}, contracts);

        // 订单详情直接流式插入
        if (!orders.empty()) {
            // 将表名转换为 pqxx::table_path 类型
            auto order_stream = pqxx::stream_to::table(txn, {"order_detail"}, 
                {"contract_number", "product_code", "product_model", "quantity", 
                 "estimated_delivery_date", "lodgement_date", "salesman_number"});

            for (const auto& o : orders) {
                // 正确捕获 order_stream 变量
                std::apply([&order_stream](const auto&... args) {
                    order_stream.write_values(args...);
                }, o);
            }
            order_stream.complete();
        }

        txn.commit();
        auto db_end = std::chrono::high_resolution_clock::now();
        std::cout << "数据库操作耗时: " 
                << std::chrono::duration<double>(db_end - db_start).count() << "s\n";

    } catch (const std::exception& e) {
        std::cerr << "错误: " << e.what() << std::endl;
        return 1;
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    std::cout << "\n总运行时间: "
            << std::chrono::duration<double>(total_end - total_start).count() 
            << " 秒\n";

    return 0;
}