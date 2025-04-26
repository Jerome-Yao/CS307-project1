#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_set>
#include <chrono>
#include <future>
#include <pqxx/pqxx>
#include <iomanip>
#include "csv.h"

const std::string DB_CONFIG = "host=localhost dbname=project1 user=postgres password=123 port=5432";

// 自定义元组哈希
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

// 并行插入模板
template <typename T>
void parallel_table_insert(const std::string& table_name, 
                            std::initializer_list<std::string_view> columns,
                          const std::unordered_set<T>& dataset) {
    try {
        pqxx::connection conn(DB_CONFIG);
        pqxx::work txn(conn);
        
        pqxx::table_path tablePath{table_name};
        auto stream = pqxx::stream_to::table(txn, tablePath, columns);
        
        for (const auto& item : dataset) {
            std::apply([&](const auto&... args) {
                stream.write_values(args...);
            }, item);
        }
        
        stream.complete();
        txn.commit();
        std::cout << "成功插入 " << table_name << " (" << dataset.size() << " 行)\n";
    } catch (const std::exception& e) {
        std::cerr << "插入 " << table_name << " 失败: " << e.what() << std::endl;
        throw;
    }
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
            
            // 清洗日期字段
            contract_dt = clean_date(contract_dt);
            est_deliv_dt = clean_date(est_deliv_dt);
            lodge_dt = clean_date(lodge_dt);

            // 收集数据
            supply_centers.emplace(supply_ctr, director);
            clients.emplace(client_ent, country, supply_ctr, city, industry);
            products.emplace(product_cd, product_nm);
            product_models.emplace(product_cd, product_mdl, unit_price);
            sales.emplace(salesman_num, salesman, gender, mobile, age);
            contracts.emplace(contract_num, client_ent, contract_dt);
            orders.emplace_back(contract_num, product_cd, product_mdl, quantity, 
                              est_deliv_dt, lodge_dt, salesman_num);
        }

        auto load_end = std::chrono::high_resolution_clock::now();
        std::cout << "数据加载完成 | 耗时: " 
                << std::chrono::duration<double>(load_end - load_start).count() 
                << "s\n";

        // 阶段2: 并行插入
        std::vector<std::future<void>> tasks;
        
        // 第一组并行任务（无依赖表）
        tasks.push_back(std::async(std::launch::async, [&](){
            parallel_table_insert<decltype(supply_centers)::value_type>( 
            "supply_center", 
            {"center_name", "director"}, 
            std::ref(supply_centers));}
        ));

        tasks.push_back(std::async(std::launch::async, [&](){
            parallel_table_insert<decltype(products)::value_type>(
            "product", 
            {"product_code", "product_name"}, 
            std::ref(products));}
        ));

        // 等待第一组完成
        for (auto& t : tasks) t.wait();
        tasks.clear();

        // 第二组并行任务（部分依赖）
        tasks.push_back(std::async(std::launch::async, [&](){
            parallel_table_insert<decltype(clients)::value_type>(
            "client", 
            {"client_name", "country", "supply_center", "city", "industry"}, 
            std::ref(clients));}
        ));

        tasks.push_back(std::async(std::launch::async, [&](){
            parallel_table_insert<decltype(product_models)::value_type>(
            "product_model", 
            {"product_code", "product_model", "unit_price"}, 
            std::ref(product_models));}
        ));

        tasks.push_back(std::async(std::launch::async, [&](){
            parallel_table_insert<decltype(sales)::value_type>(
            "sales", 
            {"salesman_number", "salesman_name", "gender", "mobile_number", "age"}, 
            std::ref(sales));}
        ));

        // 等待第二组完成
        for (auto& t : tasks) t.wait();
        tasks.clear();

        // 第三阶段（顺序执行）
        parallel_table_insert("contract", 
            {"contract_number", "client_name", "contract_date"}, 
            contracts);

        // 插入订单详情
        if (!orders.empty()) {
            pqxx::connection conn(DB_CONFIG);
            pqxx::work txn(conn);
            
            auto stream = pqxx::stream_to::table(txn, pqxx::table_path{"order_detail"}, 
                {"contract_number", "product_code", "product_model", "quantity", 
                 "estimated_delivery_date", "lodgement_date", "salesman_number"});
            
            for (const auto& o : orders) {
                std::apply([&](const auto&... args) {
                    stream.write_values(args...);
                }, o);
            }
            
            stream.complete();
            txn.commit();
            std::cout << "成功插入 order_detail (" << orders.size() << " 行)\n";
        }

    } catch (const std::exception& e) {
        std::cerr << "主程序异常: " << e.what() << std::endl;
        return 1;
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    std::cout << "\n总运行时间: "
            << std::chrono::duration<double>(total_end - total_start).count() 
            << " 秒\n";

    return 0;
}