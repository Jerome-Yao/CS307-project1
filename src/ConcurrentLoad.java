import com.opencsv.CSVReader;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ConcurrentLoad {
    private final Connection conn;
    private final ExecutorService executor;
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy/M/d");
    private static HikariDataSource dataSource;


    private static final String CSV_PATH = "resources/output25S.csv";

    public ConcurrentLoad(Connection conn, HikariDataSource dataSource) {
        this.conn = conn;
        this.dataSource = dataSource;
        this.executor = Executors.newFixedThreadPool(8); // 按需调整线程数
        try {
            this.conn.setAutoCommit(false); // 关闭自动提交
        } catch (SQLException e) {
            throw new RuntimeException("初始化连接失败", e);
        }
    }

    public void load() throws Exception {
        List<ClientRecord> clients = new ArrayList<>();
        List<SalesRecord> sales = new ArrayList<>();
        List<ProductRecord> products = new ArrayList<>();
        List<ProductModelRecord> productModels = new ArrayList<>();
        List<ContractRecord> contracts = new ArrayList<>();
        List<OrderRecord> orders = new ArrayList<>();
        long start = System.currentTimeMillis();
        parseCsv(clients, sales, products, productModels, contracts, orders);
        long end = System.currentTimeMillis();
        System.out.printf("parse耗时: %.3f 秒\n", (end - start) / 1000.0);

        //first load
        CompletableFuture<Void> clientFuture = insertAsync("client", clients, ClientRecord::getClientName);
        CompletableFuture<Void> salesFuture = insertAsync("sales", sales, SalesRecord::getSalesmanNumber);
        CompletableFuture<Void> productFuture = insertAsync("product", products, ProductRecord::getProductCode);

        try {
            CompletableFuture.allOf(clientFuture, salesFuture, productFuture).get();
        } catch (ExecutionException e) {
            System.err.println("load failed");
            e.printStackTrace();
        }

        //second load
        CompletableFuture<Void> productModelFuture = insertAsync("product_model", productModels, productModelRecord -> productModelRecord.productCode + "#" + productModelRecord.productModel);
        CompletableFuture<Void> contractFuture = insertAsync("contract", contracts, ContractRecord::getContractNumber);

        try {
            CompletableFuture.allOf(productModelFuture, contractFuture).get();
        } catch (ExecutionException e) {
            System.err.println("secondLoad failed");
            e.printStackTrace();
        }

        insertAllOrders(orders).get();

        executor.shutdown();
    }

    public CompletableFuture<Void> insertAllOrders(List<OrderRecord> orders) {
        long startTime = System.currentTimeMillis();
        // 1. 将数据切分为多个批次
        List<List<OrderRecord>> batches = splitIntoBatches(orders, 2000);
        // 2. 提交所有批次任务
        List<CompletableFuture<Void>> futures = batches.stream()
                .map(batch -> insertAsync("order_detail", batch, executor))
                .collect(Collectors.toList());
        // 3. 等待所有任务完成
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((v, e) -> {
                    executor.shutdown();
                    long endTime = System.currentTimeMillis();
                    System.out.printf("插入数据 [order_detail] 耗时: %.3f 秒\n", (endTime - startTime) / 1000.0);
                });
    }

    private void parseCsv(List<ClientRecord> clients, List<SalesRecord> sales, List<ProductRecord> products, List<ProductModelRecord> productModel, List<ContractRecord> contract, List<OrderRecord> order) throws Exception {
        // 与原代码相同，通过 CSV 文件解析其他表数据
        try (CSVReader reader = new CSVReader(new FileReader(CSV_PATH))) {
            System.out.println("open csv successfully");
            String[] headers = reader.readNext();
            String[] row;

            while ((row = reader.readNext()) != null) {
                Map<String, String> rowMap = toRowMap(headers, row);
                // 提取各表数据（根据实际字段填充）
                clients.add(new ClientRecord(
                        rowMap.get("client enterprise"),
                        rowMap.get("country"),
                        rowMap.get("supply center"),
                        rowMap.get("city"),
                        rowMap.get("industry")
                ));

                sales.add(new SalesRecord(
                        rowMap.get("salesman number"),
                        rowMap.get("salesman"),
                        rowMap.get("gender"),
                        rowMap.get("age"),
                        rowMap.get("mobile phone")
                ));

                products.add(new ProductRecord(
                        rowMap.get("product code"),
                        rowMap.get("product name")
                ));
                productModel.add(new ProductModelRecord(
                        rowMap.get("product code"),
                        rowMap.get("product model"),
                        Integer.parseInt(rowMap.get("unit price"))
                ));

                contract.add(new ContractRecord(
                        rowMap.get("contract number"),
                        rowMap.get("client enterprise"),
                        LocalDate.parse(rowMap.get("contract date"), DATE_FORMATTER)
                ));
                LocalDate estDeliveryDate;
                LocalDate lodgementDate;
                if (rowMap.get("estimated delivery date").equals("") || rowMap.get("estimated delivery date") == null) {
                    estDeliveryDate = null;
                } else {
                    estDeliveryDate = LocalDate.parse(rowMap.get("estimated delivery date"), DATE_FORMATTER);
                }
                if (rowMap.get("lodgement date").equals("") || rowMap.get("lodgement date") == null) {
                    lodgementDate = null;
                } else {
                    lodgementDate = LocalDate.parse(rowMap.get("lodgement date"), DATE_FORMATTER);
                }
                order.add(new OrderRecord(
                        rowMap.get("contract number"),
                        rowMap.get("product code"),
                        rowMap.get("product model"),
                        Integer.parseInt(rowMap.get("quantity")),
                        estDeliveryDate,
                        lodgementDate,
                        Integer.parseInt(rowMap.get("salesman number"))
                ));
            }
        } catch (Exception e) {
            throw new RuntimeException("解析 CSV 文件失败", e);
        }
    }

    //    private void secondParse(List<ProductModelRecord> product, List<ContractRecord> contract) throws Exception {
//        // 与原代码相同，通过 CSV 文件解析其他表数据
//        try (CSVReader reader = new CSVReader(new FileReader(CSV_PATH))) {
//            System.out.println("open csv successfully");
//            String[] headers = reader.readNext();
//            String[] row;
//
//            while ((row = reader.readNext()) != null) {
//                Map<String, String> rowMap = toRowMap(headers, row);
//                // 提取各表数据（根据实际字段填充）
//                product.add(new ProductModelRecord(
//                        rowMap.get("product code"),
//                        rowMap.get("product model"),
//                        Integer.parseInt(rowMap.get("unit price"))
//                ));
//
//                contract.add(new ContractRecord(
//                        rowMap.get("contract number"),
//                        rowMap.get("client enterprise"),
//                        LocalDate.parse(rowMap.get("contract date"), DATE_FORMATTER)
//                ));
//            }
//        } catch (Exception e) {
//            throw new RuntimeException("解析 CSV 文件失败", e);
//        }
//    }
//
//    private void orderParse(List<OrderRecord> order) throws Exception {
//        // 与原代码相同，通过 CSV 文件解析其他表数据
//        try (CSVReader reader = new CSVReader(new FileReader(CSV_PATH))) {
//            System.out.println("open csv successfully");
//            String[] headers = reader.readNext();
//            String[] row;
//
//            while ((row = reader.readNext()) != null) {
//                Map<String, String> rowMap = toRowMap(headers, row);
//                // 提取各表数据（根据实际字段填充）
//                LocalDate estDeliveryDate;
//                LocalDate lodgementDate;
//                if (rowMap.get("estimated delivery date").equals("") || rowMap.get("estimated delivery date") == null) {
//                    estDeliveryDate = null;
//                } else {
//                    estDeliveryDate = LocalDate.parse(rowMap.get("estimated delivery date"), DATE_FORMATTER);
//                }
//                if (rowMap.get("lodgement date").equals("") || rowMap.get("lodgement date") == null) {
//                    lodgementDate = null;
//                } else {
//                    lodgementDate = LocalDate.parse(rowMap.get("lodgement date"), DATE_FORMATTER);
//                }
//
//                order.add(new OrderRecord(
//                        rowMap.get("contract number"),
//                        rowMap.get("product code"),
//                        rowMap.get("product model"),
//                        Integer.parseInt(rowMap.get("quantity")),
//                        estDeliveryDate,
//                        lodgementDate,
//                        Integer.parseInt(rowMap.get("salesman number"))
//                ));
//            }
//        } catch (Exception e) {
//            throw new RuntimeException("解析 CSV 文件失败", e);
//        }
//    }
    // 分批次工具方法
    private <T> List<List<T>> splitIntoBatches(List<T> list, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            int end = Math.min(i + batchSize, list.size());
            batches.add(list.subList(i, end));
        }
        return batches;
    }

    private <T> CompletableFuture<Void> insertAsync(
            String tableName,
            List<T> records,
            java.util.function.Function<T, String> uniqueKeyGetter
    ) {
        return CompletableFuture.runAsync(() -> {
            try (Connection hikariConn = dataSource.getConnection()) {
                long start = System.currentTimeMillis();
                Set<String> cache = ConcurrentHashMap.newKeySet();
                String sql = buildInsertSql(tableName);
                try (PreparedStatement stmt = hikariConn.prepareStatement(sql)) {
                    for (T record : records) {
                        String key = uniqueKeyGetter.apply(record);
                        if (cache.contains(key)) {
                            continue;
                        }
                        bindParameters(stmt, tableName, record);
                        stmt.addBatch();
                        cache.add(key);
                        if (cache.size() % 1000 == 0) stmt.executeBatch();
                    }
                    stmt.executeBatch();
                    hikariConn.commit();
                    System.out.println("插入数据 [" + tableName + "] 完成");
                } catch (SQLException e) {
                    hikariConn.rollback();
                    throw e;
                }
                long end = System.currentTimeMillis();
                System.out.println("插入数据 [" + tableName + "] 耗时: " + (end - start) / 1000.0 + " 秒");
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("插入失败: " + tableName, e);
            }
        }, executor);
    }

    //overload
    private <T> CompletableFuture<Void> insertAsync(
            String tableName,
            List<T> records,
            Executor executor
    ) {
        return CompletableFuture.runAsync(() -> {
            try (Connection hikariConn = dataSource.getConnection()) {
                String sql = buildInsertSql(tableName);
                int cnt = 1;
                try (PreparedStatement stmt = hikariConn.prepareStatement(sql)) {
                    for (T record : records) {
                        bindParameters(stmt, tableName, record);
                        stmt.addBatch();
                        if (cnt % 1000 == 0) stmt.executeBatch();
                        cnt++;
                    }
                    stmt.executeBatch();
                    hikariConn.commit();
                } catch (SQLException e) {
                    hikariConn.rollback();
                    throw e;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("插入失败: " + tableName, e);
            }
        }, executor);
    }

    //overload
    private <T> CompletableFuture<Void> insertAsync(
            String tableName,
            List<T> records
    ) {
        return CompletableFuture.runAsync(() -> {
            try (Connection hikariConn = dataSource.getConnection()) {
                String sql = buildInsertSql(tableName);
                int cnt = 1;
                try (PreparedStatement stmt = hikariConn.prepareStatement(sql)) {
                    for (T record : records) {
                        bindParameters(stmt, tableName, record);
                        stmt.addBatch();
                        if (cnt % 1000 == 0) stmt.executeBatch();
                        cnt++;
                    }
                    stmt.executeBatch();
                    hikariConn.commit();
                    System.out.println("插入数据 [" + tableName + "] 完成");
                } catch (SQLException e) {
                    hikariConn.rollback();
                    throw e;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("插入失败: " + tableName, e);
            }
        }, executor);
    }

    // ================ 辅助方法 ================
    private String buildInsertSql(String tableName) {
        // 根据表名返回 SQL（与原代码相同）
        switch (tableName) {
            case "client":
                return "INSERT INTO client (client_name, country, supply_center, city, industry) " +
                        "VALUES (?, ?, ?, ?, ?) ON CONFLICT (client_name) DO NOTHING";
            case "sales":
                return "INSERT INTO sales (salesman_number, salesman_name, gender, age, mobile_number) " +
                        "VALUES (?, ?, ?, ?, ?) ON CONFLICT (salesman_number) DO NOTHING";
            case "product":
                return "INSERT INTO product (product_code, product_name) " +
                        "VALUES (?, ?) ON CONFLICT (product_code) DO NOTHING";
            case "product_model":
                return "INSERT INTO product_model (product_code, product_model, unit_price) " +
                        "VALUES (?, ?, ?) ON CONFLICT (product_code, product_model) DO NOTHING";
            case "contract":
                return "INSERT INTO contract (contract_number, client_name, contract_date) " +
                        "VALUES (?, ?, ?) ON CONFLICT (contract_number) DO NOTHING";
            case "order_detail":
                return "INSERT INTO order_detail (contract_number, product_code, product_model, quantity, estimated_delivery_date, lodgement_date, salesman_number) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)";
            default:
                throw new IllegalArgumentException("未知表名: " + tableName);
        }
    }

    private void bindParameters(PreparedStatement stmt, String tableName, Object record) throws SQLException {
        // 与原代码相同
        switch (tableName) {
            case "client":
                ClientRecord client = (ClientRecord) record;
                stmt.setString(1, client.clientName);
                stmt.setString(2, client.country);
                stmt.setString(3, client.supplyCenter);
                stmt.setString(4, client.city);
                stmt.setString(5, client.industry);
                break;
            case "sales":
                SalesRecord sale = (SalesRecord) record;
                int salesmanNumber = Integer.parseInt(sale.salesmanNumber);
                int age = Integer.parseInt(sale.age);
                stmt.setInt(1, salesmanNumber);
                stmt.setString(2, sale.salesmanName);
                stmt.setString(3, sale.gender);
                stmt.setInt(4, age);
                stmt.setString(5, sale.mobilePhone);
                break;
            case "product":
                ProductRecord product = (ProductRecord) record;
                stmt.setString(1, product.productCode);
                stmt.setString(2, product.productName);
                break;
            case "product_model":
                ProductModelRecord productModel = (ProductModelRecord) record;
                stmt.setString(1, productModel.productCode);
                stmt.setString(2, productModel.productModel);
                stmt.setInt(3, productModel.unit_price);
                break;
            case "contract":
                ContractRecord contract = (ContractRecord) record;
                java.sql.Date sqlDate = java.sql.Date.valueOf(contract.contractDate);
                stmt.setString(1, contract.contractNumber);
                stmt.setString(2, contract.clientName);
                stmt.setDate(3, sqlDate);
                break;
            case "order_detail":
                OrderRecord order = (OrderRecord) record;
                if (order.estimated_delivery_date != null) {
                    stmt.setDate(5, java.sql.Date.valueOf(order.estimated_delivery_date));
                } else {
                    stmt.setNull(5, Types.DATE);  // 第5个参数设为 NULL
                }
                if (order.lodgement_date != null) {
                    stmt.setDate(6, java.sql.Date.valueOf(order.lodgement_date));
                } else {
                    stmt.setNull(6, Types.DATE);  // 第6个参数设为 NULL
                }
                stmt.setString(1, order.contractNumber);
                stmt.setString(2, order.productCode);
                stmt.setString(3, order.productModel);
                stmt.setInt(4, order.quantity);
                stmt.setInt(7, order.salesman_number);
                break;
        }
    }

    private Map<String, String> toRowMap(String[] headers, String[] row) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            map.put(headers[i], row[i]);
        }
        return map;
    }

    private static class ClientRecord extends Data {
        String clientName;
        String country;
        String supplyCenter;
        String city;
        String industry;

        public ClientRecord(String clientName, String country, String supplyCenter, String city, String industry) {
            this.clientName = clientName;
            this.country = country;
            this.supplyCenter = supplyCenter;
            this.city = city;
            this.industry = industry;
        }

        public String getClientName() {
            return clientName;
        }
        // 构造方法、Getter
    }

    private static class SalesRecord extends Data {
        String salesmanNumber;
        String salesmanName;
        String gender;
        String age;
        String mobilePhone;

        public SalesRecord(String salesmanNumber, String salesmanName, String gender, String age, String mobilePhone) {
            this.salesmanNumber = salesmanNumber;
            this.salesmanName = salesmanName;
            this.age = age;
            this.gender = gender;
            this.mobilePhone = mobilePhone;
        }

        public String getSalesmanNumber() {
            return salesmanNumber;
        }
        // 构造方法、Getter
    }

    private static class ProductRecord extends Data {
        String productCode;
        String productName;

        public ProductRecord(String productCode, String productName) {
            this.productCode = productCode;
            this.productName = productName;
        }

        public String getProductCode() {
            return productCode;
        }
    }

    private static class ProductModelRecord extends Data {
        String productCode;
        String productModel;
        int unit_price;

        public ProductModelRecord(String productCode, String productModel, int unit_price) {
            this.productCode = productCode;
            this.productModel = productModel;
            this.unit_price = unit_price;
        }
    }

    private static class ContractRecord extends Data {
        String contractNumber;
        String clientName;
        LocalDate contractDate;

        public ContractRecord(String contractNumber, String clientName, LocalDate contractDate) {
            this.contractNumber = contractNumber;
            this.clientName = clientName;
            this.contractDate = contractDate;
        }

        public String getContractNumber() {
            return contractNumber;
        }
    }

    private static class Data {
    }

    private static class OrderRecord {
        String contractNumber;
        String productCode;
        String productModel;
        int quantity;
        LocalDate estimated_delivery_date;
        LocalDate lodgement_date;
        int salesman_number;

        public OrderRecord(String contractNumber, String productCode, String productModel, int quantity, LocalDate estimated_delivery_date, LocalDate lodgement_date, int salesman_number) {
            this.contractNumber = contractNumber;
            this.productCode = productCode;
            this.productModel = productModel;
            this.quantity = quantity;
            this.estimated_delivery_date = estimated_delivery_date;
            this.lodgement_date = lodgement_date;
            this.salesman_number = salesman_number;
        }
    }
}