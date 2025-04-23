package javaImplementation;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Load {
    private static Connection conn = null;
    private static PreparedStatement stmt = null;
    private static HikariDataSource dataSource=null;

    //open database
    private static void openDB(Properties prop) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
            System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
            System.exit(1);
        }
        String url = "jdbc:postgresql://" + prop.getProperty("host") + "/" + prop.getProperty("database");
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setUsername(prop.getProperty("user"));
            config.setPassword(prop.getProperty("password"));
            config.setMaximumPoolSize(10);
            config.setAutoCommit(false);
            dataSource=new HikariDataSource(config);
            conn = DriverManager.getConnection(url, prop);
            if (conn != null) {
                System.out.println("Successfully connected to the database "
                        + prop.getProperty("database") + " as " + prop.getProperty("user"));
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    //load user properties
    private static Properties loadDBUser() {
        Properties properties = new Properties();
        try {
            properties.load(new InputStreamReader(new FileInputStream("resources/dbUser.properties")));
            return properties;
        } catch (IOException e) {
            System.err.println("can not find db user file");
            throw new RuntimeException(e);
        }
    }

    private static void closeDB() {
        if (conn != null) {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                conn.close();
                conn = null;
            } catch (Exception ignored) {
            }
        }
    }

    public static void main(String[] args) {
        Properties prop = loadDBUser();
        openDB(prop);
        PrepareTool prepareTool = new PrepareTool();
        prepareTool.truncateAllTables(conn);

        long start = System.currentTimeMillis();
        prepareTool.importSupplyCenter(conn);

        try {
            if (args[0].equals("0")) {
                System.out.println("concurrent mode");
                ConcurrentLoad concurrentLoad = new ConcurrentLoad(conn,dataSource);
                concurrentLoad.load();
            }
            else if(args[0].equals("1")){
                System.out.println("low load mode");
                LowLoad load = new LowLoad(conn,dataSource);
                load.load();
            }
            else {
                System.out.println("illegal argument");
            }
        }
        catch (Exception e) {
            System.err.println("加载数据失败: " + e.getMessage());
            e.printStackTrace();
            try {
                conn.rollback(); // 回滚事务
                System.out.println("事务已回滚");
            } catch (SQLException ex) {
                System.err.println("回滚失败: " + ex.getMessage());
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Loading speed : " + (end - start)/1000.0 + " s");

    }
}
