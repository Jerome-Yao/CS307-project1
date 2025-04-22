import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class PrepareTool {
    public void truncateAllTables(Connection conn) {
        // 定义清空顺序（从子表到父表，避免外键约束）
        String[] tables = {
                "order_detail",
                "contract",
                "client",
                "product_model",
                "product",
                "sales",
                "supply_center"  // 最后清空父表
        };

        try (Statement stmt = conn.createStatement()) {
            // 禁用外键检查（PostgreSQL 不支持，需按顺序清空）
            for (String table : tables) {
                stmt.executeUpdate("TRUNCATE TABLE " + table + " CASCADE"); // CASCADE 自动处理依赖
            }
            conn.commit();
        }
        catch (SQLException e) {
            System.err.println("Error while truncating tables: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("All tables truncated successfully.");
    }

    public void importSupplyCenter(Connection conn){
        String[][] SUPPLY_CENTER_DATA = {
                {"Europe", "Qian Qianqiu"},
                {"America", "Kong Yibo"},
                {"Asia", "David Robinson"},
                {"Eastern China", "You Xiangxing"},
                {"Northern China", "Jiang Feiqing"},
                {"Southern China", "Liz Jones"},
                {"Southwestern China", "Zheng Jiaxuan"},
                {"Hong Kong, Macao and Taiwan regions of China", "Gaston Harris"}
        };
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE supply_center CASCADE");
            conn.commit();
        }
        catch (SQLException e) {
            System.err.println("Error while truncating supply_center table: " + e.getMessage());
            e.printStackTrace();
        }

        // 插入硬编码数据
        String sql = "INSERT INTO supply_center (center_name, director) VALUES (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (String[] row : SUPPLY_CENTER_DATA) {
                pstmt.setString(1, row[0]);  // center_name
                pstmt.setString(2, row[1]);  // director
                pstmt.addBatch();            // 添加到批处理
            }
            pstmt.executeBatch();            // 执行批量插入
            conn.commit();
        }
        catch (SQLException e) {
            System.err.println("Error while inserting supply_center data: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Supply center data imported successfully.");
    }
}
