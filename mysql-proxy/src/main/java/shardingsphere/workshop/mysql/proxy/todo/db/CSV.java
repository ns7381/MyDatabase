package shardingsphere.workshop.mysql.proxy.todo.db;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CSV {
    private static final String DB_PATH = "E:\\github\\project\\workshop20200415\\mysql-proxy\\src\\main\\resources\\data\\";

    public static List<String> read(String tableName, String columnName) {
        List<String> result = new ArrayList<String>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(DB_PATH + tableName + ".csv"));
            String head = reader.readLine();
            String[] columns = head.split(",");
            int index = -1;
            for (int i = 0; i < columns.length; i++) {
                if (columnName.equalsIgnoreCase(columns[i])) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return null;
            }
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] item = line.split(",");

                result.add(item[index]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        read("t_order", "order_id");
    }
}
