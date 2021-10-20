package com.starrocks.connector.flink;

import com.starrocks.connector.flink.exception.HttpException;
import com.starrocks.connector.flink.manager.StarRocksSourceManager;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

public class StarRocksSourceTest {

    @Test
    public void testSourceManager() {

        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://172.26.92.152:9632")
                .withProperty("http-nodes", "172.26.92.152:8634,172.26.92.152:8634,172.26.92.152:8634")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("table-name", "test_1")
                .withProperty("database-name", "cjs_test")
                .withProperty("columns", "col1, event_day")
                .withProperty("filter", "col1 = 0")
                .build();

        StarRocksSourceManager manager = new StarRocksSourceManager(options);
        try {
            manager.startToRead();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("ClassNotFoundException");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IOException");
        } catch (HttpException e) {
            e.printStackTrace();
            System.out.println("HttpException");
        }
    }
}
