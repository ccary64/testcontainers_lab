package com.example.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_PORT = "5432";
    private static final String DEFAULT_USERNAME = "postgres";
    private static final String DEFAULT_PASSWORD = "postgres";
    private static final String DEFAULT_DATABASE = "postgres";

    public static Connection getConnection() throws SQLException {
        String host = System.getProperty("DB_HOST", System.getenv().getOrDefault("DB_HOST", DEFAULT_HOST));
        String port = System.getProperty("DB_PORT", System.getenv().getOrDefault("DB_PORT", DEFAULT_PORT));
        String username = System.getProperty("DB_USERNAME", System.getenv().getOrDefault("DB_USERNAME", DEFAULT_USERNAME));
        String password = System.getProperty("DB_PASSWORD", System.getenv().getOrDefault("DB_PASSWORD", DEFAULT_PASSWORD));
        String database = System.getProperty("DB_NAME", System.getenv().getOrDefault("DB_NAME", DEFAULT_DATABASE));

        String url;
        if ("h2".equals(System.getProperty("DB_TYPE"))) {
            url = "jdbc:h2:mem:" + database + ";DB_CLOSE_DELAY=-1";
            username = "sa";
            password = "";
        } else {
            url = String.format("jdbc:postgresql://%s:%s/%s", host, port, database);
        }
        return DriverManager.getConnection(url, username, password);
    }
}