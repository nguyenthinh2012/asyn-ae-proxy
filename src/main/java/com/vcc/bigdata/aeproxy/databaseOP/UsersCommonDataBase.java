package com.vcc.bigdata.aeproxy.databaseOP;


import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 *
 * @author cuongnv
 *
 */
public class UsersCommonDataBase {

    private final static String FILE = "resources/user.properties";

    private static UsersCommonDataBase connectDataBase;
    private static ComboPooledDataSource cpds;

    private String filename;

    private UsersCommonDataBase() {
        init();
    }

    private void init() {
        Properties prop = new Properties();
        try {
            FileInputStream fis = new FileInputStream(new File(FILE));
            prop.load(fis);
            fis.close();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e2) {
            e2.printStackTrace();
        }
        String usr = prop.getProperty("username");
        String pwd = prop.getProperty("password");
        String driver = prop.getProperty("driver");
        String host = prop.getProperty("host");
        String dbName = prop.getProperty("dbName");
        String appendCode = prop.getProperty("appendCode");
        String url = "jdbc:mysql://" + host + "/" + dbName + (appendCode != null ? appendCode : "");

        try {
            cpds = new ComboPooledDataSource();
            cpds.setDriverClass(driver);
            cpds.setUser(usr);
            cpds.setPassword(pwd);
            cpds.setJdbcUrl(url);
            cpds.setMinPoolSize(4);
            cpds.setInitialPoolSize(4);
            cpds.setAcquireIncrement(4);
            cpds.setMaxPoolSize(128);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
    }

    public static synchronized UsersCommonDataBase getInstance() {
        if (connectDataBase == null) {
            connectDataBase = new UsersCommonDataBase();
        }
        return connectDataBase;
    }

    public Connection connectDataBase() {
        Connection connection = null;
        try {
            connection = cpds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

}
