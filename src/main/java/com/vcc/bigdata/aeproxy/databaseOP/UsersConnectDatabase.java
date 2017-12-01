package com.vcc.bigdata.aeproxy.databaseOP;


import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

/**
 *
 * @author cuongnv
 * @since 18/4/2017
 *
 */

public class UsersConnectDatabase {

    /**
     * @author cuongnv
     * @param query
     *            Cau query voi tham so dau ?
     * @param params
     *            Gia tri tham so truyen vao
     * @param entityClass
     *            Class muon bieu diem du lieu truy van: Bien giong ten truong
     *            truy van tra ra va co get/set
     * @return List<entityClass>
     * @since 18/4/2017
     *
     */
    public <T> List<T> executeQuery(StringBuilder query, List<Object> params, Class<T> entityClass) {

        List<T> arrayT = new ArrayList<>();
        UsersCommonDataBase dataBase = UsersCommonDataBase.getInstance();
        Connection connect = dataBase.connectDataBase();
        if (connect == null) {
            return Collections.emptyList();
        }
        try {

            PreparedStatement stmt = (PreparedStatement) connect.prepareStatement(query.toString());
            if (stmt == null) {
                return Collections.emptyList();
            }
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    stmt.setObject(i + 1, params.get(i));
                }
            }
            ResultSet rs = stmt.executeQuery();
            Field[] entityFields = entityClass.getDeclaredFields();

            while (rs.next()) {
                JsonObject datasetItem = new JsonObject();
                for (Field f : entityFields) {
                    try {
                        String values = rs.getString(f.getName());
                        datasetItem.addProperty(f.getName(), values);
                    } catch (SQLException e) {

                    }
                }
                GsonBuilder builder = new GsonBuilder();
                Gson gson = builder.create();
                T item = gson.fromJson(datasetItem, entityClass);
                arrayT.add(item);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return Collections.emptyList();
        } finally {
            try {
                connect.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return arrayT;

    }

    /**
     * <b>thu thi tra ve 1 truong du lieu duy nhat hoac truong du lieu dau tien
     * cua ban ghi</b>
     *
     * @param query
     *            Cau query voi tham so dau ?
     *
     * @param params
     *            Gia tri tham so truyen vao
     * @return Object
     * @author cuongnv
     */
    public Object executeQuery(StringBuilder query, List<Object> params) {
        Object result = null;
        UsersCommonDataBase dataBase = UsersCommonDataBase.getInstance();
        Connection connect = dataBase.connectDataBase();
        if (connect == null) {
            return null;
        }
        try {

            PreparedStatement stmt = connect.prepareStatement(query.toString());
            if (stmt == null) {
                return null;
            }
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    stmt.setObject(i + 1, params.get(i));
                }
            }
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                result = rs.getObject(1);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                connect.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * <b>Thuc hien edit ban ghi</b>
     *
     * @param query
     * @param params
     * @return insert : Tra ve khoa duoc tao ra <br/>
     *         update/delete : so ban ghi thuc hien thanh cong
     * @author cuongnv
     */
    public int executeQueryDeleteOrUpdateOrInsert(StringBuilder query, List<Object> params) {
        int value = 0;
        UsersCommonDataBase dataBase = UsersCommonDataBase.getInstance();
        Connection connect = dataBase.connectDataBase();
        if (connect == null) {
            return 0;
        }
        try {
            PreparedStatement stmt = connect.prepareStatement(query.toString());
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    stmt.setObject(i + 1, params.get(i));
                }
            }
            value = stmt.executeUpdate();

        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                connect.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;
    }

    /**
     * <b>Thuc hien edit su dung transactios</b> <br/>
     * <b>Chi su dung voi thao tac nhieu ban ghi du lieu dam bao rollback
     * code</b><br/>
     * {@link #executeQueryDeleteOrUpdateOrInsert(StringBuilder, List)}
     *
     * @author cuongnv
     */
    @Deprecated
    public int executeTransactionQuery(StringBuilder query, List<Object> params, Connection connect)
            throws SQLException {
        int value = 0;
        if (connect == null) {
            return 0;
        }
        PreparedStatement stmt = connect.prepareStatement(query.toString());
        if (params != null) {
            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }
        }
        value = stmt.executeUpdate();
        if (value == 0) {
            return value;
        }
        ResultSet generatedKeys = stmt.getGeneratedKeys();
        if (generatedKeys.next()) {
            value = generatedKeys.getInt(1);
        }
        return value;
    }

}
