package com.vcc.bigdata.aeproxy.databaseOP;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Giao tiếp với csdl user
 */
public class UserDAO {


    public boolean checkUser(String user, String pass) {
        return true;
    }

    public boolean checkUser(String user, String pass, int role) {
        return true;
    }

    public boolean checkUser(String user, int role) {
        return true;
    }

    public Map<String, String> getAllUsersAndPasswords() {
        UsersConnectDatabase db = new UsersConnectDatabase();
        Map<String, String> map = new HashMap<>();

        StringBuilder query = new StringBuilder();
        query.append("SELECT usr.usrname usrname, usr.passwrd passwrd, usr.role role ");
        query.append("FROM users usr ");

        List<EntityUsers> list = db.executeQuery(query, null, EntityUsers.class);
        if (list.isEmpty()) {
            return null;
        }
        for (EntityUsers entity : list) {
            map.put(entity.usrname, entity.passwrd);
        }

        return map;
    }

    public Map<String, Integer> getAllUsersAndRoles() {
        UsersConnectDatabase db = new UsersConnectDatabase();
        Map<String, Integer> map = new HashMap<>();

        StringBuilder query = new StringBuilder();
        query.append("SELECT usr.usrname usrname, usr.passwrd passwrd, usr.role role ");
        query.append("FROM users usr ");

        List<EntityUsers> list = db.executeQuery(query, null, EntityUsers.class);
        if (list.isEmpty()) {
            return null;
        }
        for (EntityUsers entity : list) {
            map.put(entity.usrname, entity.role);
        }

        return map;
    }

    public int addUser(String usr, String pwd, int role) {
        UsersConnectDatabase db = new UsersConnectDatabase();
        List<Object> params = new ArrayList<>();

        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO users (usrname, passwrd, role) ");
        query.append("VALUES (?, ?, ?)");
        params.add(usr);
        params.add(pwd);
        params.add(role);

        return db.executeQueryDeleteOrUpdateOrInsert(query, params);
    }

    public int updateUser(String usr, String pwd) {
        UsersConnectDatabase db = new UsersConnectDatabase();
        List<Object> params = new ArrayList<>();

        StringBuilder query = new StringBuilder();
        query.append("UPDATE users ");
        query.append("SET passwrd = ? ");
        query.append("WHERE usrname = ?");
        params.add(pwd);
        params.add(usr);

        return db.executeQueryDeleteOrUpdateOrInsert(query, params);
    }

    public int deleteUser(String usr) {
        UsersConnectDatabase db = new UsersConnectDatabase();
        List<Object> params = new ArrayList<>();

        StringBuilder query = new StringBuilder();
        query.append("DELETE FROM users ");
        query.append("WHERE usrname = ?");
        params.add(usr);

        return db.executeQueryDeleteOrUpdateOrInsert(query, params);
    }

    public int setRole4User(String usr, int role) {
        UsersConnectDatabase db = new UsersConnectDatabase();
        List<Object> params = new ArrayList<>();

        StringBuilder query = new StringBuilder();
        query.append("UPDATE users ");
        query.append("SET role = ? ");
        query.append("WHERE usrname = ? ");
        params.add(role);
        params.add(usr);

        return db.executeQueryDeleteOrUpdateOrInsert(query, params);
    }

}

class EntityUsers {
    public String usrname;
    public String passwrd;
    public Integer role;
}
