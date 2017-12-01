package com.vcc.bigdata.aeproxy.connection;

import com.vcc.bigdata.aeproxy.connection.ConnectionInfo;
import io.vertx.core.net.NetSocket;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionManager {
    private volatile Map <NetSocket, ConnectionInfo> clients;

    public ConnectionManager() {
        clients = new ConcurrentHashMap <NetSocket, ConnectionInfo>();
    }

    public void addClient(NetSocket sock) {
        ConnectionInfo info = new ConnectionInfo()
                .setSock(sock);
        info.setAuthed(true);
        clients.put(sock, info);
    }
    public int getSockNum(){
        return clients.size();
    }
    public boolean isContain(NetSocket sock){
        return clients.containsKey(sock);
    }
    public void remove(NetSocket sock){
        clients.remove(sock);
    }
    public ConnectionInfo getInfo(NetSocket sock) {
        return clients.get(sock);
    }
    public void setAuthen(NetSocket sock){
        ConnectionInfo ci = clients.get(sock);
        ci.setAuthed(true);
        clients.put(sock, ci);
    }
}
