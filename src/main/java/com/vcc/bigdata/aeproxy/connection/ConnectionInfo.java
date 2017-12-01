package com.vcc.bigdata.aeproxy.connection;

import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.net.impl.NetSocketImpl;

public class ConnectionInfo {
    private NetSocket sock;
    private boolean isAuthed;

    public ConnectionInfo(NetSocket sock) {
        this.sock = sock;
        this.isAuthed = false;
    }

    public ConnectionInfo() {

    }

    public NetSocket getSock() {
        return sock;
    }

    public ConnectionInfo setSock(NetSocket sock) {
        this.sock = sock;
        return this;
    }

    public boolean isAuthed() {
        return isAuthed;
    }

    public ConnectionInfo setAuthed(boolean authed) {
        isAuthed = authed;
        return this;
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        SocketAddress remoteAddr = ((NetSocketImpl) sock).remoteAddress();
        SocketAddress localAddr = ((NetSocketImpl) sock).localAddress();
        json.put("remoteAddr", remoteAddr.host() + ":" + remoteAddr.port());
        json.put("localAddr", remoteAddr.host() + ":" + remoteAddr.port());
        json.put("authed", isAuthed);
        return json;
    }

    @Override
    public String toString() {
        return toJson().encodePrettily();
    }
}
