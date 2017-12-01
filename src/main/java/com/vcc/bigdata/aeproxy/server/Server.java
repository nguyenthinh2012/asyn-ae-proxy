package com.vcc.bigdata.aeproxy.server;

import com.sun.org.apache.xpath.internal.operations.Bool;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Server {
    public static final Logger LOG = LogManager.getLogger(Server.class);
    public static void main(String[] args) {
        Properties p = new Properties();
        String host = "";
        int port = 5000;
        int poolSize = 32;
        boolean reusePort = false;
        boolean tcpFastOpen = false;
        boolean tcpCork = false;
        boolean tcpQuickAck = false;
        try
        {
            InputStream inp = new FileInputStream(new File("resources/config.properties"));
            p.load(inp);
            host = p.getProperty("host");
            port = Integer.parseInt(p.getProperty("port"));
            reusePort = Boolean.parseBoolean(p.getProperty("reuseport"));
            tcpFastOpen = Boolean.parseBoolean(p.getProperty("tcpfastopen"));
            tcpCork = Boolean.parseBoolean(p.getProperty("tcpcork"));
            tcpQuickAck = Boolean.parseBoolean(p.getProperty("tcpquickack"));
        }
        catch (Exception e){
            e.printStackTrace();
        }

        int cores = Runtime.getRuntime().availableProcessors();
        poolSize = cores * 2;
        Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(2).setPreferNativeTransport(true));
        vertx.deployVerticle(ProxyVerticle.class.getName(), new DeploymentOptions().setInstances(2));

    }
}
