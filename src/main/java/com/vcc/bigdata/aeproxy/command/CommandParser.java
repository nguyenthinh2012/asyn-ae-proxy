package com.vcc.bigdata.aeproxy.command;

import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import io.vertx.core.buffer.Buffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class CommandParser {
    private ClientWrapper client;
    private String host;
    private Node node;
    public static final Logger LOG = LogManager.getLogger(WrapperCommand.class);

    public CommandParser(ClientWrapper client, String host) {
        this.client = client;
        this.host = host;
        this.node = getNode(client.getCluster());
    }
    private Node getNode(Cluster cluster) {

        // return node corresponding with address 192.168.23.37
        Node[] nodes = cluster.getNodes();
        // return cluster.getRandomNode().getConnection(10000);
        for (Node n : nodes) {
            if (n.getHost().name.equals(host)) {
//                 LOG.info(n.getHost() );
                return n;
            }
        }
        // return first node
        LOG.info("No node!!!");
        return null;
    }

    public WrapperCommand parse(Buffer buf, Map<String, Integer> userAuthen, Map<String, String> userPro) {
        //read header
        WrapperCommand cmd = new WrapperCommand(client.getCluster(),
                client.readPolicyDefault, host, node, userAuthen, userPro);
        int type = buf.getByte(1);
        cmd.setType(type)
                .setBuf(buf);
        return cmd;
    }
}
