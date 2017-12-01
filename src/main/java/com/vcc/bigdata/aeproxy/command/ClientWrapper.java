package com.vcc.bigdata.aeproxy.command;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;

public class ClientWrapper extends AerospikeClient {
    public ClientWrapper(String hostname, int port) throws AerospikeException {
        super(hostname, port);
    }

    public ClientWrapper(ClientPolicy policy, String hostname, int port) throws AerospikeException {
        super(policy, hostname, port);
    }

    public ClientWrapper(ClientPolicy policy, Host... hosts) throws AerospikeException {
        super(policy, hosts);
    }

    protected ClientWrapper(ClientPolicy policy) {
        super(policy);
    }
    public Cluster getCluster(){
        return this.cluster;
    }
    public Policy getPolicy(){
        return this.readPolicyDefault;
    }
}
