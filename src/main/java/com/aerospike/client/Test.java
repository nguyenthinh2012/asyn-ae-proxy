package com.aerospike.client;


import com.aerospike.client.admin.Role;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;
import io.vertx.core.cli.CLI;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

class threadWrite1 extends  Thread{
    Host[] hosts = {new Host("10.5.36.30",5000),
            new Host("10.5.36.42",5000),
            new Host("10.5.36.43",5000)};
    @Override
    public void run() {
        super.run();

        AerospikeClient client = new AerospikeClient(null, hosts);
        //   Info.request(client.cluster.getNodes()[0].getConnection(2000), "peers-clear-std");
//

        for(long i = 0 ; i < 1000000 ; i ++){
            Key k = new Key("hadoop","testdata",i);
            Bin b = new Bin("aa", i *100000000 * 1000000_000 + "abc");
            client.put(null, k, b);
           //  System.out.println(client.get(null, k).bins.values());

        }
        System.out.println("ok1");
    }
}
class threadRead extends  Thread{
    Host[] hosts = {new Host("10.5.36.30",5000),
            new Host("10.5.36.42",5000),
            new Host("10.5.36.43",5000)};
    @Override
    public void run() {
        super.run();
        AerospikeClient client = new AerospikeClient(null, hosts);
        //   Info.request(client.cluster.getNodes()[0].getConnection(2000), "peers-clear-std");
//

        for(int i = 0 ; i < 1000000 ; i ++){
            Key k = new Key("hadoop","testdata",i);
            Bin b = new Bin("aa",i + "xyz");
//            client.put(null, k, b);

             System.out.println(client.get(null, k).bins.values());

        }
        System.out.println("ok2");
    }
}
public class Test {
    public static void main(String[] args){
//        String s = "abc";
//        System.out.println("$2a$10$7EqJtq98hPqEX7fNZaFWoOYfVUs94gD2f55OKlAbxubc/KUOj3Lb6".length());
        AdminPolicy cp = new AdminPolicy();
        ClientPolicy cl = new ClientPolicy();
        cl.user = "admin";
        cl.password = "123456";
        AerospikeClient client = new AerospikeClient(cl,"127.0.0.1", 5000);
//        ArrayList<String> al = new ArrayList<>();
//        al.add("2");

//        Info.request(client.cluster.getNodes()[0].getConnection(10000), "service");
        Key k = new Key("hadoop","testdata","sdxxcbxcvab");
        Bin b = new Bin("aa",1000 + "xyz");
        client.put(null, k, b);
//        System.out.println(client.get(null, k));
//        System.out.println(client.get(null, k).bins.values());
//        threadRead t = new threadRead();
//        threadWrite1 t1 = new threadWrite1();
//        t.start();
//        t1.start();
    }
}
