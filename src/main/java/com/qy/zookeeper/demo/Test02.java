package com.qy.zookeeper.demo;


public class Test02 {
    
    public static void main(String[] args) throws Exception {
        String host_port = "10.40.248.253:2181";
        String zk_path = "/zookeeper/qy_lock";
        for(int i = 0; i < 2; i++) {
            new Thread(new DistributedLock(host_port, zk_path)).start();
        }
    }
}
