package com.qy.zookeeper.demo;


public class Test01 {

    public static void main(String[] args) {
        new Thread(new GetConf("10.40.248.253:2181", "/zookeeper/qy3")).start();
        new Thread(new GetConf("10.40.248.253:3181", "/zookeeper/qy3")).start();
    }
}
