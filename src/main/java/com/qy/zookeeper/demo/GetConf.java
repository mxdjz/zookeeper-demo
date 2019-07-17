package com.qy.zookeeper.demo;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

public class GetConf implements Runnable {

    private ZooKeeper zk;

    private String    host_port;

    private String    path;

    private boolean   interrupt = false;
    
    private String conf = "";

    public GetConf(String host_port, String path) {
        super();
        this.host_port = host_port;
        this.path = path;
    }

    public boolean init() {
        boolean result = true;
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            zk = new ZooKeeper(host_port, 3000, new GetConfWatcher(countDownLatch));
            waitUnitConnected(zk, countDownLatch);
        } catch (Exception e) {
            result = false;
            e.printStackTrace();
        }
        return result;
    }

    public void waitUnitConnected(ZooKeeper zk, CountDownLatch countDownLatch) {
        if (States.CONNECTING.equals(zk.getState())) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    

    @Override
    public void run() {
        boolean initFlag = init();
        
        // 启动线程执行获取
        if (initFlag) {
            try {
                
                byte[] data = zk.getData(this.path, true, null);
                conf = new String(data, "utf-8");
                while (!interrupt) {
                    System.out.println(LocalDateTime.now() + " - " + Thread.currentThread().getName() + " - " + conf);
                    Thread.sleep(5000L);
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            }
            

        }
    }

    class GetConfWatcher implements Watcher {
        /**
         * 阻塞非连接zookeeper线程以外的线程，直到连接zookeeper的线程连接成功为止
         */
        private CountDownLatch connectedLatch;

        public GetConfWatcher(CountDownLatch connectedLatch) {
            super();
            this.connectedLatch = connectedLatch;
        }

        @Override
        public void process(WatchedEvent event) {
            
            LocalDateTime now = LocalDateTime.now();
            String current = now.getYear() + "-" + 
                             now.getMonthValue() + "-" + 
                             now.getDayOfMonth() + " " +
                             now.getHour() + ":" +
                             now.getMinute() + ":" +
                             now.getSecond() + " - " + 
                             Thread.currentThread().getName();
            
            String path = event.getPath();
            KeeperState state = event.getState();
            EventType type = event.getType();
            
            System.out.println("收到watch通知...");
            System.out.println("连接状态：" + state);
            System.out.println("事件类型：" + type);
            
            try {
                
                if(KeeperState.SyncConnected.equals(state)) {
                    
                    if(EventType.None.equals(type)) {  // connected type
                        
                        System.out.println(current + " - 已连接zk服务...");
                        connectedLatch.countDown();
                        
                    } else if(EventType.NodeCreated.equals(type)) {  // create type 
                        
                        System.out.println(current + " - 创建一个节点...");
                        
                    } else if(EventType.NodeDataChanged.equals(type)) {  // update data type
                        
                        System.out.println(current + " - " + path + "节点数据更新...");
                        byte[] data = zk.getData(path, true, null);
                        conf = new String(data, "utf-8");
                        
                    } else if(EventType.NodeDeleted.equals(type)) {  // delete type
                        
                        System.out.println(current + " - " + path + "节点被删除");
                        
                    } else if(EventType.NodeChildrenChanged.equals(type)) {  // children node change type
                        
                        System.out.println(current + " - " + path + "的子节点被修改");
                        
                    }
                    
                    if(!EventType.NodeDataChanged.equals(type)) {
                        zk.exists(path, true);  // watch仅会触发一次，需要再次添加
                    }
                    
                } else if(KeeperState.AuthFailed.equals(state)) {
                    
                    System.out.println(current + " - 权限检查失败...");
                    
                } else if(KeeperState.Expired.equals(state)) {
                    
                    System.out.println(current + " - 会话已过期...");
                    zk.close(3000);
                    init();
                    
                } else if(KeeperState.Disconnected.equals(state)) {
                    
                    System.out.println(current + " - 与ZK服务断开连接...");
                    interrupt = true;
                    zk.close();
                    
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            System.out.println();
            
        }

    }
}
