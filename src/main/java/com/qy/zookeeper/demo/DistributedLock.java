package com.qy.zookeeper.demo;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

/**
 * 基于Zookeeper的分布式锁
 * @author qy199
 *
 */
public class DistributedLock implements Runnable {

    private String    host_port;

    private ZooKeeper zk;

    private String    path;
    
    public DistributedLock(String host_port, String path) {
        super();
        this.host_port = host_port;
        this.path = path;
    }

    public boolean init() {
        boolean result = true;
        try {
            zk = new ZooKeeper(host_port, 60000, new GetLockWatch());
        } catch (IOException e) {
            e.printStackTrace();
            result = false;
        }

        return result;
    }

    @Override
    public void run() {
        boolean initFlag = init();
        if(initFlag) {
            try {
                
                while(States.CONNECTING.equals(zk.getState())) {  // 等待连接完成
                }
                // 异步处理分布式锁的获得
                zk.exists(path, true, new DistributedNodeCallBack(zk), null);
                
                while(!States.CLOSED.equals(zk.getState())) {  // 等待处理完成
                }
                System.out.println(Thread.currentThread().getName() + "已停止...");
                
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        }
    }

    class GetLockWatch implements Watcher {
        
        public GetLockWatch() {
            super();
        }

        @Override
        public void process(WatchedEvent event) {
            LocalDateTime now = LocalDateTime.now();
            String current = Thread.currentThread().getName() + " - " +
                             now.getYear() + "-" + 
                             now.getMonthValue() + "-" + 
                             now.getDayOfMonth() + " " +
                             now.getHour() + ":" +
                             now.getMinute() + ":" +
                             now.getSecond();
            
            String path = event.getPath();
            KeeperState state = event.getState();
            EventType type = event.getType();
            
            try {
                
                if(KeeperState.SyncConnected.equals(state)) {
                    
                    if(EventType.None.equals(type)) {
                        System.out.println(current + " - 收到节点 " + path + " watch通知...");
                        System.out.println(current + " - 连接状态：" + state);
                        System.out.println(current + " - 事件类型：" + type);
                        System.out.println(current + " - 已连接zk服务...");
                    } 
                        
                } else if(KeeperState.Expired.equals(state)) {
                    
                    System.out.println(current + " - 收到节点 " + path + " watch通知...");
                    System.out.println(current + " - 连接状态：" + state);
                    System.out.println(current + " - 事件类型：" + type);
                    System.out.println(current + " - 会话失效重新连接zk服务...");
                    zk.close(3000);
                    init();
                    
                } else if(KeeperState.Disconnected.equals(state)) {
                    
                    System.out.println(current + " - 收到节点 " + path + " watch通知...");
                    System.out.println(current + " - 连接状态：" + state);
                    System.out.println(current + " - 事件类型：" + type);
                    System.out.println(current + " - 与zk服务断开连接...");
                    zk.close();
                    
                } else if(KeeperState.Closed.equals(state)) {
                    
                    System.out.println(current + " - 收到节点 " + path + " watch通知...");
                    System.out.println(current + " - 连接状态：" + state);
                    System.out.println(current + " - 事件类型：" + type);
                    System.out.println(current + " - 客户端已关闭...");
                    
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            System.out.println();
        }

    }
}

/**
 * 异步处理分布式锁的获取
 * @author qy199
 *
 */
class DistributedNodeCallBack implements StatCallback {
    
    private ZooKeeper zk;
    
    private String nodeData;
    
    public DistributedNodeCallBack(ZooKeeper zk) {
        super();
        this.zk = zk;
        this.nodeData = Thread.currentThread().getName();
    }
    
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        LocalDateTime now = LocalDateTime.now();
        String current = Thread.currentThread().getName() + " - " +
                         now.getYear() + "-" + 
                         now.getMonthValue() + "-" + 
                         now.getDayOfMonth() + " " +
                         now.getHour() + ":" +
                         now.getMinute() + ":" +
                         now.getSecond();
        
        try {
            
            if(stat == null) {
                String schema = "digest";
                String id = "qy:123456";
                ACL acl = new ACL(Perms.READ, new Id(schema, DigestAuthenticationProvider.generateDigest(id)));
                List<ACL> acls = new ArrayList<ACL>();
                acls.add(acl);
                zk.create(path, nodeData.getBytes("utf-8"), acls, CreateMode.EPHEMERAL);
                System.out.println(current + " - 已获得节点锁" + path + "...");
                
                // 5S后释放锁
//                Thread.sleep(5000L);
//                zk.close();
                
            } else {
                // 等待节点锁被释放并重新获取
                zk.exists(path, true, this, ctx);
                System.out.println(current + " - 节点锁 " + path + " 已被占用...");
                Thread.sleep(1000L);
            }
            
        } catch (Exception e) {
            System.out.println(current + " - 获取锁失败...");
            try {
                // 等待节点锁被释放并重新获取
                Thread.sleep(1000L);
                zk.exists(path, true, this, ctx);
                
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        
        System.out.println();
    }
}