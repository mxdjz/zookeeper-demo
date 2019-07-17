package com.qy.zookeeper.demo;

import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

public class Demo01 implements Watcher {

    /**
     * 阻塞非连接zookeeper线程以外的线程，直到连接zookeeper的线程连接成功为止
     */
    private CountDownLatch   connectedLatch;
    private ZooKeeper        zk;

    private static final int TIME_OUT = 60000;

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public boolean init() throws Exception {
        boolean result = true;
        try {
            
            zk = new ZooKeeper("10.40.248.253:2181", TIME_OUT, this);
            connectedLatch = new CountDownLatch(1);
            waitUnitConnected(zk, connectedLatch);

        } catch (Exception e) {
            e.printStackTrace();
            result = false;
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        Demo01 bootstrap = new Demo01();
        long start = System.currentTimeMillis();
        boolean init = bootstrap.init();
        long end = System.currentTimeMillis();
        System.out.println("connect time: " + (end - start) + "ms");
        if (init) {
            System.out.println();
            
            bootstrap.getZk().exists("/zookeeper/qy_lock", true, new ExistsNodeCallBack(bootstrap.getZk()), null);
//            List<String> children = bootstrap.getZk().getChildren("/zookeeper", true);

//            bootstrap.getZk().addAuthInfo("digest", "qy:12345".getBytes());
            
//            bootstrap.getZk().exists("/zookeeper/qy7", true, null, null);
//            for (String string : children) {
//                
//                System.out.println("node: " + string);
//                bootstrap.getZk().exists("/zookeeper/" + string, true, null, null);
////                 byte[] data = bootstrap.getZk().getData("/zookeeper/" + string, true, null);
////                 System.out.println("node data: " + new String(data, "utf-8"));
////                bootstrap.getZk().getData("/zookeeper/" + string, true, new getDataCallBack(bootstrap.getZk()), null);
//                System.out.println();
//            }
        } else {
            throw new Exception("connect fail...");
        }

         Thread.sleep(Long.MAX_VALUE);
        // bootstrap.getZk().close(10000);
    }

    /**
     * 等待与zk服务器连接成功
     * @param zk
     * @param countDownLatch
     */
    public static void waitUnitConnected(ZooKeeper zk, CountDownLatch countDownLatch) {
        if (States.CONNECTING.equals(zk.getState())) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 默认watch处理方法，在构造zookeeper时传入该实现
     */
    @Override
    public void process(WatchedEvent event) {
        LocalDateTime now = LocalDateTime.now();
        String current = now.getYear() + "-" + 
                         now.getMonthValue() + "-" + 
                         now.getDayOfMonth() + " " +
                         now.getHour() + ":" +
                         now.getMinute() + ":" +
                         now.getSecond();
        
        String path = event.getPath();
        KeeperState state = event.getState();
        EventType type = event.getType();
        
        System.out.println("收到节点: " + path + " 的watch通知...");
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
                    System.out.println(current + " - 更新后的数据：" + new String(data, "utf-8"));
                    
                } else if(EventType.NodeDeleted.equals(type)) {  // delete type
                    
                    System.out.println(current + " - " + path + "节点被删除");
                    System.out.println(current + " - 临时节点" + path + "已被删除");
                    
                } else if(EventType.NodeChildrenChanged.equals(type)) {  // children node change type
                    
                    System.out.println(current + " - " + path + "的子节点被修改");
                    
                }
                
                if(!EventType.None.equals(type) && !EventType.NodeDataChanged.equals(type)) {
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
                zk.close();
                
            } else if(KeeperState.Closed.equals(state)) {
                
                System.out.println(current + " - 客户端已关闭...");
                
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        System.out.println();
    }

}

/**
 * 异步接收数据实现
 * @author qy199
 *
 */
class getDataCallBack implements DataCallback {
    
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        try {
            String dataStr = new String(data, "utf-8");
            System.out.println(LocalDateTime.now() + " - " + path + " - " + dataStr);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        };
    }

}

class ExistsNodeCallBack implements StatCallback {
    
    private ZooKeeper zk;
    
    public ExistsNodeCallBack(ZooKeeper zk) {
        super();
        this.zk = zk;
    }

    
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        LocalDateTime now = LocalDateTime.now();
        String current = now.getYear() + "-" + 
                         now.getMonthValue() + "-" + 
                         now.getDayOfMonth() + " " +
                         now.getHour() + ":" +
                         now.getMinute() + ":" +
                         now.getSecond() + " - " + 
                         Thread.currentThread().getName();
        try {
            
            if(stat == null) {
                zk.create(path, "lock".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                System.out.println(current + " - 获得节点锁...");
                
//                Thread.sleep(5000L);
//                zk.close();
            } else {
                zk.exists(path, true, this, ctx);
                System.out.println(current + " - 节点锁已被占用...");
            }
            
        } catch (Exception e) {
            zk.exists(path, true, this, ctx);
            System.out.println(current + " - 节点锁获取失败...");
        }
    }
    
}