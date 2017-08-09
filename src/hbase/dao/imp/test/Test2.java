package hbase.dao.imp.test;

import java.io.IOException;

import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
public class Test2 {

	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		Watcher watcher = new Watcher() {
            public void process(WatchedEvent event) {
				System.out.println(event.toString());
			}
             
        };
         
        ZooKeeper zk = new ZooKeeper("192.168.1.31:2181", 3000, watcher);
//        System.out.println("====创建节点");
//        zk.create("/cjw", "znode1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        System.out.println("====查看节点是否安装成功");
//        System.out.println(new String(zk.getData("/cjw", false, null)));
//        System.out.println("====修改节点的数据");
//        zk.setData("/cjw", "cjw2015".getBytes(), -1);
//        System.out.println("====查看修改的节点是否成功");
//        System.out.println(new String(zk.getData("/cjw", false, null)));
        System.out.println("====删除节点");
        zk.delete("/cjw", -1);
        System.out.println("====查看节点是否被删除");
        System.out.println("节点状态：" + zk.exists("/cjw", false));
        RecoverableZooKeeper zk2 = new RecoverableZooKeeper(null, 0, watcher, 0, 0);
      Long sjs=  zk2.getSessionId();
        System.out.println("long :"+ sjs);
        zk.close();
	}
}
