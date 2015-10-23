package com.ickes.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.Gson;

/**
 * @author Ickes
 * @since v.1.0
 */
public class ZookeeperSupport {
	// 连接超时时间
	private static final int SESSION_TIMEOUT = 30000;
	private static final String ZKHOST = "192.168.177.131:2281,192.168.177.132:2281,192.168.177.133:2281";

	private ZooKeeper zooKeeper;

	public void connect() {
		try {
			zooKeeper = new ZooKeeper(ZKHOST, SESSION_TIMEOUT, new Watcher() {
				public void process(WatchedEvent event) {
					System.out.println("process : " + event.getPath());
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * <pre>
	 * 创建节点
	 * Sample:create("/application");
	 * </pre>
	 * 
	 * @param path
	 *            路径
	 * @return
	 */
	public String create(String path, byte[] datas) {
		String result = null;
		try {
			/**
			 * p1:创建的节点路径 p2:ACL访问控制,OPEN_ACL_UNSAFE允许客户所有操作， p3:创建类型
			 * 
			 */
			result = zooKeeper.create(path, datas, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 根据节点路径获取节点数据
	 * 
	 * @param path
	 *            节点数据
	 * @return
	 */
	public String getNodeData(String path) {
		String result = null;
		try {
			// 参数： 路径、监视器、数据版本等信息
			byte[] bytes = zooKeeper.getData(path, null, null);
			result = new String(bytes);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 删除节点
	 */
	public void delete(String path) {
		try {
			/**
			 * -1,任意版本都会删除
			 */
			zooKeeper.delete(path, -1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	public Stat isExists(String path) {
		Stat stat = null;
		try {
			stat = zooKeeper.exists(path, false);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return stat;
	}

	public Stat setNodeDate(String path, byte[] datas) {
		Stat stat = null;
		/**
		 * 设置某个节点下面的数据，-1表示匹配所有版本
		 */
		try {
			stat = zooKeeper.setData(path, datas, -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return stat;

	}
	/**
	 * 获取某个目录下面所有子节点数据
	 * @param path
	 * @return
	 */
	public List<String> getChildNodeData(String path) {
		List<String> list = null;
		try {
			list = zooKeeper.getChildren(path, true);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return list;
	}

	public void close() {
		try {
			zooKeeper.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		ZookeeperSupport zk = new ZookeeperSupport();
		zk.connect();
		// zk.delete("/app");
		// zk.create("/app","ickes.iteye.com".getBytes());
		// zk.create("/app/aa","aa".getBytes());
		// zk.create("/app/bb","bb".getBytes());
		Stat stat = zk.isExists("/app");
		Gson g = new Gson();
		System.out.println(g.toJson(stat));
	}

}
