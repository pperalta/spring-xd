package org.springframework.xd.dirt.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by pperalta on 7/2/14.
 */
public class StreamStatusCache {
	private static final int STREAM_NAME_NODE = 3;

	private static final int STATUS_NODE = 4;

	private final ZooKeeperConnection zkConnection;

	private final MyCallback callback = new MyCallback();

	private final MyWatcher watcher = new MyWatcher();

	private final ConcurrentMap<String, DeploymentUnitStatus> mapStatus =
			new ConcurrentHashMap<String, DeploymentUnitStatus>();

	private enum Context { INITIAL, STATUS_QUERY }

	public StreamStatusCache(ZooKeeperConnection zkConnection) {
		this.zkConnection = zkConnection;
	}

	public void start() {
		try {
			zkConnection.getClient().getChildren().usingWatcher(watcher)
					.inBackground(callback, Context.INITIAL)
					.forPath(Paths.build(Paths.STREAM_DEPLOYMENTS));
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
	}

	public void stop() {
		mapStatus.clear();
	}

	private String getStreamName(String[] elements) {
//		String name = elements[STREAM_NAME_NODE];
		return "ticktock";
	}

	private class MyWatcher implements CuratorWatcher {

		@Override
		public void process(WatchedEvent event) throws Exception {
			System.out.println("event: " + event);

			String path = event.getPath();
			String[] elements = path.split("/");
			System.out.println(Arrays.asList(elements));

			// stream added/removed
			switch (event.getType()) {
				case NodeCreated:
				case NodeChildrenChanged:
					zkConnection.getClient().getData().usingWatcher(watcher)
							.inBackground(callback, Context.STATUS_QUERY)
							.forPath(Paths.build(Paths.STREAM_DEPLOYMENTS, getStreamName(elements), Paths.STATUS));
					break;
				case NodeDeleted:
					mapStatus.remove(getStreamName(elements));
					break;
			}

			//event.getType()
			/*
			None (-1),
            NodeCreated (1),
            NodeDeleted (2),
            NodeDataChanged (3),
            NodeChildrenChanged (4);

			 */

			// if a new stream was added, grab the stream name and status
			// if status
		}
	}

	private class MyCallback implements BackgroundCallback  {

		@Override
		public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
			Context context = (Context) event.getContext();

			switch (context) {
				case INITIAL:
					for (String node : event.getChildren()) {
						client.getData().usingWatcher(watcher)
								.inBackground(callback, Context.STATUS_QUERY)
								.forPath(Paths.build(Paths.STREAM_DEPLOYMENTS, node, Paths.STATUS));
					}
					break;
				case STATUS_QUERY:
					String path = event.getPath();
					String[] elements = path.split("/");
					String streamName = getStreamName(elements);

					byte[] data = event.getData();
					if (data == null) {
						client.getData().usingWatcher(watcher)
								.inBackground(callback, Context.STATUS_QUERY)
								.forPath(Paths.build(Paths.STREAM_DEPLOYMENTS, streamName, Paths.STATUS));
					}
					else {
						DeploymentUnitStatus status = new DeploymentUnitStatus(ZooKeeperUtils.bytesToMap(data));
						System.out.println(status);
						mapStatus.put(streamName, status);
					}
					break;
			}
		}
	}


	public static void main(String[] args) throws Exception {
		ZooKeeperConnection connection = new ZooKeeperConnection("localhost:2181");
		connection.start();

		StreamStatusCache cache = new StreamStatusCache(connection);
		cache.start();

		while (true) {
			Thread.sleep(2000);
			System.out.println("");
			System.out.println("Cache content");
			System.out.println("----------");
			System.out.println(cache.mapStatus);
		}
	}
}
