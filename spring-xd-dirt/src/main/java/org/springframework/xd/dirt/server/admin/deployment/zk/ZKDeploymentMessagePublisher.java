/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.xd.dirt.server.admin.deployment.zk;

import java.util.EnumSet;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.core.RuntimeTimeoutException;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentException;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentMessage;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentMessagePublisher;
import org.springframework.xd.dirt.zookeeper.Paths;


/**
 * ZooKeeper based {@link org.springframework.xd.dirt.server.admin.deployment.DeploymentMessagePublisher}
 * that sends the {@link org.springframework.xd.dirt.server.admin.deployment.DeploymentMessage} into
 * ZK distributed queue.
 *
 * @author Ilayaperumal Gopinathan
 */
public class ZKDeploymentMessagePublisher implements DeploymentMessagePublisher {

	public static final String SUCCESS = "success";

	public static final String ERROR = "error";

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private final DeploymentQueue deploymentQueue;

	/**
	 * todo; inject this
	 */
	private final long timeout = 900000;

	/**
	 * Construct the deployment message producer.
	 *
	 * @param deploymentQueue the deployment queue
	 */
	public ZKDeploymentMessagePublisher(DeploymentQueue deploymentQueue) {
		this.deploymentQueue = deploymentQueue;
	}

	private CuratorFramework getClient() {
		return this.deploymentQueue.getClient();
	}

	/**
	 * Produces the deployment message into ZK distributed queue.
	 *
	 * @param message the deployment message
	 */
	@Override
	public void publish(DeploymentMessage message) {
		try {
			this.deploymentQueue.getDistributedQueue().put(message);
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void poll(DeploymentMessage message) {
		CuratorFramework client = getClient();
		String requestId = message.getRequestId();
		Assert.hasText(requestId, "requestId for message required");
		ResultWatcher watcher = new ResultWatcher();
		String resultPath = Paths.build(Paths.DEPLOYMENTS, Paths.RESPONSES, requestId);

		try {
			logger.info("result path: {}", resultPath);
			client.create().creatingParentsIfNeeded().forPath(resultPath);
			client.getChildren().usingWatcher(watcher).forPath(resultPath);
			publish(message);

			long expiry = System.currentTimeMillis() + this.timeout;
			synchronized (this) {
				while (watcher.getState() == State.incomplete && System.currentTimeMillis() < expiry) {
					wait(this.timeout);
				}
			}
			switch (watcher.getState()) {
				case incomplete:
					throw new RuntimeTimeoutException("Timeout waiting for request to complete");  // todo better error message
				case error:
					throw new DeploymentException(watcher.getErrorDesc());
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		finally {
			try {
				client.delete().forPath(resultPath);
			}
			catch (Exception e) {
				// ignore
			}
		}

	}

	private enum State { incomplete, success, error}

	private class ResultWatcher implements CuratorWatcher {

		private State state = State.incomplete;

		private String errorDesc;

		public State getState() {
			return state;
		}

		public String getErrorDesc() {
			return errorDesc;
		}

		@Override
		public void process(WatchedEvent event) throws Exception {
			logger.info("event: {}", event);
			if (EnumSet.of(Watcher.Event.KeeperState.SyncConnected,
					Watcher.Event.KeeperState.SaslAuthenticated,
					Watcher.Event.KeeperState.ConnectedReadOnly).contains(event.getState())) {
				if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
					List<String> children = getClient().getChildren().forPath(event.getPath());
					Assert.state(children.size() == 1);
					synchronized (ZKDeploymentMessagePublisher.this) {
						if (children.contains(ERROR)) {
							errorDesc = new String(getClient().getData().forPath(Paths.build(event.getPath(), ERROR)));
							state = State.error;
						}
						else {
							state = State.success;
						}
						ZKDeploymentMessagePublisher.this.notifyAll();
					}
				}
				else {
					logger.debug("Ignoring event: {}", event);
				}
			}
		}
	}

}
