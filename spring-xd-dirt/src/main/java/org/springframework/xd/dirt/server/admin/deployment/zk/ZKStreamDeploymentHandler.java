/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.xd.dirt.server.admin.deployment.zk;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.NoContainerException;
import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.core.StreamDeploymentsPath;
import org.springframework.xd.dirt.server.admin.deployment.ContainerMatcher;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentException;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentUnitStateCalculator;
import org.springframework.xd.dirt.server.admin.deployment.ModuleDeploymentStatus;
import org.springframework.xd.dirt.server.admin.deployment.StreamRuntimePropertiesProvider;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;

/**
 * Deployment handler that is responsible for deploying Stream.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class ZKStreamDeploymentHandler extends ZKDeploymentHandler {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(ZKStreamDeploymentHandler.class);

	/**
	 * Factory to construct {@link org.springframework.xd.dirt.core.Stream} instance
	 */
	@Autowired
	private StreamFactory streamFactory;

	/**
	 * Matcher that applies container matching criteria
	 */
	@Autowired
	private ContainerMatcher containerMatcher;

	/**
	 * Repository for the containers
	 */
	@Autowired
	private ContainerRepository containerRepository;

	/**
	 * Utility that writes module deployment requests to ZK path
	 */
	@Autowired
	private ModuleDeploymentWriter moduleDeploymentWriter;

	/**
	 * Deployment unit state calculator
	 */
	@Autowired
	private DeploymentUnitStateCalculator stateCalculator;


	public void deploy(String streamName) throws DeploymentException {
		CuratorFramework client = zkConnection.getClient();
		try {
			deployStream(client, DeploymentLoader.loadStream(client, streamName, streamFactory));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new DeploymentException(String.format("Deployment of stream '%s' was interrupted", streamName), e);
		}
		catch (Exception e) {
			throw new DeploymentException(String.format("Exception while deploying stream '%s'", streamName), e);
		}
	}

	/**
	 * Issue deployment requests for the modules of the given stream.
	 *
	 * @param stream stream to be deployed
	 *
	 * @throws InterruptedException
	 */
	private void deployStream(CuratorFramework client, Stream stream) throws InterruptedException {
		// Ensure that the path for modules used by the container to write
		// ephemeral nodes exists. The presence of this path is assumed
		// by the supervisor when it calculates stream state when it is
		// assigned leadership. See XD-2170 for details.
		try {
			client.create().creatingParentsIfNeeded().forPath(
					Paths.build(Paths.STREAM_DEPLOYMENTS, stream.getName(), Paths.MODULES));
		}
		catch (Exception e) {
			ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NodeExistsException.class);
		}

		String statusPath = Paths.build(Paths.STREAM_DEPLOYMENTS, stream.getName(), Paths.STATUS);

		// assert that the deployment status has been correctly set to "deploying"
		DeploymentUnitStatus deployingStatus = null;
		try {
			deployingStatus = new DeploymentUnitStatus(ZooKeeperUtils.bytesToMap(
					client.getData().forPath(statusPath)));
		}
		catch (Exception e) {
			// an exception indicates that the status has not been set
		}
		Assert.state(deployingStatus != null
						&& deployingStatus.getState() == DeploymentUnitStatus.State.deploying,
				String.format("Expected 'deploying' status for stream '%s'; current status: %s",
						stream.getName(), deployingStatus));

		try {
			Collection<ModuleDeploymentStatus> deploymentStatuses = new ArrayList<ModuleDeploymentStatus>();
			DefaultModuleDeploymentPropertiesProvider deploymentPropertiesProvider =
					new DefaultModuleDeploymentPropertiesProvider(stream);
			for (Iterator<ModuleDescriptor> descriptors = stream.getDeploymentOrderIterator(); descriptors.hasNext(); ) {
				ModuleDescriptor descriptor = descriptors.next();
				ModuleDeploymentProperties deploymentProperties = deploymentPropertiesProvider.propertiesForDescriptor(descriptor);

				// write out all of the required modules for this stream (including runtime properties);
				// this does not actually perform a deployment...this data is used in case there are not
				// enough containers to deploy the stream
				StreamRuntimePropertiesProvider partitionPropertiesProvider =
						new StreamRuntimePropertiesProvider(stream, deploymentPropertiesProvider);
				int moduleCount = deploymentProperties.getCount();
				if (moduleCount == 0) {
					createModuleDeploymentRequestsPath(client, descriptor,
							partitionPropertiesProvider.propertiesForDescriptor(descriptor));
				}
				else {
					for (int i = 0; i < moduleCount; i++) {
						createModuleDeploymentRequestsPath(client, descriptor,
								partitionPropertiesProvider.propertiesForDescriptor(descriptor));
					}
				}

				try {
					// find the containers that can deploy these modules
					Collection<Container> containers = containerMatcher.match(descriptor, deploymentProperties,
							containerRepository.findAll());

					// write out the deployment requests targeted to the containers obtained above;
					// a new instance of StreamPartitionPropertiesProvider is created since this
					// object is responsible for generating unique sequence ids for modules
					StreamRuntimePropertiesProvider deploymentRuntimeProvider =
							new StreamRuntimePropertiesProvider(stream, deploymentPropertiesProvider);

					deploymentStatuses.addAll(moduleDeploymentWriter.writeDeployment(
							descriptor, deploymentRuntimeProvider, containers));
				}
				catch (NoContainerException e) {
					logger.warn("No containers available for deployment of module '{}' for stream '{}'",
							descriptor.getModuleLabel(), stream.getName());
				}
			}

			DeploymentUnitStatus status = stateCalculator.calculate(stream, deploymentPropertiesProvider,
					deploymentStatuses);
			logger.info("Deployment status for stream '{}': {}", stream.getName(), status);

			client.setData().forPath(statusPath, ZooKeeperUtils.mapToBytes(status.toMap()));
		}
		catch (InterruptedException e) {
			throw e;
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
	}

	@Override
	public void undeploy(String id) throws DeploymentException {
		logger.info("Undeploying stream {}", id);

		String streamDeploymentPath = Paths.build(Paths.STREAM_DEPLOYMENTS, id);
		String streamModuleDeploymentPath = Paths.build(streamDeploymentPath, Paths.MODULES);
		CuratorFramework client = zkConnection.getClient();
		Deque<String> paths = new ArrayDeque<String>();

		try {
			client.setData().forPath(
					Paths.build(Paths.STREAM_DEPLOYMENTS, id, Paths.STATUS),
					ZooKeeperUtils.mapToBytes(new DeploymentUnitStatus(
							DeploymentUnitStatus.State.undeploying).toMap()));
		}
		catch (Exception e) {
			logger.warn("Exception while transitioning stream {} state to {}", id,
					DeploymentUnitStatus.State.undeploying, e);
		}

		// Place all module deployments into a tree keyed by the
		// ZK transaction id. The ZK transaction id maintains
		// total ordering of all changes. This allows the
		// undeployment of modules in the reverse order in
		// which they were deployed.

		/*
		 * TODO: ordering by TX id is not a reliable means to determine
		 * the order in which modules should be undeployed because
		 * a source module may be redeployed during the lifetime
		 * of the stream. Instead we should re-load the stream
		 * and determine the correct module ordering.
		 */

		Map<Long, String> txMap = new TreeMap<Long, String>();
		try {
			List<String> deployments = client.getChildren().forPath(streamModuleDeploymentPath);
			for (String deployment : deployments) {
				String path = new StreamDeploymentsPath(Paths.build(streamModuleDeploymentPath, deployment)).build();
				Stat stat = client.checkExists().forPath(path);
				Assert.notNull(stat);
				txMap.put(stat.getCzxid(), path);
			}
		}
		catch (Exception e) {
			//NoNodeException - nothing to delete
			ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NoNodeException.class);
		}

		for (String deployment : txMap.values()) {
			paths.add(deployment);
		}

		for (Iterator<String> iterator = paths.descendingIterator(); iterator.hasNext();) {
			try {
				String path = iterator.next();
				logger.trace("removing path {}", path);
				client.delete().deletingChildrenIfNeeded().forPath(path);
			}
			catch (Exception e) {
				ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NoNodeException.class);
			}
		}

		try {
			client.delete().deletingChildrenIfNeeded().forPath(streamDeploymentPath);
		}
		catch (KeeperException.NotEmptyException e) {
			List<String> children = new ArrayList<String>();
			try {
				children.addAll(client.getChildren().forPath(streamModuleDeploymentPath));
			}
			catch (Exception ex) {
				children.add("Could not load list of children due to " + ex);
			}
			throw new IllegalStateException(String.format(
					"The following children were not deleted from %s: %s", streamModuleDeploymentPath, children), e);
		}
		catch (Exception e) {
			ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NoNodeException.class);
		}

		super.undeploy(id);
	}

	@Override
	protected String getDeploymentPath() {
		return Paths.STREAM_DEPLOYMENTS;
	}
}
