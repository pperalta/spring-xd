/*
 * Copyright 2011-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.NoContainerException;
import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.core.DeploymentUnit;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.ModuleDeploymentRequestsPath;
import org.springframework.xd.dirt.core.ResourceDeployer;
import org.springframework.xd.dirt.server.admin.deployment.ContainerMatcher;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentStrategy;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentUnitStateCalculator;
import org.springframework.xd.dirt.server.admin.deployment.ModuleDeploymentPropertiesProvider;
import org.springframework.xd.dirt.server.admin.deployment.ModuleDeploymentStatus;
import org.springframework.xd.dirt.server.admin.deployment.zk.DefaultModuleDeploymentPropertiesProvider;
import org.springframework.xd.dirt.server.admin.deployment.zk.ModuleDeploymentWriter;
import org.springframework.xd.dirt.server.admin.deployment.zk.SupervisorElectedEvent;
import org.springframework.xd.dirt.server.admin.deployment.zk.SupervisorElectionListener;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.RuntimeModuleDeploymentProperties;
import org.springframework.xd.rest.domain.support.DeploymentPropertiesFormat;

/**
 * Abstract implementation of the @link {@link org.springframework.xd.dirt.core.ResourceDeployer} interface. It provides
 * the basic support for calling CrudRepository methods and sending deployment messages.
 *
 * @author Luke Taylor
 * @author Mark Pollack
 * @author Eric Bottard
 * @author Andy Clement
 * @author David Turanski
 */
public final class ZooKeeperResourceDeployer implements ResourceDeployer, SupervisorElectionListener {

	private static final Logger logger = LoggerFactory.getLogger(ZooKeeperResourceDeployer.class);

	/**
	 * Pattern used for parsing a single deployment property key. Group 1 is the module name, Group 2 is the
	 * deployment property name.
	 */
	private static final Pattern DEPLOYMENT_PROPERTY_PATTERN = Pattern.compile("module\\.([^\\.]+)\\.([^=]+)");

	@Autowired
	private ZooKeeperConnection zkConnection;

	@Autowired
	private XDParser parser;

	@Autowired
	private DeploymentValidator validator;

	/**
	 * Cache of children under the module deployment requests path.
	 */
	private PathChildrenCache moduleDeploymentRequests;

	private final DeploymentStrategy deploymentStrategy;

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


	public ZooKeeperResourceDeployer(DeploymentStrategy deploymentStrategy) {
		this.deploymentStrategy = deploymentStrategy;
	}

	@Override
	public void deploy(DeploymentUnit deploymentUnit) {
		String name = deploymentUnit.getName();
		logger.info("Deploying {}", name);

		// todo: since the deployment unit has already been
		// loaded, we probably don't need to worry about this
//		this.validator.validateBeforeDeploy(name, properties);

		String deploymentPath = this.deploymentStrategy.getDeploymentPath(name);
		CuratorFramework client = this.zkConnection.getClient();

		// Ensure that the path for modules used by the container to write
		// ephemeral nodes exists. The presence of this path is assumed
		// by the supervisor when it calculates deployment unit state when it is
		// assigned leadership. See XD-2170 for details.
		try {
			client.create().creatingParentsIfNeeded().forPath(Paths.build(deploymentPath, Paths.MODULES));
		}
		catch (Exception e) {
			ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NodeExistsException.class);
		}

		String statusPath = Paths.build(deploymentPath, Paths.STATUS);
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
				String.format("Expected 'deploying' status for %s '%s'; current status: %s",
						this.deploymentStrategy.getParsingContext().toString(), name, deployingStatus));

		try {
			Collection<ModuleDeploymentStatus> deploymentStatuses = new ArrayList<ModuleDeploymentStatus>();
			DefaultModuleDeploymentPropertiesProvider deploymentPropertiesProvider =
					new DefaultModuleDeploymentPropertiesProvider(deploymentUnit);
			for (Iterator<ModuleDescriptor> descriptors = deploymentUnit.getDeploymentOrderIterator();
				 descriptors.hasNext();) {
				ModuleDescriptor descriptor = descriptors.next();
				ModuleDeploymentProperties deploymentProperties =
						deploymentPropertiesProvider.propertiesForDescriptor(descriptor);

				// write out all of the required modules for this unit (including runtime properties);
				// this does not actually perform a deployment...this data is used in case there are not
				// enough containers to deploy the unit
				ModuleDeploymentPropertiesProvider<RuntimeModuleDeploymentProperties> runtimePropertiesProvider =
						this.deploymentStrategy.runtimePropertiesProvider(deploymentUnit, deploymentPropertiesProvider);

				int moduleCount = deploymentProperties.getCount();
				if (moduleCount == 0) {
					createModuleDeploymentRequestsPath(client, descriptor,
							runtimePropertiesProvider.propertiesForDescriptor(descriptor));
				}
				else {
					for (int i = 0; i < moduleCount; i++) {
						createModuleDeploymentRequestsPath(client, descriptor,
								runtimePropertiesProvider.propertiesForDescriptor(descriptor));
					}
				}

				try {
					// find the containers that can deploy these modules
					Collection<Container> containers = containerMatcher.match(descriptor, deploymentProperties,
							containerRepository.findAll());

					// write out the deployment requests targeted to the containers obtained above;
					// a new instance of the properties provider is created since this
					// object is responsible for generating unique sequence ids for modules
					ModuleDeploymentPropertiesProvider<RuntimeModuleDeploymentProperties> deploymentRuntimeProvider =
							this.deploymentStrategy.runtimePropertiesProvider(deploymentUnit, deploymentPropertiesProvider);

					deploymentStatuses.addAll(moduleDeploymentWriter.writeDeployment(
							descriptor, deploymentRuntimeProvider, containers));
				}
				catch (NoContainerException e) {
					logger.warn("No containers available for deployment of module '{}' for {} '{}'",
							descriptor.getModuleLabel(), this.deploymentStrategy.getParsingContext(), name);
				}
			}

			// todo: seems that status write should happen in a finally...
			DeploymentUnitStatus status = stateCalculator.calculate(deploymentUnit, deploymentPropertiesProvider,
					deploymentStatuses);
			logger.info("Deployment status for {} '{}': {}", this.deploymentStrategy.getParsingContext(), name, status);

			client.setData().forPath(statusPath, ZooKeeperUtils.mapToBytes(status.toMap()));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw ZooKeeperUtils.wrapThrowable(e);
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}

	}

	@Override
	public void undeploy(DeploymentUnit deploymentUnit) {
		String name = deploymentUnit.getName();
		logger.info("Undeploying {}", name);
		validator.validateBeforeUndeploy(name);

		String deploymentPath = this.deploymentStrategy.getDeploymentPath(name);
		CuratorFramework client = zkConnection.getClient();

		try {
			client.setData().forPath(
					Paths.build(deploymentPath, Paths.STATUS),
					ZooKeeperUtils.mapToBytes(new DeploymentUnitStatus(
							DeploymentUnitStatus.State.undeploying).toMap()));
		}
		catch (Exception e) {
			logger.warn("Exception while transitioning {} state to {}", deploymentUnit,
					DeploymentUnitStatus.State.undeploying, e);
		}

		// todo: while this works for streams and jobs, it will not undeploy
		// stream modules in the correct order; this can only be done by loading
		// the stream in order to sort the modules by undeployment order
		try {
			client.delete().deletingChildrenIfNeeded()
					.forPath(deploymentPath);
		}
		catch (Exception e) {
			//NoNodeException - nothing to delete
			ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NoNodeException.class);
		}

		Assert.notNull(moduleDeploymentRequests, "moduleDeploymentRequests == null");
		ModuleDeploymentRequestsPath path;
		for (ChildData requestedModulesData : moduleDeploymentRequests.getCurrentData()) {
			path = new ModuleDeploymentRequestsPath(requestedModulesData.getPath());
			if (path.getDeploymentUnitName().equals(name)) {
				try {
					zkConnection.getClient().delete().deletingChildrenIfNeeded().forPath(path.build());
				}
				catch (Exception e) {
					throw ZooKeeperUtils.wrapThrowable(e);
				}
			}
		}
	}

	@Override
	public void undeployAll() {
		// todo: maybe instead of unddeployAll, we need some way to
		// return a list of deployed units?

		throw new UnsupportedOperationException();

//		try {
//			List<String> children = zkConnection.getClient().getChildren().forPath(
//					this.deploymentStrategy.getDeploymentsPath());
//			for (String child : children) {
//				undeploy(child);
//			}
//		}
//		catch (Exception e) {
//			//NoNodeException - nothing to delete
//			ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NoNodeException.class);
//		}
	}

	/**
	 * Return the ZooKeeper connection.
	 *
	 * @return the ZooKeeper connection
	 */
	protected ZooKeeperConnection getZooKeeperConnection() {
		return zkConnection;
	}

	/**
	 * Create {@link org.springframework.xd.dirt.core.ModuleDeploymentRequestsPath} for the given
	 * {@link org.springframework.xd.module.ModuleDescriptor} and
	 * the {@link org.springframework.xd.module.RuntimeModuleDeploymentProperties}.
	 *
	 * @param client the curator client
	 * @param descriptor the module descriptor
	 * @param deploymentProperties the runtime deployment properties
	 */
	protected void createModuleDeploymentRequestsPath(CuratorFramework client, ModuleDescriptor descriptor,
			RuntimeModuleDeploymentProperties deploymentProperties) {
		// Create and set the data for the requested modules path
		String requestedModulesPath = new ModuleDeploymentRequestsPath()
				.setDeploymentUnitName(descriptor.getGroup())
				.setModuleType(descriptor.getType().toString())
				.setModuleLabel(descriptor.getModuleLabel())
				.setModuleSequence(deploymentProperties.getSequenceAsString())
				.build();
		try {
			client.create().creatingParentsIfNeeded().forPath(requestedModulesPath,
					ZooKeeperUtils.mapToBytes(deploymentProperties));
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
	}

	/**
	 * Prepares a deployment by persisting the deployment data (including formatted
	 * deployment properties) and current deployment state of "deploying".
	 *
	 * @see org.springframework.xd.dirt.core.DeploymentUnitStatus.State#deploying
	 */
	@Deprecated
	public void prepareDeployment(String name, Map<String, String> properties) {
		// ***** TODO *****
		// This method creates the deployment path (i.e. /xd/deployments/streams/<name>)
		// and writes out the properties - this is a pre-requisite for instantiating
		// a DeploymentUnit via the Stream/Job factories.
		// It's a hack for now until we figure out a better way to instantiate
		// DeploymentUnits.

		Assert.hasText(name, "name cannot be blank or null");
		logger.trace("Preparing deployment of '{}'", name);

		try {
			String deploymentPath = this.deploymentStrategy.getDeploymentPath(name);

			logger.warn("deployment strategy: {}", deploymentStrategy);
			logger.warn("deployment path:     {}", deploymentPath);

			String statusPath = Paths.build(deploymentPath, Paths.STATUS);
			byte[] propertyBytes = DeploymentPropertiesFormat.formatDeploymentProperties(properties).getBytes("UTF-8");
			byte[] statusBytes = ZooKeeperUtils.mapToBytes(
					new DeploymentUnitStatus(DeploymentUnitStatus.State.deploying).toMap());

			zkConnection.getClient().inTransaction()
					.create().forPath(deploymentPath, propertyBytes).and()
					.create().withMode(CreateMode.EPHEMERAL).forPath(statusPath, statusBytes).and()
					.commit();
			logger.trace("Deployment state of '{}' set to 'deploying'", name);
		}
		catch (KeeperException.NodeExistsException e) {
			throw new AlreadyDeployedException(name,
					String.format("The %s named '%%s' is already deployed", this.deploymentStrategy.getParsingContext()));
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
	}

	@Override
	public void onSupervisorElected(SupervisorElectedEvent supervisorElectedEvent) {
		this.moduleDeploymentRequests = supervisorElectedEvent.getModuleDeploymentRequests();
	}

	@Override
	public DeploymentUnitStatus getDeploymentStatus(String id) {
		String path = Paths.build(this.deploymentStrategy.getDeploymentPath(id), Paths.STATUS);
		byte[] statusBytes = null;

		try {
			statusBytes = zkConnection.getClient().getData().forPath(path);
		}
		catch (Exception e) {
			// missing node means this unit has not been deployed
			ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NoNodeException.class);
		}

		return (statusBytes == null)
				? new DeploymentUnitStatus(DeploymentUnitStatus.State.undeployed)
				: new DeploymentUnitStatus(ZooKeeperUtils.bytesToMap(statusBytes));
	}

}
