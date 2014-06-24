/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.xd.dirt.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.ContainerMatcher;
import org.springframework.xd.dirt.cluster.RedeploymentContainerMatcher;
import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.core.DeploymentUnit;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.Job;
import org.springframework.xd.dirt.core.JobDeploymentsPath;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.core.StreamDeploymentsPath;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.util.MapBytesUtility;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.ModuleType;


/**
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public abstract class ModuleRedeployer {

	/**
	 * Logger.
	 */
	protected final Logger logger = LoggerFactory.getLogger(ModuleRedeployer.class);

	/**
	 * Utility to convert maps to byte arrays.
	 */
	protected final MapBytesUtility mapBytesUtility = new MapBytesUtility();

	/**
	 * Container matcher for matching modules to containers.
	 */
	protected final ContainerMatcher containerMatcher;

	/**
	 * Utility for writing module deployment requests to ZooKeeper.
	 */
	protected final ModuleDeploymentWriter moduleDeploymentWriter;

	/**
	 * Utility for loading streams and jobs (including deployment metadata).
	 */
	protected final DeploymentLoader deploymentLoader = new DeploymentLoader();

	/**
	 * Stream factory.
	 */
	protected final StreamFactory streamFactory;

	/**
	 * Job factory.
	 */
	protected final JobFactory jobFactory;

	/**
	 * {@link Converter} from {@link ChildData} in stream deployments to Stream name.
	 *
	 * @see #streamDeployments
	 */
	protected final DeploymentNameConverter deploymentNameConverter = new DeploymentNameConverter();

	/**
	 * Cache of children under the job deployment path.
	 */
	protected final PathChildrenCache jobDeployments;

	/**
	 * Cache of children under the module deployment requests path.
	 */
	protected final PathChildrenCache moduleDeploymentRequests;

	/**
	 * Cache of children under the stream deployment path.
	 */
	protected final PathChildrenCache streamDeployments;

	/**
	 * State calculator for stream/job state.
	 */
	private final DeploymentUnitStateCalculator stateCalculator;

	public ModuleRedeployer(ZooKeeperConnection zkConnection,
			ContainerRepository containerRepository,
			StreamFactory streamFactory, JobFactory jobFactory,
			PathChildrenCache streamDeployments, PathChildrenCache jobDeployments,
			PathChildrenCache moduleDeploymentRequests, ContainerMatcher containerMatcher,
			DeploymentUnitStateCalculator stateCalculator) {
		this.containerMatcher = containerMatcher;
		this.moduleDeploymentWriter = new ModuleDeploymentWriter(zkConnection,
				containerRepository, containerMatcher);
		this.streamFactory = streamFactory;
		this.jobFactory = jobFactory;
		this.streamDeployments = streamDeployments;
		this.jobDeployments = jobDeployments;
		this.moduleDeploymentRequests = moduleDeploymentRequests;
		this.stateCalculator = stateCalculator;
	}

	protected abstract void redeployModules(CuratorFramework client, Container container) throws Exception;

	/**
	 * Return true if the {@link org.springframework.xd.dirt.cluster.Container}
	 * is allowed to deploy the {@link org.springframework.xd.module.ModuleDescriptor}
	 * given the criteria present in the
	 * {@link org.springframework.xd.module.ModuleDeploymentProperties}.
	 *
	 * @param container target container
	 * @param descriptor module descriptor
	 * @param properties deployment properties
	 * @return true if the container is allowed to deploy the module
	 */
	protected boolean isCandidateForDeployment(final Container container, ModuleDescriptor descriptor,
			ModuleDeploymentProperties properties) {
		return !CollectionUtils.isEmpty(containerMatcher.match(descriptor, properties,
				Collections.singletonList(container)));
	}

	/**
	 * Determine which containers, if any, have deployed a module for a stream.
	 *
	 * @param client curator client
	 * @param descriptor module descriptor
	 *
	 * @return list of containers that have deployed this module; empty
	 * list is returned if no containers have deployed it
	 *
	 * @throws Exception thrown by Curator
	 */
	private List<String> getContainersForStreamModule(CuratorFramework client, ModuleDescriptor descriptor)
			throws Exception {
		List<String> containers = new ArrayList<String>();
		String moduleType = descriptor.getModuleDefinition().getType().toString();
		String moduleLabel = descriptor.getModuleLabel();
		String moduleDeploymentPath = Paths.build(Paths.STREAM_DEPLOYMENTS, descriptor.getGroup(), Paths.MODULES);
		try {
			List<String> moduleDeployments = client.getChildren().forPath(moduleDeploymentPath);
			for (String moduleDeployment : moduleDeployments) {
				StreamDeploymentsPath path = new StreamDeploymentsPath(
						Paths.build(moduleDeploymentPath, moduleDeployment));
				if (path.getModuleType().equals(moduleType)
						&& path.getModuleLabel().equals(moduleLabel)) {
					containers.add(path.getContainer());
				}
			}
		}
		catch (KeeperException.NoNodeException e) {
			// stream has not been (or is no longer) deployed
		}
		return containers;
	}

	/**
	 * Determine which containers, if any, have deployed job module of the given job.
	 *
	 * @param client curator client
	 * @param descriptor module descriptor
	 *
	 * @return list of containers that have deployed this module; empty
	 * list is returned if no containers have deployed it
	 *
	 * @throws Exception thrown by Curator
	 */
	protected List<String> getContainersForJobModule(CuratorFramework client, ModuleDescriptor descriptor)
			throws Exception {
		List<String> containers = new ArrayList<String>();
		//String moduleType = descriptor.getModuleDefinition().getType().toString();
		String moduleLabel = descriptor.getModuleLabel();
		String moduleDeploymentPath = Paths.build(Paths.JOB_DEPLOYMENTS, descriptor.getGroup(), Paths.MODULES);
		try {
			List<String> moduleDeployments = client.getChildren().forPath(moduleDeploymentPath);
			for (String moduleDeployment : moduleDeployments) {
				JobDeploymentsPath path = new JobDeploymentsPath(
						Paths.build(moduleDeploymentPath, moduleDeployment));
				if (path.getModuleLabel().equals(moduleLabel)) {
					containers.add(path.getContainer());
				}
			}
		}
		catch (KeeperException.NoNodeException e) {
			// job has not been (or is no longer) deployed
		}
		return containers;
	}

	/**
	 * Return a {@link org.springframework.xd.dirt.cluster.ContainerMatcher} that
	 * will <b>exclude</b> containers that are already hosting the module.
	 *
	 * @param client            curator client
	 * @param moduleDescriptor  containers that have deployed this module
	 *                          will be excluded from the results returned
	 *                          by the {@code ContainerMatcher}.
	 * @return container matcher that will return containers not hosting
	 *         the module
	 * @throws Exception
	 */
	protected ContainerMatcher instantiateContainerMatcher(CuratorFramework client,
			ModuleDescriptor moduleDescriptor) throws Exception {
		Collection<String> containers = (moduleDescriptor.getType() == ModuleType.job)
				? getContainersForJobModule(client, moduleDescriptor)
				: getContainersForStreamModule(client, moduleDescriptor);

		return new RedeploymentContainerMatcher(containerMatcher, containers);
	}

	/**
	 * Log unwanted re-deployment of the module if the module count is less
	 * than or equal to zero.
	 * @param criteria the criteria for the module deployment
	 * @param moduleLabel the module label
	 */
	protected void logUnwantedRedeployment(String criteria, String moduleLabel) {
		StringBuilder builder = new StringBuilder();
		builder.append("Module '").append(moduleLabel).append("' is targeted to all containers");
		if (StringUtils.hasText(criteria)) {
			builder.append(" matching criteria '").append(criteria).append('\'');
		}
		builder.append("; it does not need to be redeployed");
		logger.info(builder.toString());
	}

	/**
	 * Issue a module deployment request for the provided module using
	 * the provided container matcher. This also transitions the deployment
	 * unit state to {@link DeploymentUnitStatus.State#deploying}. Once
	 * the deployment attempt completes, the status for the deployment unit
	 * should be updated via {@link #updateDeploymentUnitState} using the
	 * {@link ModuleDeploymentStatus} returned from this method.
	 *
	 * @param client             curator client
	 * @param moduleDeployment   contains module redeployment details such as
	 *                           stream, module descriptor, and deployment properties
	 * @param containerMatcher   matches modules to containers
	 * @return result of module deployment request
	 * @throws Exception
	 * @see #transitionToDeploying
	 */
	protected ModuleDeploymentStatus deployModule(CuratorFramework client,
			ModuleDeployment moduleDeployment, ContainerMatcher containerMatcher) throws Exception {
		transitionToDeploying(client, moduleDeployment.deploymentUnit);

		return moduleDeploymentWriter.writeDeployment(moduleDeployment.moduleDescriptor,
				moduleDeployment.moduleSequence,
				moduleDeployment.deploymentProperties, containerMatcher);
	}

	/**
	 * Issue a module deployment request for the provided module to
	 * the provided container. This also transitions the deployment
	 * unit state to {@link DeploymentUnitStatus.State#deploying}. Once
	 * the deployment attempt completes, the status for the deployment unit
	 * should be updated via {@link #updateDeploymentUnitState} using the
	 * {@link ModuleDeploymentStatus} returned from this method.
	 *
	 * @param client             curator client
	 * @param moduleDeployment   contains module redeployment details such as
	 *                           stream, module descriptor, and deployment properties
	 * @param container          target container for module deployment
	 * @return result of module deployment request
	 * @throws Exception
	 * @see #transitionToDeploying
	 */
	protected ModuleDeploymentStatus deployModule(CuratorFramework client,
			ModuleDeployment moduleDeployment, final Container container) throws Exception {
		transitionToDeploying(client, moduleDeployment.deploymentUnit);
		ContainerMatcher matcher = new ContainerMatcher() {

			@Override
			public Collection<Container> match(ModuleDescriptor moduleDescriptor,
					ModuleDeploymentProperties deploymentProperties,
					Iterable<Container> containers) {
				return Collections.singleton(container);
			}
		};
		return moduleDeploymentWriter.writeDeployment(moduleDeployment.moduleDescriptor,
				moduleDeployment.moduleSequence,
				moduleDeployment.deploymentProperties, matcher);
	}

	/**
	 * Transitions the deployment unit state to {@link DeploymentUnitStatus.State#deploying}.
	 * This transition should occur before making a module deployment attempt.
	 *
	 * @param client         curator client
	 * @param deploymentUnit deployment unit that contains a module to be deployed
	 * @throws Exception
	 * @see #deployModule
	 * @see #updateDeploymentUnitState
	 */
	private void transitionToDeploying(CuratorFramework client, DeploymentUnit deploymentUnit) throws Exception {
		String pathPrefix = (deploymentUnit instanceof Stream)
				? Paths.STREAM_DEPLOYMENTS
				: Paths.JOB_DEPLOYMENTS;

		client.setData().forPath(
				Paths.build(pathPrefix, deploymentUnit.getName(), Paths.STATUS),
				ZooKeeperUtils.mapToBytes(new DeploymentUnitStatus(
						DeploymentUnitStatus.State.deploying).toMap()));
	}

	/**
	 * Return a <em>mutable</em> collection of {@link ModuleDeploymentStatus module statuses}
	 * for all of the modules that comprise the provided {@link DeploymentUnit}. This
	 * information is obtained from ZooKeeper via the ephemeral nodes created
	 * by the individual containers that have deployed these modules.
	 * <p />
	 * This collection is used (and modified) in {@link #updateDeploymentUnitState}.
	 *
	 * @param client          curator client
	 * @param deploymentUnit  deployment unit for which to return the individual
	 *                        module statuses
	 * @return mutable collection of status objects
	 * @throws Exception
	 * @see #updateDeploymentUnitState
	 */
	private Collection<ModuleDeploymentStatus> aggregateState(CuratorFramework client,
			DeploymentUnit deploymentUnit) throws Exception {
		Assert.state(deploymentUnit instanceof Stream || deploymentUnit instanceof Job);
		String pathPrefix = (deploymentUnit instanceof Stream)
				? Paths.STREAM_DEPLOYMENTS
				: Paths.JOB_DEPLOYMENTS;

		String path = Paths.build(pathPrefix, deploymentUnit.getName(), Paths.MODULES);
		List<String> modules = client.getChildren().forPath(path);
		Collection<ModuleDeploymentStatus> results = new ArrayList<ModuleDeploymentStatus>();
		for (String module : modules) {
			String deploymentUnitName;
			ModuleType type;
			String label;
			String moduleSequence;
			String container;

			if (deploymentUnit instanceof Stream) {
				StreamDeploymentsPath streamDeploymentsPath = new StreamDeploymentsPath(Paths.build(path, module));
				deploymentUnitName = streamDeploymentsPath.getStreamName();
				Assert.state(deploymentUnitName.equals(deploymentUnit.getName()));
				type = ModuleType.valueOf(streamDeploymentsPath.getModuleType());
				label = streamDeploymentsPath.getModuleLabel();
				moduleSequence = streamDeploymentsPath.getModuleSequence();
				container = streamDeploymentsPath.getContainer();
			}
			else {
				JobDeploymentsPath jobDeploymentsPath = new JobDeploymentsPath(Paths.build(path, module));
				deploymentUnitName = jobDeploymentsPath.getJobName();
				Assert.state(deploymentUnitName.equals(deploymentUnit.getName()));
				type = ModuleType.job;
				label = jobDeploymentsPath.getModuleLabel();
				moduleSequence = jobDeploymentsPath.getModuleSequence();
				container = jobDeploymentsPath.getContainer();
			}

			ModuleDescriptor.Key moduleDescriptorKey = new ModuleDescriptor.Key(deploymentUnitName, type, label);
			results.add(new ModuleDeploymentStatus(container, moduleSequence, moduleDescriptorKey,
					ModuleDeploymentStatus.State.deployed, null));
		}

		return results;
	}

	/**
	 * Update the ZooKeeper ephemeral node that indicates the status for the
	 * provided deployment unit.
	 *
	 * @param client            curator client
	 * @param moduleDeployment  module that a redeploy was attempted for
	 * @param deploymentStatus  deployment status for the module; may be null
	 *                          if a module deployment was not attempted
	 *                          (for example if there were no containers
	 *                          available for deployment)
	 * @throws Exception
	 */
	protected void updateDeploymentUnitState(CuratorFramework client, ModuleDeployment moduleDeployment,
			ModuleDeploymentStatus deploymentStatus) throws Exception {
		final DeploymentUnit deploymentUnit = moduleDeployment.deploymentUnit;
		final ModuleDescriptor moduleDescriptor = moduleDeployment.moduleDescriptor;

		Collection<ModuleDeploymentStatus> aggregateStatuses = aggregateState(client, deploymentUnit);

		// If the module deployment was successful, it will be present in this
		// list; remove it to avoid duplication since the deployment status
		// that was returned from the deployment request will be used instead.
		// This is especially important if the deployment failed because
		// that deployment status will contain the error condition that
		// caused the failure.
		if (deploymentStatus != null) {
			for (Iterator<ModuleDeploymentStatus> iterator = aggregateStatuses.iterator(); iterator.hasNext();) {
				ModuleDeploymentStatus status = iterator.next();
				if (logger.isTraceEnabled()) {
					logger.trace("module deployment status: {}", status);
					logger.trace("deploymentStatus: {}", deploymentStatus);
				}

				if (status.getKey().getLabel().equals(moduleDescriptor.getModuleLabel())
						&& status.getContainer().equals(deploymentStatus.getContainer())) {
					iterator.remove();
				}
			}
			aggregateStatuses.add(deploymentStatus);
		}

		Assert.state(deploymentUnit instanceof Stream || deploymentUnit instanceof Job);
		boolean isStream = (deploymentUnit instanceof Stream);

		ModuleDeploymentPropertiesProvider provider = (isStream)
				? new StreamDeploymentListener.StreamModuleDeploymentPropertiesProvider((Stream) deploymentUnit)
				: new JobDeploymentListener.JobModuleDeploymentPropertiesProvider((Job) deploymentUnit);

		DeploymentUnitStatus status = stateCalculator.calculate(deploymentUnit, provider, aggregateStatuses);

		logger.info("Deployment state for {} '{}': {}",
				isStream ? "stream" : "job", deploymentUnit.getName(), status);

		client.setData().forPath(
				Paths.build(isStream ? Paths.STREAM_DEPLOYMENTS : Paths.JOB_DEPLOYMENTS,
						deploymentUnit.getName(), Paths.STATUS),
				ZooKeeperUtils.mapToBytes(status.toMap()));
	}


	/**
	 * Converter from {@link ChildData} to deployment name string.
	 */
	public static class DeploymentNameConverter implements Converter<ChildData, String> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String convert(ChildData source) {
			return Paths.stripPath(source.getPath());
		}
	}


	/**
	 * Holds information about a module that needs to be re-deployed.
	 * The {@link Comparable} implementation for this class sorts
	 * first by deployment unit name followed by (reverse) module descriptor
	 * index - i.e., the biggest index appearing first.
	 *
	 * @author Eric Bottard
	 * @author Ilayaperumal Gopinathan
	 */
	protected static class ModuleDeployment implements Comparable<ModuleDeployment> {

		protected final DeploymentUnit deploymentUnit;

		protected final ModuleDescriptor moduleDescriptor;

		protected final ModuleDeploymentProperties deploymentProperties;

		protected final String moduleSequence;

		ModuleDeployment(DeploymentUnit deploymentUnit, ModuleDescriptor moduleDescriptor,
				ModuleDeploymentProperties deploymentProperties, String moduleSequence) {
			this.deploymentUnit = deploymentUnit;
			this.moduleDescriptor = moduleDescriptor;
			this.deploymentProperties = deploymentProperties;
			this.moduleSequence = moduleSequence;
		}

		@Override
		public int compareTo(ModuleDeployment o) {
			int c = this.deploymentUnit.getName().compareTo(o.deploymentUnit.getName());
			if (c == 0) {
				c = o.moduleDescriptor.getIndex() - this.moduleDescriptor.getIndex();
			}
			if (c == 0) {
				c = Integer.valueOf(o.moduleSequence) - Integer.valueOf(this.moduleSequence);
			}
			return c;
		}

	}


}
