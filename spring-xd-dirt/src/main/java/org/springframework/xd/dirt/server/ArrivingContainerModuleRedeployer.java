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
import java.util.Iterator;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.ContainerMatcher;
import org.springframework.xd.dirt.cluster.NoContainerException;
import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.core.Job;
import org.springframework.xd.dirt.core.RequestedModulesPath;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.core.StreamDeploymentsPath;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.util.DeploymentPropertiesUtility;
import org.springframework.xd.dirt.zookeeper.ChildPathIterator;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;


/**
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class ArrivingContainerModuleRedeployer extends ModuleRedeployer {

	/**
	 * Logger.
	 */
	protected final Logger logger = LoggerFactory.getLogger(ArrivingContainerModuleRedeployer.class);

	public ArrivingContainerModuleRedeployer(ZooKeeperConnection zkConnection,
			ContainerRepository containerRepository,
			StreamFactory streamFactory, JobFactory jobFactory,
			PathChildrenCache streamDeployments, PathChildrenCache jobDeployments,
			PathChildrenCache moduleDeploymentRequests, ContainerMatcher containerMatcher,
			DeploymentUnitStateCalculator stateCalculator) {
		super(zkConnection, containerRepository, streamFactory, jobFactory, streamDeployments, jobDeployments,
				moduleDeploymentRequests, containerMatcher, stateCalculator);
	}

	/**
	 * Handle the arrival of a container. This implementation will scan the
	 * existing streams/jobs and determine if any modules should be deployed to
	 * the new container.
	 *
	 * @param client curator client
	 * @param container the arriving container
	 */
	@Override
	protected void redeployModules(CuratorFramework client, Container container) throws Exception {
		logger.info("Container arrived: {}", container.getName());
		this.redeployJobs(client, container);
		this.redeployStreams(client, container);
	}

	/**
	 * Deploy any "orphaned" jobs (jobs that are supposed to be deployed but
	 * do not have any modules deployed in any container).
	 *
	 * @param client curator client
	 * @param container target container for job module deployment
	 * @throws Exception
	 */
	private void redeployJobs(CuratorFramework client, Container container) throws Exception {
		// check for "orphaned" jobs that can be deployed to this new container
		for (Iterator<String> jobDeploymentIterator =
				new ChildPathIterator<String>(deploymentNameConverter, jobDeployments); jobDeploymentIterator.hasNext();) {
			String jobName = jobDeploymentIterator.next();

			// if job is null this means the job was destroyed or undeployed
			Job job = deploymentLoader.loadJob(client, jobName, this.jobFactory);
			if (job != null) {
				ModuleDescriptor descriptor = job.getJobModuleDescriptor();

				// See XD-1777: in order to support partitioning we now include
				// module deployment properties in the module deployment request
				// path. However in this case the original path is not available
				// so this redeployment will simply create a new instance of
				// properties. As a result, if this module had a partition index
				// assigned to it, it will no longer be associated with that
				// partition index. This will be fixed in the future.
				ModuleDeploymentProperties moduleDeploymentProperties =
						DeploymentPropertiesUtility.createModuleDeploymentProperties(
								job.getDeploymentProperties(), descriptor);
				if (isCandidateForDeployment(container, descriptor, moduleDeploymentProperties)) {
					int moduleCount = moduleDeploymentProperties.getCount();
					if (moduleCount <= 0 || getContainersForJobModule(client, descriptor).size() < moduleCount) {
						// either the module has a count of 0 (therefore it should be deployed everywhere)
						// or the number of containers that have deployed the module is less than the
						// amount specified by the module descriptor
						logger.info("Deploying module {} to {}",
								descriptor.getModuleDefinition().getName(), container);

						ModuleDeploymentStatus deploymentStatus = null;
						ModuleDeployment moduleDeployment = new ModuleDeployment(job,
								descriptor, moduleDeploymentProperties, "0");
						try {
							deploymentStatus = deployModule(client, moduleDeployment, container);
						}
						catch (NoContainerException e) {
							logger.warn("Could not deploy job {} to container {}; " +
									"this container may have just departed the cluster", job.getName(), container);
						}
						finally {
							updateDeploymentUnitState(client, moduleDeployment, deploymentStatus);
						}
					}
				}
			}
		}
	}

	/**
	 * Deploy any "orphaned" stream modules. These are stream modules that
	 * are supposed to be deployed but currently do not have enough containers
	 * available to meet the deployment criteria (i.e. if a module requires
	 * 3 containers but only 2 are available). Furthermore this will deploy
	 * modules with a count of 0 if this container matches the deployment
	 * criteria.
	 *
	 * @param client curator client
	 * @param container target container for stream module deployment
	 * @throws Exception
	 */
	private void redeployStreams(CuratorFramework client, Container container) throws Exception {
		List<RequestedModulesPath> requestedModulesPaths = new ArrayList<RequestedModulesPath>();
		for (ChildData requestedModulesData : moduleDeploymentRequests.getCurrentData()) {
			requestedModulesPaths.add(new RequestedModulesPath(requestedModulesData.getPath()));
		}
		// iterate the cache of stream deployments
		for (ChildData data : streamDeployments.getCurrentData()) {
			String streamName = deploymentNameConverter.convert(data);
			final Stream stream = deploymentLoader.loadStream(client, streamName, streamFactory);
			// if stream is null this means the stream was destroyed or undeployed
			if (stream != null) {
				//TODO: Check the status of the stream, proceed if the status is not deployed.
				//TODO: It would be beneficial to have stream status on the streamDeployments cache.
				//TODO: But that would require modifying the data that has deployment properties
				List<RequestedModulesPath> requestedModules = RequestedModulesPath.getModules(requestedModulesPaths,
						streamName);
				List<String> deployedModules = new ArrayList<String>();
				for (String deployedModule : client.getChildren().forPath(Paths.build(data.getPath(), Paths.MODULES))) {
					deployedModules.add(Paths.stripPath(new StreamDeploymentsPath(Paths.build(data.getPath(),
							Paths.MODULES,
							deployedModule)).getModule()));
				}
				Collection<ModuleDescriptor> deployedDescriptors = new ArrayList<ModuleDescriptor>();
				for (RequestedModulesPath path : requestedModules) {
					ModuleDescriptor moduleDescriptor = stream.getModuleDescriptor(path.getModuleLabel());
					if ((path.getModuleSequence().equals("0") || !deployedModules.contains(path.getModule()))
							&& !deployedDescriptors.contains(moduleDescriptor)) {
						deployedDescriptors.add(moduleDescriptor);
						ModuleDeploymentProperties moduleDeploymentProperties = new ModuleDeploymentProperties();
						moduleDeploymentProperties.putAll(mapBytesUtility.toMap(moduleDeploymentRequests.getCurrentData(
								path.build()).getData()));
						if (isCandidateForDeployment(container, moduleDescriptor, moduleDeploymentProperties)) {
							//either the module has a count of 0 (therefore it should be deployed everywhere)
							// or the number of containers that have deployed the module is less than the
							// amount specified by the module descriptor
							logger.info("Deploying module {} to {}",
									moduleDescriptor.getModuleDefinition().getName(), container);

							ModuleDeploymentStatus deploymentStatus = null;
							ModuleDeployment moduleDeployment = new ModuleDeployment(
									stream, moduleDescriptor, moduleDeploymentProperties, path.getModuleSequence());
							try {
								deploymentStatus = deployModule(client, moduleDeployment, container);
							}
							catch (NoContainerException e) {
								logger.warn("Could not deploy module {} for stream {} to container {}; " +
										"this container may have just departed the cluster",
										moduleDescriptor.getModuleDefinition().getName(),
										stream.getName(), container);
							}
							finally {
								updateDeploymentUnitState(client, moduleDeployment, deploymentStatus);
							}
						}
					}
				}
			}
		}
	}


}
