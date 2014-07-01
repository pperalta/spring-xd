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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.ContainerMatcher;
import org.springframework.xd.dirt.cluster.NoContainerException;
import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.Job;
import org.springframework.xd.dirt.core.JobDeploymentsPath;
import org.springframework.xd.dirt.core.RequestedModulesPath;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.server.ModuleDeploymentWriter.ResultCollector;
import org.springframework.xd.dirt.util.DeploymentPropertiesUtility;
import org.springframework.xd.dirt.zookeeper.ChildPathIterator;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.ModuleType;


/**
 * Listener implementation that handles job deployment requests.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class JobDeploymentListener implements PathChildrenCacheListener {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(JobDeploymentListener.class);

	/**
	 * Container matcher for matching modules to containers.
	 */
	private final ContainerMatcher containerMatcher;

	/**
	 * Repository from which to obtain containers in the cluster.
	 */
	private final ContainerRepository containerRepository;

	/**
	 * Utility for writing module deployment requests to ZooKeeper.
	 */
	private final ModuleDeploymentWriter moduleDeploymentWriter;

	/**
	 * Cache of children under the module deployment requests path.
	 */
	protected final PathChildrenCache moduleDeploymentRequests;

	/**
	 * Utility for loading jobs (including deployment metadata).
	 */
	private final DeploymentLoader deploymentLoader = new DeploymentLoader();

	/**
	 * Job factory.
	 */
	private final JobFactory jobFactory;

	/**
	 * State calculator for job state.
	 */
	private final DeploymentUnitStateCalculator stateCalculator;

	/**
	 * {@link org.springframework.core.convert.converter.Converter} from
	 * {@link org.apache.curator.framework.recipes.cache.ChildData} in
	 * job deployments to job name.
	 */
	private final ModuleRedeployer.DeploymentNameConverter deploymentNameConverter = new ModuleRedeployer.DeploymentNameConverter();

	/**
	 * Construct a JobDeploymentListener.
	 *
	 * @param zkConnection ZooKeeper connection
	 * @param containerRepository repository to obtain container data
	 * @param jobFactory factory to construct {@link Job}
	 * @param containerMatcher matches modules to containers
	 * @param stateCalculator calculator for job state
	 */
	public JobDeploymentListener(ZooKeeperConnection zkConnection, PathChildrenCache moduleDeploymentRequests,
			ContainerRepository containerRepository, JobFactory jobFactory,
			ContainerMatcher containerMatcher, DeploymentUnitStateCalculator stateCalculator) {
		this.moduleDeploymentRequests = moduleDeploymentRequests;
		this.containerMatcher = containerMatcher;
		this.containerRepository = containerRepository;
		this.moduleDeploymentWriter = new ModuleDeploymentWriter(zkConnection,
				containerRepository, containerMatcher);
		this.jobFactory = jobFactory;
		this.stateCalculator = stateCalculator;
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Handle child events for the {@link Paths#JOBS} path.
	 */
	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		ZooKeeperUtils.logCacheEvent(logger, event);
		switch (event.getType()) {
			case CHILD_ADDED:
				onChildAdded(client, event.getData());
				break;
			case CHILD_REMOVED:
				onChildRemoved(client, event.getData());
			default:
				break;
		}
	}

	/**
	 * Handle the creation of a new job deployment.
	 *
	 * @param client curator client
	 * @param data job deployment request data
	 */
	private void onChildAdded(CuratorFramework client, ChildData data) throws Exception {
		String jobName = Paths.stripPath(data.getPath());
		Job job = deploymentLoader.loadJob(client, jobName, jobFactory);
		deployJob(client, job);
	}

	private void onChildRemoved(CuratorFramework client, ChildData data) throws Exception {
		String jobName = Paths.stripPath(data.getPath());
		RequestedModulesPath path;
		for (ChildData requestedModulesData : moduleDeploymentRequests.getCurrentData()) {
			path = new RequestedModulesPath(requestedModulesData.getPath());
			if (path.getStreamName().equals(jobName)) {
				client.delete().deletingChildrenIfNeeded().forPath(path.build());
			}
		}
	}

	/**
	 * Issue deployment requests for a job. This deployment will occur if:
	 * <ul>
	 *     <li>the job has not been destroyed</li>
	 *     <li>the job has not been undeployed</li>
	 *     <li>there is a container that can deploy the job</li>
	 * </ul>
	 *
	 * @param job the job instance to redeploy
	 * @throws InterruptedException
	 */
	private void deployJob(CuratorFramework client, final Job job) throws InterruptedException {
		if (job != null) {
			String statusPath = Paths.build(Paths.JOB_DEPLOYMENTS, job.getName(), Paths.STATUS);

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
					String.format("Expected 'deploying' status for job '%s'; current status: %s",
							job.getName(), deployingStatus));

			ModuleDeploymentPropertiesProvider provider = new JobModuleDeploymentPropertiesProvider(job);
			List<ModuleDescriptor> descriptors = new ArrayList<ModuleDescriptor>();
			descriptors.add(job.getJobModuleDescriptor());

			try {
				Collection<ModuleDeploymentStatus> deploymentStatuses = new ArrayList<ModuleDeploymentStatus>();
				for (ModuleDescriptor descriptor : job.getModuleDescriptors()) {
					ModuleDeploymentProperties deploymentProperties = provider.propertiesForDescriptor(descriptor);
					Deque<Container> matchedContainers = new ArrayDeque<Container>(containerMatcher.match(descriptor,
							deploymentProperties,
							containerRepository.findAll()));
					ResultCollector collector = moduleDeploymentWriter.new ResultCollector();
					// Modules count == 0
					if (deploymentProperties.getCount() == 0) {
						String moduleSequence = String.valueOf(0);
						createRequestedModulesPath(client, descriptor, deploymentProperties, moduleSequence);
						for (Container container : matchedContainers) {
							moduleDeploymentWriter.writeModuleDeployment(client, collector, deploymentProperties,
									descriptor, container, moduleSequence);
						}
					}
					// Modules count > 0
					else {
						for (int i = 1; i <= deploymentProperties.getCount(); i++) {
							String moduleSequence = String.valueOf(i);
							createRequestedModulesPath(client, descriptor, deploymentProperties, moduleSequence);
							if (matchedContainers.size() > 0) {
								moduleDeploymentWriter.writeModuleDeployment(client, collector, deploymentProperties,
										descriptor, matchedContainers.pop(), moduleSequence);
							}
						}
					}
					deploymentStatuses.addAll(moduleDeploymentWriter.processResults(client,
							collector));

					DeploymentUnitStatus status = stateCalculator.calculate(job, provider, deploymentStatuses);

					logger.info("Deployment status for job '{}': {}", job.getName(), status);

					client.setData().forPath(statusPath, ZooKeeperUtils.mapToBytes(status.toMap()));
				}
			}
			catch (NoContainerException e) {
				logger.warn("No containers available for deployment of job {}", job.getName());
			}
			catch (InterruptedException e) {
				throw e;
			}
			catch (Exception e) {
				throw ZooKeeperUtils.wrapThrowable(e);
			}
		}
	}

	private void createRequestedModulesPath(CuratorFramework client, ModuleDescriptor descriptor,
			ModuleDeploymentProperties deploymentProperties, String moduleSequence) {
		// Create and set the data for the requested modules path
		String requestedModulesPath = new RequestedModulesPath().setStreamName(
				descriptor.getGroup()).setModuleType(descriptor.getType().toString()).setModuleLabel(
				descriptor.getModuleLabel()).setModuleSequence(moduleSequence).build();
		try {
			client.create().creatingParentsIfNeeded().forPath(requestedModulesPath,
					ZooKeeperUtils.mapToBytes(deploymentProperties));
		}
		catch (Exception e) {
			ZooKeeperUtils.wrapThrowable(e);
		}
	}

	/**
	 * Iterate all deployed jobs, recalculate the deployment status of each, and
	 * create an ephemeral node indicating the job state. This is typically invoked
	 * upon leader election.
	 *
	 * @param client          curator client
	 * @param jobDeployments  curator cache of job deployments
	 * @throws Exception
	 */
	public void recalculateJobStates(CuratorFramework client, PathChildrenCache jobDeployments) throws Exception {
		for (Iterator<String> iterator = new ChildPathIterator<String>(deploymentNameConverter, jobDeployments); iterator.hasNext();) {
			String jobName = iterator.next();
			Job job = deploymentLoader.loadJob(client, jobName, jobFactory);
			if (job != null) {
				String jobModulesPath = Paths.build(Paths.JOB_DEPLOYMENTS, jobName);
				List<ModuleDeploymentStatus> statusList = new ArrayList<ModuleDeploymentStatus>();
				List<String> moduleDeployments = client.getChildren().forPath(jobModulesPath);
				for (String moduleDeployment : moduleDeployments) {
					JobDeploymentsPath jobDeploymentsPath = new JobDeploymentsPath(
							Paths.build(jobModulesPath, moduleDeployment));
					statusList.add(new ModuleDeploymentStatus(
							jobDeploymentsPath.getContainer(),
							jobDeploymentsPath.getModuleSequence(),
							new ModuleDescriptor.Key(jobName, ModuleType.job, jobDeploymentsPath.getModuleLabel()),
							ModuleDeploymentStatus.State.deployed, null));
				}
				DeploymentUnitStatus status = stateCalculator.calculate(job,
						new JobModuleDeploymentPropertiesProvider(job), statusList);

				logger.info("Deployment status for job '{}': {}", job.getName(), status);

				String statusPath = Paths.build(Paths.JOB_DEPLOYMENTS, job.getName(), Paths.STATUS);
				Stat stat = client.checkExists().forPath(statusPath);
				if (stat != null) {
					logger.warn("Found unexpected path {}; stat: {}", statusPath, stat);
					client.delete().forPath(statusPath);
				}
				client.create().withMode(CreateMode.EPHEMERAL).forPath(statusPath,
						ZooKeeperUtils.mapToBytes(status.toMap()));
			}
		}
	}


	/**
	 * Module deployment properties provider for job modules.
	 */
	public static class JobModuleDeploymentPropertiesProvider implements ModuleDeploymentPropertiesProvider {

		private final Job job;

		public JobModuleDeploymentPropertiesProvider(Job job) {
			this.job = job;
		}

		@Override
		public ModuleDeploymentProperties propertiesForDescriptor(ModuleDescriptor descriptor) {
			return DeploymentPropertiesUtility.createModuleDeploymentProperties(
					job.getDeploymentProperties(), descriptor);
		}

	}

}
