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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.cluster.ContainerMatcher;
import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.module.ModuleDefinitionRepository;
import org.springframework.xd.dirt.stream.JobDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnectionListener;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.options.ModuleOptionsMetadataResolver;

/**
 * Process that watches ZooKeeper for Container arrivals and departures from
 * the XD cluster. Each {@code DeploymentSupervisor} instance will attempt
 * to request leadership, but at any given time only one {@code DeploymentSupervisor}
 * instance in the cluster will have leadership status.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 *
 * @see org.apache.curator.framework.recipes.leader.LeaderSelector
 */
public class DeploymentSupervisor implements ApplicationListener<ApplicationEvent>, DisposableBean {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(DeploymentSupervisor.class);

	/**
	 * ZooKeeper connection.
	 */
	private final ZooKeeperConnection zkConnection;

	/**
	 * Repository to load the containers.
	 */
	private final ContainerRepository containerRepository;

	/**
	 * Repository to load stream definitions.
	 */
	private final StreamDefinitionRepository streamDefinitionRepository;

	/**
	 * Repository to load job definitions.
	 */
	private final JobDefinitionRepository jobDefinitionRepository;

	/**
	 * Repository to load module definitions.
	 */
	private final ModuleDefinitionRepository moduleDefinitionRepository;

	/**
	 * Resolver for module options metadata.
	 */
	private final ModuleOptionsMetadataResolver moduleOptionsMetadataResolver;

	/**
	 * {@link ApplicationContext} for this admin server. This reference is updated
	 * via an application context event and read via {@link #getId()}.
	 */
	private volatile ApplicationContext applicationContext;

	/**
	 * Container matcher for matching modules to containers.
	 */
	private final ContainerMatcher containerMatcher;

	/**
	 * Leader selector to elect admin server that will handle stream deployment requests. Marked volatile because this
	 * reference is written and read by the Curator event dispatch threads - there is no guarantee that the same thread
	 * will do the reading and writing.
	 */
	private volatile LeaderSelector leaderSelector;

	/**
	 * Listener that is invoked when this admin server is elected leader.
	 */
	private final LeaderSelectorListener leaderListener = new LeaderListener();

	/**
	 * ZooKeeper connection listener that attempts to obtain leadership when
	 * the ZooKeeper connection is established.
	 */
	private final ConnectionListener connectionListener = new ConnectionListener();

	/**
	 * Executor service used to execute Curator path cache events.
	 *
	 * @see #instantiatePathChildrenCache
	 */
	private final ExecutorService executorService =
			Executors.newSingleThreadExecutor(ThreadUtils.newThreadFactory("DeploymentSupervisorCacheListener"));

	/**
	 * State calculator for stream/job state.
	 */
	private final DeploymentUnitStateCalculator stateCalculator;

	/**
	 * Construct a {@code DeploymentSupervisor}.
	 *
	 * @param zkConnection ZooKeeper connection
	 * @param containerRepository repository for the containers
	 * @param streamDefinitionRepository repository for streams definitions
	 * @param jobDefinitionRepository repository for job definitions
	 * @param moduleDefinitionRepository repository for modules
	 * @param moduleOptionsMetadataResolver resolver for module options metadata
	 * @param containerMatcher matches modules to containers
	 * @param stateCalculator calculator for stream/job state
	 */
	public DeploymentSupervisor(ZooKeeperConnection zkConnection,
			ContainerRepository containerRepository,
			StreamDefinitionRepository streamDefinitionRepository,
			JobDefinitionRepository jobDefinitionRepository,
			ModuleDefinitionRepository moduleDefinitionRepository,
			ModuleOptionsMetadataResolver moduleOptionsMetadataResolver,
			ContainerMatcher containerMatcher,
			DeploymentUnitStateCalculator stateCalculator) {
		Assert.notNull(zkConnection, "ZooKeeperConnection must not be null");
		Assert.notNull(containerRepository, "ContainerRepository must not be null");
		Assert.notNull(streamDefinitionRepository, "StreamDefinitionRepository must not be null");
		Assert.notNull(moduleDefinitionRepository, "ModuleDefinitionRepository must not be null");
		Assert.notNull(moduleOptionsMetadataResolver, "moduleOptionsMetadataResolver must not be null");
		Assert.notNull(containerMatcher, "containerMatcher must not be null");
		Assert.notNull(stateCalculator, "stateCalculator must not be null");
		this.zkConnection = zkConnection;
		this.containerRepository = containerRepository;
		this.streamDefinitionRepository = streamDefinitionRepository;
		this.jobDefinitionRepository = jobDefinitionRepository;
		this.moduleDefinitionRepository = moduleDefinitionRepository;
		this.moduleOptionsMetadataResolver = moduleOptionsMetadataResolver;
		this.containerMatcher = containerMatcher;
		this.stateCalculator = stateCalculator;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextRefreshedEvent) {
			this.applicationContext = ((ContextRefreshedEvent) event).getApplicationContext();
			if (this.zkConnection.isConnected()) {
				requestLeadership(this.zkConnection.getClient());
			}
			this.zkConnection.addListener(connectionListener);
		}
		else if (event instanceof ContextStoppedEvent) {
			if (this.leaderSelector != null) {
				this.leaderSelector.close();
			}
		}
	}

	/**
	 * Return the UUID for this admin server.
	 *
	 * @return id for this admin server
	 */
	private String getId() {
		return this.applicationContext.getId();
	}

	/**
	 * Register the {@link LeaderListener} if not already registered. This method is {@code synchronized} because it may
	 * be invoked either by the thread starting the {@link ApplicationContext} or the thread that raises the ZooKeeper
	 * connection event.
	 *
	 * @param client the {@link CuratorFramework} client
	 */
	private synchronized void requestLeadership(CuratorFramework client) {
		try {
			Paths.ensurePath(client, Paths.MODULE_DEPLOYMENTS);
			Paths.ensurePath(client, Paths.STREAM_DEPLOYMENTS);
			Paths.ensurePath(client, Paths.JOB_DEPLOYMENTS);
			Paths.ensurePath(client, Paths.CONTAINERS);
			Paths.ensurePath(client, Paths.STREAMS);
			Paths.ensurePath(client, Paths.JOBS);

			if (leaderSelector == null) {
				leaderSelector = new LeaderSelector(client, Paths.build(Paths.ADMINS), leaderListener);
				leaderSelector.setId(getId());
				leaderSelector.start();
			}
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {
		if (leaderSelector != null) {
			leaderSelector.close();
			leaderSelector = null;
		}
	}

	/**
	 * Instantiate a Curator {@link PathChildrenCache} for the provided path with
	 * the following parameters:
	 * <ul>
	 *     <li>node cache enabled</li>
	 *     <li>data compression disabled</li>
	 *     <li>executor service {@link #executorService} for invoking event handlers</li>
	 * </ul>
	 *
	 * @param client the {@link CuratorFramework} client
	 * @param path   the path for the cache
	 * @return Curator path children cache
	 */
	private PathChildrenCache instantiatePathChildrenCache(CuratorFramework client, String path) {
		return new PathChildrenCache(client, path, true, false, executorService);
	}


	/**
	 * {@link ZooKeeperConnectionListener} implementation that requests leadership
	 * upon connection to ZooKeeper.
	 */
	private class ConnectionListener implements ZooKeeperConnectionListener {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void onConnect(CuratorFramework client) {
			logger.info("Admin {} CONNECTED", getId());
			requestLeadership(client);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void onDisconnect(CuratorFramework client) {
			try {
				destroy();
			}
			catch (Exception e) {
				logger.warn("exception occurred while closing leader selector", e);
			}
		}
	}

	/**
	 * Listener implementation that is invoked when this server becomes the leader.
	 */
	class LeaderListener extends LeaderSelectorListenerAdapter {

		/**
		 * {@inheritDoc}
		 * <p/>
		 * Upon leadership election, this Admin server will create a {@link PathChildrenCache}
		 * for {@link Paths#STREAMS} and {@link Paths#JOBS}. These caches will have
		 * {@link PathChildrenCacheListener PathChildrenCacheListeners} attached to them
		 * that will react to stream and job creation and deletion. Upon leadership
		 * relinquishment, the listeners will be removed and the caches shut down.
		 */
		@Override
		public void takeLeadership(CuratorFramework client) throws Exception {
			logger.info("Leader Admin {} is watching for stream/job deployment requests.", getId());

			cleanupDeployments(client);

			PathChildrenCache containers = null;
			PathChildrenCache streamDeployments = null;
			PathChildrenCache jobDeployments = null;
			PathChildrenCache moduleDeploymentRequests = null;
			StreamDeploymentListener streamDeploymentListener;
			JobDeploymentListener jobDeploymentListener;
			PathChildrenCacheListener containerListener;

			try {
				StreamFactory streamFactory = new StreamFactory(streamDefinitionRepository, moduleDefinitionRepository,
						moduleOptionsMetadataResolver);

				JobFactory jobFactory = new JobFactory(jobDefinitionRepository, moduleDefinitionRepository,
						moduleOptionsMetadataResolver);

				String requestedModulesPath = Paths.build(Paths.MODULE_DEPLOYMENTS, Paths.REQUESTED);
				Paths.ensurePath(client, requestedModulesPath);
				moduleDeploymentRequests = instantiatePathChildrenCache(client, requestedModulesPath);
				moduleDeploymentRequests.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

				streamDeploymentListener = new StreamDeploymentListener(zkConnection,
						moduleDeploymentRequests,
						containerRepository,
						streamFactory,
						containerMatcher,
						stateCalculator);

				streamDeployments = instantiatePathChildrenCache(client, Paths.STREAM_DEPLOYMENTS);
				streamDeployments.getListenable().addListener(streamDeploymentListener);

				// using BUILD_INITIAL_CACHE so that all known streams are populated
				// in the cache before invoking recalculateStreamStates; same for
				// jobs below
				streamDeployments.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
				streamDeploymentListener.recalculateStreamStates(client, streamDeployments);

				jobDeploymentListener = new JobDeploymentListener(zkConnection,
						moduleDeploymentRequests,
						containerRepository,
						jobFactory,
						containerMatcher,
						stateCalculator);

				jobDeployments = instantiatePathChildrenCache(client, Paths.JOB_DEPLOYMENTS);
				jobDeployments.getListenable().addListener(jobDeploymentListener);
				jobDeployments.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
				jobDeploymentListener.recalculateJobStates(client, jobDeployments);

				containerListener = new ContainerListener(zkConnection,
						containerRepository,
						streamFactory,
						jobFactory,
						streamDeployments,
						jobDeployments,
						moduleDeploymentRequests,
						containerMatcher,
						stateCalculator);

				containers = instantiatePathChildrenCache(client, Paths.CONTAINERS);
				containers.getListenable().addListener(containerListener);
				containers.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

				Thread.sleep(Long.MAX_VALUE);
			}
			catch (InterruptedException e) {
				logger.info("Leadership canceled due to thread interrupt");
				Thread.currentThread().interrupt();
			}
			finally {
				if (containers != null) {
					containers.close();
				}

				if (streamDeployments != null) {
					streamDeployments.close();
				}

				if (jobDeployments != null) {
					jobDeployments.close();
				}
				if (moduleDeploymentRequests != null) {
					moduleDeploymentRequests.close();
				}
			}
		}

		/**
		 * Remove module deployments targeted to containers that are no longer running.
		 *
		 * @param client the {@link CuratorFramework} client
		 *
		 * @throws Exception
		 */
		private void cleanupDeployments(CuratorFramework client) throws Exception {
			Set<String> containerDeployments = new HashSet<String>();

			try {
				containerDeployments.addAll(client.getChildren().forPath(Paths.build(Paths.MODULE_DEPLOYMENTS)));
				containerDeployments.removeAll(client.getChildren().forPath(Paths.build(Paths.CONTAINERS)));
			}
			catch (KeeperException.NoNodeException e) {
				// ignore
			}

			for (String oldContainer : containerDeployments) {
				try {
					client.delete().deletingChildrenIfNeeded().forPath(
							Paths.build(Paths.MODULE_DEPLOYMENTS, oldContainer));
				}
				catch (KeeperException.NoNodeException e) {
					// ignore
				}
			}
		}
	}

}
