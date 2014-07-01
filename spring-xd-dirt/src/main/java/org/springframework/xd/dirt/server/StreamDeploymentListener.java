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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

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
import org.springframework.xd.dirt.core.RequestedModulesPath;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.core.StreamDeploymentsPath;
import org.springframework.xd.dirt.server.ModuleDeploymentWriter.ResultCollector;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.util.DeploymentPropertiesUtility;
import org.springframework.xd.dirt.zookeeper.ChildPathIterator;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.ModuleType;

/**
 * Listener implementation that handles stream deployment requests.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class StreamDeploymentListener implements PathChildrenCacheListener {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(StreamDeploymentListener.class);

	/**
	 * Container matcher for matching modules to containers.
	 */
	private final ContainerMatcher containerMatcher;

	/**
	 * Repository from which to obtain containers in the cluster.
	 */
	private final ContainerRepository containerRepository;

	/**
	 * Cache of children under the module deployment requests path.
	 */
	protected final PathChildrenCache moduleDeploymentRequests;

	/**
	 * Utility for writing module deployment requests to ZooKeeper.
	 */
	private final ModuleDeploymentWriter moduleDeploymentWriter;

	/**
	 * Utility for loading streams (including deployment metadata).
	 */
	private final DeploymentLoader deploymentLoader = new DeploymentLoader();

	/**
	 * Stream factory.
	 */
	private final StreamFactory streamFactory;

	/**
	 * State calculator for stream state.
	 */
	private final DeploymentUnitStateCalculator stateCalculator;

	/**
	 * {@link org.springframework.core.convert.converter.Converter} from
	 * {@link org.apache.curator.framework.recipes.cache.ChildData} in
	 * stream deployments to Stream name.
	 */
	private final ModuleRedeployer.DeploymentNameConverter deploymentNameConverter = new ModuleRedeployer.DeploymentNameConverter();

	/**
	 * Executor service dedicated to handling events raised from
	 * {@link org.apache.curator.framework.recipes.cache.PathChildrenCache}.
	 *
	 * @see #childEvent
	 * @see StreamDeploymentListener.EventHandler
	 */
	private final ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {

		@Override
		public Thread newThread(Runnable runnable) {
			Thread thread = new Thread(runnable, "Stream Deployer");
			thread.setDaemon(true);
			return thread;
		}
	});

	/**
	 * Construct a StreamDeploymentListener.
	 *
	 * @param zkConnection ZooKeeper connection
	 * @param containerRepository repository to obtain container data
	 * @param streamFactory factory to construct {@link Stream}
	 * @param containerMatcher matches modules to containers
	 * @param stateCalculator calculator for stream state
	 */
	public StreamDeploymentListener(ZooKeeperConnection zkConnection,
			PathChildrenCache moduleDeploymentRequests,
			ContainerRepository containerRepository,
			StreamFactory streamFactory,
			ContainerMatcher containerMatcher, DeploymentUnitStateCalculator stateCalculator) {
		this.moduleDeploymentRequests = moduleDeploymentRequests;
		this.containerMatcher = containerMatcher;
		this.containerRepository = containerRepository;
		this.moduleDeploymentWriter = new ModuleDeploymentWriter(zkConnection,
				containerRepository, containerMatcher);
		this.streamFactory = streamFactory;
		this.stateCalculator = stateCalculator;
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Handle child events for the {@link Paths#STREAMS} path.
	 */
	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		ZooKeeperUtils.logCacheEvent(logger, event);
		executorService.submit(new EventHandler(client, event));
	}

	/**
	 * Handle the creation of a new stream deployment.
	 *
	 * @param client curator client
	 * @param data stream deployment request data
	 */
	private void onChildAdded(CuratorFramework client, ChildData data) throws Exception {
		String streamName = Paths.stripPath(data.getPath());
		Stream stream = deploymentLoader.loadStream(client, streamName, streamFactory);
		if (stream != null) {
			logger.info("Deploying stream {}", stream);
			deployStream(client, stream);
			logger.info("Stream {} deployment attempt complete", stream);
		}
	}

	private void onChildRemoved(CuratorFramework client, ChildData data) throws Exception {
		String streamName = Paths.stripPath(data.getPath());
		RequestedModulesPath path;
		for (ChildData requestedModulesData : moduleDeploymentRequests.getCurrentData()) {
			path = new RequestedModulesPath(requestedModulesData.getPath());
			if (path.getStreamName().equals(streamName)) {
				client.delete().deletingChildrenIfNeeded().forPath(path.build());
			}
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
		String statusPath = Paths.build(Paths.STREAM_DEPLOYMENTS, stream.getName(), Paths.STATUS);

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
			StreamModuleDeploymentPropertiesProvider provider =
					new StreamModuleDeploymentPropertiesProvider(stream);
			List<RuntimeDeploymentPropertiesProvider> runtimePropertiesProviders = new ArrayList<RuntimeDeploymentPropertiesProvider>();
			runtimePropertiesProviders.add(new StreamPartitionPropertiesProvider(stream));
			Collection<ModuleDeploymentStatus> deploymentStatuses = new ArrayList<ModuleDeploymentStatus>();
			for (Iterator<ModuleDescriptor> descriptors = stream.getDeploymentOrderIterator(); descriptors.hasNext();) {
				ModuleDescriptor descriptor = descriptors.next();
				ModuleDeploymentProperties deploymentProperties = provider.propertiesForDescriptor(descriptor);
				Deque<Container> matchedContainers = new ArrayDeque<Container>(containerMatcher.match(descriptor,
						deploymentProperties,
						containerRepository.findAll()));
				ResultCollector collector = moduleDeploymentWriter.new ResultCollector();
				// Modules count == 0
				if (deploymentProperties.getCount() == 0) {
					for (RuntimeDeploymentPropertiesProvider runtimePropertiesProvider : runtimePropertiesProviders) {
						deploymentProperties.putAll(runtimePropertiesProvider.runtimeProperties(descriptor));
					}
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
						for (RuntimeDeploymentPropertiesProvider runtimePropertiesProvider : runtimePropertiesProviders) {
							deploymentProperties.putAll(runtimePropertiesProvider.runtimeProperties(descriptor));
						}
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
			}

			DeploymentUnitStatus status = stateCalculator.calculate(stream, provider, deploymentStatuses);
			logger.info("Deployment status for stream '{}': {}", stream.getName(), status);

			client.setData().forPath(statusPath, ZooKeeperUtils.mapToBytes(status.toMap()));
		}
		catch (NoContainerException e) {
			logger.warn("No containers available for deployment of stream {}", stream.getName());
		}
		catch (InterruptedException e) {
			throw e;
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
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
	 * Iterate all deployed streams, recalculate the state of each, and create
	 * an ephemeral node indicating the stream state. This is typically invoked
	 * upon leader election.
	 *
	 * @param client             curator client
	 * @param streamDeployments  curator cache of stream deployments
	 * @throws Exception
	 */
	public void recalculateStreamStates(CuratorFramework client, PathChildrenCache streamDeployments) throws Exception {
		for (Iterator<String> iterator =
				new ChildPathIterator<String>(deploymentNameConverter, streamDeployments); iterator.hasNext();) {
			String streamName = iterator.next();
			String definitionPath = Paths.build(Paths.build(Paths.STREAM_DEPLOYMENTS, streamName));
			Stream stream = deploymentLoader.loadStream(client, streamName, streamFactory);
			if (stream != null) {
				String streamModulesPath = Paths.build(definitionPath, Paths.MODULES);
				List<ModuleDeploymentStatus> statusList = new ArrayList<ModuleDeploymentStatus>();
				List<String> moduleDeployments = client.getChildren().forPath(streamModulesPath);
				for (String moduleDeployment : moduleDeployments) {
					StreamDeploymentsPath streamDeploymentsPath = new StreamDeploymentsPath(
							Paths.build(streamModulesPath, moduleDeployment));
					statusList.add(new ModuleDeploymentStatus(
							streamDeploymentsPath.getContainer(),
							streamDeploymentsPath.getModuleSequence(),
							new ModuleDescriptor.Key(streamName,
									ModuleType.valueOf(streamDeploymentsPath.getModuleType()),
									streamDeploymentsPath.getModuleLabel()),
							ModuleDeploymentStatus.State.deployed, null
							));
				}
				DeploymentUnitStatus status = stateCalculator.calculate(stream,
						new StreamModuleDeploymentPropertiesProvider(stream), statusList);

				logger.info("Deployment status for stream '{}': {}", stream.getName(), status);

				String statusPath = Paths.build(Paths.STREAM_DEPLOYMENTS, stream.getName(), Paths.STATUS);
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
	 * Module deployment properties provider for stream modules. This provider
	 * generates properties required for stream partitioning support.
	 */
	public static class StreamModuleDeploymentPropertiesProvider
			implements ModuleDeploymentPropertiesProvider {

		/**
		 * Cache of module deployment properties.
		 */
		private final Map<ModuleDescriptor.Key, ModuleDeploymentProperties> mapDeploymentProperties =
				new HashMap<ModuleDescriptor.Key, ModuleDeploymentProperties>();

		/**
		 * Stream to create module deployment properties for.
		 */
		private final Stream stream;

		/**
		 * Construct a {@code StreamModuleDeploymentPropertiesProvider} for
		 * a {@link org.springframework.xd.dirt.core.Stream}.
		 *
		 * @param stream stream to create module properties for
		 */
		public StreamModuleDeploymentPropertiesProvider(Stream stream) {
			this.stream = stream;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public ModuleDeploymentProperties propertiesForDescriptor(ModuleDescriptor moduleDescriptor) {
			ModuleDescriptor.Key key = moduleDescriptor.createKey();
			ModuleDeploymentProperties properties = mapDeploymentProperties.get(key);
			if (properties == null) {
				properties = DeploymentPropertiesUtility.createModuleDeploymentProperties(
						stream.getDeploymentProperties(), moduleDescriptor);
				mapDeploymentProperties.put(key, properties);
			}
			return properties;
		}
	}

	/**
	 * Callable that handles events from a {@link org.apache.curator.framework.recipes.cache.PathChildrenCache}. This
	 * allows for the handling of events to be executed in a separate thread from the Curator thread that raises these
	 * events.
	 */
	class EventHandler implements Callable<Void> {

		/**
		 * Curator client.
		 */
		private final CuratorFramework client;

		/**
		 * Event raised from Curator.
		 */
		private final PathChildrenCacheEvent event;

		/**
		 * Construct an {@code EventHandler}.
		 *
		 * @param client curator client
		 * @param event event raised from Curator
		 */
		EventHandler(CuratorFramework client, PathChildrenCacheEvent event) {
			this.client = client;
			this.event = event;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Void call() throws Exception {
			try {
				switch (event.getType()) {
					case CHILD_ADDED:
						onChildAdded(client, event.getData());
						break;
					case CHILD_REMOVED:
						onChildRemoved(client, event.getData());
						break;
					default:
						break;
				}
				return null;
			}
			catch (Exception e) {
				logger.error("Exception caught while handling event", e);
				throw e;
			}
		}
	}

}
