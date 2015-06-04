/*
 * Copyright 2013-2014 the original author or authors.
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

import static org.springframework.xd.dirt.stream.ParsingContext.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.ResourceDeployer;
import org.springframework.xd.dirt.core.StreamDeploymentsPath;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentHandler;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentStrategy;
import org.springframework.xd.dirt.server.admin.deployment.StreamDeploymentStrategy;
import org.springframework.xd.dirt.server.admin.deployment.zk.SupervisorElectedEvent;
import org.springframework.xd.dirt.server.admin.deployment.zk.SupervisorElectionListener;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDefinition;
import org.springframework.xd.module.ModuleDescriptor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Default implementation of {@link StreamDeployer} that uses provided
 * {@link StreamDefinitionRepository} and {@link StreamRepository} to
 * persist stream deployment and undeployment requests.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Andy Clement
 * @author Eric Bottard
 * @author Gunnar Hillert
 * @author Patrick Peralta
 * @author Ilayaperumal Gopinathan
 */
//public class StreamDeployer extends AbstractInstancePersistingDeployer<StreamDefinition, Stream> {
public class StreamDeployer implements ResourceDeployer, SupervisorElectionListener {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final ResourceDeployer deployer;

	@Autowired
	private ZooKeeperConnection zkConnection;

	public StreamDeployer(ResourceDeployer deployer) {
		this.deployer = deployer;
	}

	@Override
	public void deploy(String name, Map<String, String> properties) {
		deployer.deploy(name, properties);
	}

	@Override
	public void undeploy(String id) {
		logger.info("Undeploying stream {}", id);

		String streamDeploymentPath = Paths.build(Paths.STREAM_DEPLOYMENTS, id);
		String streamModuleDeploymentPath = Paths.build(streamDeploymentPath, Paths.MODULES);
		CuratorFramework client = zkConnection.getClient();

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

		// Stream module un-deployment happens in the following steps:
		//
		// 1. Load the stream module deployment paths for the given stream.
		//    These are the ephemeral nodes created by the container that
		//    deployed the stream.
		//
		// 2. Load/parse the stream definition. This is used to determine
		//    the order of the stream modules. Un-deployment should
		//    occur in left to right (sources, processors, sinks)
		//
		// 3. Using the parsed stream definition, sort the list of
		//    module deployment paths in the order in which they should
		//    be undeployed.
		//
		// 4. Remove the module deployment and stream paths.

		List<StreamDeploymentsPath> paths = new ArrayList<StreamDeploymentsPath>();
		try {
			List<String> deployments = client.getChildren().forPath(streamModuleDeploymentPath);
			for (String deployment : deployments) {
				paths.add(new StreamDeploymentsPath(Paths.build(streamModuleDeploymentPath, deployment)));
			}
		}
		catch (Exception e) {
			//NoNodeException - nothing to delete
			ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NoNodeException.class);
		}

		try {
			// todo
			// At a bit of a dead end here - it turns out that this class needs to know
			// a lot more detail about the underlying ResourceDeployer than I would have
			// liked. Going to attempt a different approach by forcing ResourceDeployer
			// to work directly with Streams and Jobs instead of just using IDs.
			// Also going to see if this stream undeployment routine can be made
			// generic enough to work with jobs.

			final Stream stream = loadStream(id);
			Comparator<StreamDeploymentsPath> pathComparator = new Comparator<StreamDeploymentsPath>() {
				@Override
				public int compare(StreamDeploymentsPath path0, StreamDeploymentsPath path1) {
					int i0 = 0;
					int i1 = 0;
					for (ModuleDescriptor moduleDescriptor : stream.getModuleDescriptors()) {
						if (path0.getModuleLabel().equals(moduleDescriptor.getModuleLabel())) {
							i0 = moduleDescriptor.getIndex();
						}
						else if (path1.getModuleLabel().equals(moduleDescriptor.getModuleLabel())) {
							i1 = moduleDescriptor.getIndex();
						}
						if (i0 > 0 && i1 > 0) {
							break;
						}
					}
					return Integer.compare(i0, i1);
				}
			};
			Collections.sort(paths, pathComparator);
		}
		catch (Exception e) {
			// todo: if we can't load the stream, we can't sort its modules for un-deployment
			logger.warn("Error loading stream " + id, e);
		}

		for (StreamDeploymentsPath path : paths) {
			try {
				logger.trace("removing path {}", path);
				client.delete().deletingChildrenIfNeeded().forPath(path.build());
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

		deployer.undeploy(id);
	}

	@Override
	public void undeployAll() {
		deployer.undeployAll();
	}

	@Override
	public DeploymentUnitStatus getDeploymentStatus(String name) {
		return deployer.getDeploymentStatus(name);
	}

	@Override
	public void onSupervisorElected(SupervisorElectedEvent event) throws Exception {
		if (this.deployer instanceof SupervisorElectionListener) {
			((SupervisorElectionListener) this.deployer).onSupervisorElected(event);
		}
	}

//
//	private static final Logger logger = LoggerFactory.getLogger(StreamDeployer.class);
//
//	private final static TypeReference<List<ModuleDefinition>> MODULE_DEFINITIONS_LIST = new TypeReference<List<ModuleDefinition>>() {};
//
//	private final ObjectWriter objectWriter = new ObjectMapper().writerWithType(MODULE_DEFINITIONS_LIST);
//
//	private static final String DEFINITION_KEY = "definition";
//
//	private static final String MODULE_DEFINITIONS_KEY = "moduleDefinitions";
//
//	private final ZooKeeperConnection zkConnection;
//
//	/**
//	 * Stream definition parser.
//	 */
//	private final XDParser parser;
//
//	/**
//	 * Construct a StreamDeployer.
//	 *
//	 * @param zkConnection       ZooKeeper connection
//	 * @param repository         repository for stream definitions
//	 * @param streamRepository   repository for stream instances
//	 * @param parser             stream definition parser
//	 */
//	public StreamDeployer(ZooKeeperConnection zkConnection, StreamDefinitionRepository repository,
//			StreamRepository streamRepository, XDParser parser, DeploymentHandler deploymentHandler) {
//		super(zkConnection, repository, streamRepository, parser, deploymentHandler, stream);
//		this.zkConnection = zkConnection;
//		this.parser = parser;
//
//	}
//
//	/**
//	 * {@inheritDoc}
//	 */
//	@Override
//	protected Stream makeInstance(StreamDefinition definition) {
//		return new Stream(definition);
//	}
//
//	/**
//	 * {@inheritDoc}
//	 */
//	@Override
//	protected StreamDefinition createDefinition(String name, String definition) {
//		return new StreamDefinition(name, definition);
//	}
//
//	/**
//	 * {@inheritDoc}
//	 */
//	@Override
//	protected String getDeploymentPath(StreamDefinition definition) {
//		return Paths.build(Paths.STREAM_DEPLOYMENTS, definition.getName());
//	}
//
//	/**
//	 * The migration code to run against existing stream definitions that don't have module definitions set already.
//	 * The following method will update the module definitions for those stream definitions.
//	 * See https://jira.spring.io/browse/XD-2854
//	 */
//	@PostConstruct
//	private void updateModuleDefinitions() {
//		if (this.zkConnection.getClient() != null) {
//			try {
//				CuratorFramework client = this.zkConnection.getClient();
//				if (client.checkExists().forPath(Paths.STREAMS) != null) {
//					for (StreamDefinition definition : findAll()) {
//						setModuleDefinitions(client, definition);
//					}
//				}
//			}
//			catch (Exception e) {
//				logger.error("Exception migrating stream definitions. This migration is done when the existing " +
//						"stream definitions that don't have module definitions set." , e);
//			}
//		}
//	}
//
//	/**
//	 * Set the module definitions for the given stream definition.
//	 * Module definitions are obtained by parsing the stream definition.
//	 *
//	 * @param client the curator framework client
//	 * @param definition the stream definition
//	 */
//	private void setModuleDefinitions(CuratorFramework client, StreamDefinition definition) {
//		String streamName = definition.getName();
//		String path = Paths.build(Paths.STREAMS, streamName);
//		try {
//			byte[] bytes = client.getData().forPath(path);
//			if (bytes != null) {
//				Map<String, String> map = ZooKeeperUtils.bytesToMap(bytes);
//
//				// Prior to Spring XD 1.1.1/1.2, the JSON map for stream definitions
//				// contained a single key named "definition" with the value being
//				// the definition string. As of 1.1.1/1.2, the map contains an additional
//				// key "moduleDefinitions" which contains a JSON array of module
//				// information such as class name and module URL.
//				//
//				// The following will convert "old" stream definitions into the
//				// new format.
//				if (map.get(MODULE_DEFINITIONS_KEY) == null) {
//					List<ModuleDescriptor> moduleDescriptors = this.parser.parse(streamName,
//							definition.getDefinition(), definitionKind);
//					List<ModuleDefinition> moduleDefinitions = createModuleDefinitions(moduleDescriptors);
//					if (!moduleDefinitions.isEmpty()) {
//						map.put(DEFINITION_KEY, definition.getDefinition());
//						try {
//							map.put(MODULE_DEFINITIONS_KEY,
//									objectWriter.writeValueAsString(moduleDefinitions));
//							byte[] binary = ZooKeeperUtils.mapToBytes(map);
//							BackgroundPathAndBytesable<?> op = client.checkExists().forPath(path) == null
//									? client.create() : client.setData();
//							op.forPath(path, binary);
//						}
//						catch (JsonProcessingException jpe) {
//							logger.error("Exception writing module definitions " + moduleDefinitions +
//									" for the stream " + streamName, jpe);
//						}
//					}
//				}
//			}
//		}
//		catch (Exception e) {
//			logger.error("Exception when updating module definitions for the stream "
//					+ streamName, e);
//		}
//	}

}
