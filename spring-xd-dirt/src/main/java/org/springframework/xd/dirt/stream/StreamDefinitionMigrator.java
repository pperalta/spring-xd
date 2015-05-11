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

package org.springframework.xd.dirt.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.xd.dirt.server.admin.deployment.zk.SupervisorElectedEvent;
import org.springframework.xd.dirt.server.admin.deployment.zk.SupervisorElectionListener;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDefinition;
import org.springframework.xd.module.ModuleDescriptor;

/**
 * Upon election, scan all stream definitions and modify stream definitions
 * that are in the "old" format. Prior to Spring XD 1.1.1/1.2, the JSON
 * map for stream definitions contained a single key named "definition"
 * with the value being the definition string. As of 1.1.1/1.2, the map
 * contains an additional key "moduleDefinitions" which contains a JSON
 * array of module information such as class name and module URL.
 * <p>
 * The migration only executes once; if this admin is elected leader
 * multiple times the migration will not execute.
 *
 * @author Ilayaperumal Gopinathan
 * @author Patrick Peralta
 */
public class StreamDefinitionMigrator implements SupervisorElectionListener {
	private static final Logger logger = LoggerFactory.getLogger(StreamDefinitionMigrator.class);

	private final static TypeReference<List<ModuleDefinition>> MODULE_DEFINITIONS_LIST =
			new TypeReference<List<ModuleDefinition>>() {};

	private final ObjectWriter objectWriter = new ObjectMapper().writerWithType(MODULE_DEFINITIONS_LIST);

	private static final String DEFINITION_KEY = "definition";

	private static final String MODULE_DEFINITIONS_KEY = "moduleDefinitions";

	private final ZooKeeperConnection zkConnection;

	/**
	 * Flag that determines if the migration has already executed.
	 */
	private volatile boolean executed = false;

	/**
	 * Stream definition parser.
	 */
	private final XDParser parser;

	private final StreamDefinitionRepository repository;

	public StreamDefinitionMigrator(ZooKeeperConnection zkConnection, XDParser parser,
			StreamDefinitionRepository repository) {
		this.zkConnection = zkConnection;
		this.parser = parser;
		this.repository = repository;
	}

	@Override
	public void onSupervisorElected(SupervisorElectedEvent supervisorElectedEvent) throws Exception {
		if (executed) {
			return;
		}

		if (this.zkConnection.getClient() != null) {
			try {
				CuratorFramework client = this.zkConnection.getClient();
				if (client.checkExists().forPath(Paths.STREAMS) != null) {
					for (StreamDefinition definition : repository.findAll()) {
						setModuleDefinitions(client, definition);
					}
				}
			}
			catch (Exception e) {
				logger.error("Exception migrating stream definitions", e);
			}
			finally {
				executed = true;
			}
		}
	}

	/**
	 * Set the module definitions for the given stream definition.
	 * Module definitions are obtained by parsing the stream definition.
	 *
	 * @param client the curator framework client
	 * @param definition the stream definition
	 */
	private void setModuleDefinitions(CuratorFramework client, StreamDefinition definition) {
		String streamName = definition.getName();
		String path = Paths.build(Paths.STREAMS, streamName);
		try {
			byte[] bytes = client.getData().forPath(path);
			if (bytes != null) {
				Map<String, String> map = ZooKeeperUtils.bytesToMap(bytes);
				if (map.get(MODULE_DEFINITIONS_KEY) == null) {
					List<ModuleDescriptor> moduleDescriptors = this.parser.parse(streamName,
							definition.getDefinition(), ParsingContext.stream);
					List<ModuleDefinition> moduleDefinitions = createModuleDefinitions(moduleDescriptors);
					if (!moduleDefinitions.isEmpty()) {
						map.put(DEFINITION_KEY, definition.getDefinition());
						try {
							map.put(MODULE_DEFINITIONS_KEY,
									objectWriter.writeValueAsString(moduleDefinitions));
							byte[] binary = ZooKeeperUtils.mapToBytes(map);
							BackgroundPathAndBytesable<?> op = client.checkExists().forPath(path) == null
									? client.create() : client.setData();
							op.forPath(path, binary);
						}
						catch (JsonProcessingException jpe) {
							logger.error("Exception writing module definitions " + moduleDefinitions +
									" for the stream " + streamName, jpe);
						}
					}
				}
			}
		}
		catch (Exception e) {
			logger.error("Exception when updating module definitions for the stream "
					+ streamName, e);
		}
	}

	/**
	 * Create a list of ModuleDefinitions given the results of parsing the definition.
	 *
	 * @param moduleDescriptors The list of ModuleDescriptors resulting from parsing
	 * the definition.
	 *
	 * @return a list of ModuleDefinitions
	 */
	protected List<ModuleDefinition> createModuleDefinitions(List<ModuleDescriptor> moduleDescriptors) {
		List<ModuleDefinition> moduleDefinitions = new ArrayList<ModuleDefinition>(moduleDescriptors.size());
		for (ModuleDescriptor moduleDescriptor : moduleDescriptors) {
			moduleDefinitions.add(moduleDescriptor.getModuleDefinition());
		}
		return moduleDefinitions;
	}

}
