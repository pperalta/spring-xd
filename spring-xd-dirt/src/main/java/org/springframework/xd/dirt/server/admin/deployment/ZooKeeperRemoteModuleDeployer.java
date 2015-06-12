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

package org.springframework.xd.dirt.server.admin.deployment;

import java.util.Collection;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.NoContainerException;
import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.core.ModuleDeploymentRequestsPath;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.RuntimeModuleDeploymentProperties;

/**
 * @author Patrick Peralta
 */
public class ZooKeeperRemoteModuleDeployer implements RemoteModuleDeployer {
	private static final Logger logger = LoggerFactory.getLogger(ZooKeeperRemoteModuleDeployer.class);

	@Autowired
	private ZooKeeperConnection zkConnection;

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


	@Override
	public void deploy(ModuleDescriptor descriptor,
			ModuleDeploymentPropertiesProvider<RuntimeModuleDeploymentProperties> runtimePropertiesProvider) {

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


	@Override
	public void undeploy(ModuleDescriptor descriptor) {

	}

	@Override
	public ModuleDeploymentStatus getStatus(ModuleDescriptor descriptor) {
		return null;
	}
}
