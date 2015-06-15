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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.xd.dirt.core.DeploymentUnit;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.server.admin.deployment.zk.DeploymentLoader;
import org.springframework.xd.dirt.stream.ParsingContext;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.RuntimeModuleDeploymentProperties;

/**
 * @author Patrick Peralta
 */
public class StreamDeploymentStrategy implements DeploymentStrategy {
	@Override
	public String getDeploymentPath(String name) {
		return Paths.build(Paths.STREAM_DEPLOYMENTS, name);
	}

	@Override
	public String getDeploymentsPath() {
		return Paths.STREAM_DEPLOYMENTS;
	}

	@Override
	public ParsingContext getParsingContext() {
		return ParsingContext.stream;
	}

	@Override
	public ModuleDeploymentPropertiesProvider<RuntimeModuleDeploymentProperties> runtimePropertiesProvider(
			DeploymentUnit deploymentUnit, ModuleDeploymentPropertiesProvider<ModuleDeploymentProperties> provider) {
		return new StreamRuntimePropertiesProvider((Stream) deploymentUnit, provider);
	}
}
