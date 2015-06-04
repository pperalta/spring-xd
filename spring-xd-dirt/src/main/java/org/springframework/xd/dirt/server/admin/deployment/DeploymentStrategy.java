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

import org.springframework.xd.dirt.core.DeploymentUnit;
import org.springframework.xd.dirt.stream.ParsingContext;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.RuntimeModuleDeploymentProperties;

/**
 * @author Patrick Peralta
 */
public interface DeploymentStrategy {

	/**
	 * Return the ZooKeeper path used for deployment requests for the
	 * given definition.
	 *
	 * @param name name of definition
	 *
	 * @return ZooKeeper path for deployment requests
	 */
	String getDeploymentPath(String name);

	String getDeploymentsPath();

	ParsingContext getParsingContext();

	DeploymentUnit load(String name);

	ModuleDeploymentPropertiesProvider<RuntimeModuleDeploymentProperties> runtimePropertiesProvider(
			DeploymentUnit deploymentUnit,
			ModuleDeploymentPropertiesProvider<ModuleDeploymentProperties> provider);
}