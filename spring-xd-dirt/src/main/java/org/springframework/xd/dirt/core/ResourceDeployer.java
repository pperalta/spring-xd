/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.xd.dirt.core;

import java.util.Map;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.xd.dirt.stream.StreamDefinition;

/**
 * Interface for XD Resource Services.
 *
 *
 * @author David Turanski
 * @author Gunnar Hillert
 * @author Ilayaperumal Gopinathan
 * @author Patrick Peralta
 */
public interface ResourceDeployer {

	/**
	 * Deploy a resource (job or stream).
	 *
	 * @param deploymentUnit
	 */
	void deploy(DeploymentUnit deploymentUnit);

	/**
	 * todo
	 * @param deploymentUnit
	 */
	void undeploy(DeploymentUnit deploymentUnit);

	/**
	 * Undeploy all the deployed resources.
	 */
	void undeployAll();

	/**
	 * For the given deployment unit id, return the deployment status.
	 *
	 * @param name id for deployment unit
	 * @return deployment status
	 */
	DeploymentUnitStatus getDeploymentStatus(String name);
}
