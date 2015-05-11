/*
 * Copyright 2015 the original author or authors.
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

import java.util.Map;

/**
 * This interface defines validation methods that verify a stream or job
 * can be saved, deployed, undeployed, or deleted. Successful invocation
 * of these methods indicates that the operation may proceed, whereas
 * an exception indicates that the operation will not succeed.
 * <p>
 * This mechanism is useful when issuing operations asynchronously, as
 * is the case when deploying via
 * {@link org.springframework.xd.dirt.server.admin.deployment.DeploymentMessage} and
 * {@link org.springframework.xd.dirt.server.admin.deployment.DeploymentMessagePublisher}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Patrick Peralta
 */
public interface DeploymentValidator {

	/**
	 * Assert that the definition can be saved. This may include checks for:
	 * <ul>
	 *     <li>Existing instances of a definition with this name in the repository</li>
	 *     <li>Parsing and validation of the definition string</li>
	 * </ul>
	 *
	 * @param name the deployment unit name
	 * @param definition the definition
	 *
	 * @throws DefinitionAlreadyExistsException
	 */
	void validateBeforeSave(String name, String definition) throws DefinitionAlreadyExistsException;

	/**
	 * Validate the definition (stream or job) before deployment. This may include
	 * validating...
	 * <ul>
	 *     <li>that the definition already exists in the definition repository</li>
	 *     <li>that the deployment properties are valid</li>
	 * </ul>
	 *
	 * @param name the deployment unit name
	 * @param properties deployment properties
	 *
	 * @return a definition object <D> obtained from the definition repository
	 *
	 * @throws AlreadyDeployedException
	 * @throws DefinitionAlreadyExistsException
	 */
	void validateBeforeDeploy(String name, Map<String, String> properties) throws AlreadyDeployedException,
			DefinitionAlreadyExistsException;

	/**
	 * Assert that the deployment unit has been deployed.
	 *
	 * @param name the deployment unit name
	 * @throws NoSuchDefinitionException
	 * @throws NotDeployedException
	 */
	void validateDeployed(String name) throws NoSuchDefinitionException, NotDeployedException;

	/**
	 * Assert that the deployment unit has been deployed prior to undeploying.
	 *
	 * @param name the deployment unit name
	 * @throws NoSuchDefinitionException
	 * @throws NotDeployedException
	 */
	void validateBeforeUndeploy(String name) throws NoSuchDefinitionException, NotDeployedException;

	/**
	 * Assert that the deployment unit definition exists before deleting.
	 *
	 * @param name the deployment unit name
	 * @throws NoSuchDefinitionException
	 */
	void validateBeforeDelete(String name) throws NoSuchDefinitionException;
}

