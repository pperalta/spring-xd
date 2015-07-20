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

package org.springframework.xd.dirt.spi;

import org.springframework.xd.module.ModuleDescriptor;

/**
 * Interface specifying the operations for a runtime environment
 * capable of launching {@link ModuleDescriptor modules}.
 *
 * @author Patrick Peralta
 */
public interface ModuleDeployer {

	/**
	 * Deploy the given {@code ModuleDescriptor}. Implementations
	 * may perform this operation asynchronously; therefore
	 * a successful deployment may not be assumed upon return.
	 * To determine the status of a deployment, invoke
	 * {@link #getStatus(ModuleDescriptor)}.
	 *
	 * // todo: is this an idempotent operation? if so an exception should be defined
	 *
	 * @param descriptor descriptor for module to be deployed
	 */
	void deploy(ModuleDescriptor descriptor);

	/**
	 * Un-deploy the the given {@code ModuleDescriptor}. Implementations
	 * may perform this operation asynchronously; therefore
	 * a successful un-deployment may not be assumed upon return.
	 * To determine the status of a deployment, invoke
	 * {@link #getStatus(ModuleDescriptor)}.
	 *
	 * @param descriptor descriptor for module to be un-deployed
	 */
	void undeploy(ModuleDescriptor descriptor);

	/**
	 * Return the deployment status of the given {@code ModuleDescriptor}.
	 *
	 * @param descriptor descriptor for module to obtain status for
	 *
	 * @return module deployment status
	 */
	ModuleStatus getStatus(ModuleDescriptor descriptor);
}
