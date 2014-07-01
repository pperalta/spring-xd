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

import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;

/**
 * Interface to obtain runtime {@link org.springframework.xd.module.ModuleDeploymentProperties}
 * for a {@link org.springframework.xd.module.ModuleDescriptor}.
 *
 * @author Ilayaperumal Gopinathan
 */
public interface RuntimeDeploymentPropertiesProvider {

	/**
	 * Return the runtime deployment properties for the module descriptor.
	 *
	 * @param descriptor module descriptor for module to be deployed
	 * @return deployment properties for module
	 */
	ModuleDeploymentProperties runtimeProperties(ModuleDescriptor descriptor);
}
