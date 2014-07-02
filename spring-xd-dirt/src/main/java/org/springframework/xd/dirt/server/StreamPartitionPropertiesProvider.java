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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.integration.bus.BusProperties;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.RuntimeModuleDeploymentProperties;


/**
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class StreamPartitionPropertiesProvider implements RuntimeDeploymentPropertiesProvider {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(StreamPartitionPropertiesProvider.class);

	/**
	 * Map to keep track of how many instances of a module this provider
	 * has generated properties for. This is used to generate a unique
	 * id for each module deployment per container for stream partitioning.
	 */
	private final Map<ModuleDescriptor.Key, Integer> mapModuleCount = new HashMap<ModuleDescriptor.Key, Integer>();

	/**
	 * Stream to create module deployment properties for.
	 */
	private final Stream stream;

	private final ModuleDeploymentPropertiesProvider deploymentPropertiesProvider;

	/**
	 * Construct a {@code StreamModuleDeploymentPropertiesProvider} for
	 * a {@link org.springframework.xd.dirt.core.Stream}.
	 *
	 * @param stream stream to create module properties for
	 */
	public StreamPartitionPropertiesProvider(Stream stream, ModuleDeploymentPropertiesProvider propertiesProvider) {
		this.stream = stream;
		this.deploymentPropertiesProvider = propertiesProvider;
	}

	@Override
	public RuntimeModuleDeploymentProperties runtimeProperties(ModuleDescriptor moduleDescriptor) {
		List<ModuleDescriptor> streamModules = stream.getModuleDescriptors();
		RuntimeModuleDeploymentProperties properties = new RuntimeModuleDeploymentProperties();
		properties.putAll(deploymentPropertiesProvider.propertiesForDescriptor(moduleDescriptor));

		ModuleDescriptor.Key moduleKey = moduleDescriptor.createKey();
		Integer index = mapModuleCount.get(moduleKey);
		if (index == null) {
			index = 0;
		}
		mapModuleCount.put(moduleKey, index + 1);
		// sequence number only applies if count > 0
		properties.setSequence((properties.getCount() == 0) ? 0 : index);

		int moduleIndex = moduleDescriptor.getIndex();
		if (moduleIndex > 0) {
			ModuleDescriptor previous = streamModules.get(moduleIndex - 1);
			ModuleDeploymentProperties previousProperties = deploymentPropertiesProvider.propertiesForDescriptor(previous);
			if (hasPartitionKeyProperty(previousProperties)) {
				properties.put("consumer.partitionIndex", String.valueOf(index));
			}
		}
		if (hasPartitionKeyProperty(properties)) {
			try {
				ModuleDeploymentProperties nextProperties =
						deploymentPropertiesProvider.propertiesForDescriptor(streamModules.get(moduleIndex + 1));

				String count = nextProperties.get("count");
				validateCountProperty(count, moduleDescriptor);
				properties.put("producer.partitionCount", count);
			}
			catch (IndexOutOfBoundsException e) {
				logger.warn("Module '{}' is a sink module which contains a property " +
						"of '{}' used for data partitioning; this feature is only " +
						"supported for modules that produce data", moduleDescriptor,
						"producer.partitionKeyExpression");

			}
		}
		else if (streamModules.size() > moduleIndex + 1) {
			/*
			 *  A direct binding is allowed if all of the following are true:
			 *  1. the user did not explicitly disallow direct binding
			 *  2. this module is not a partitioning producer
			 *  3. this module is not the last one in a stream
			 *  4. both this module and the next module have a count of 0
			 *  5. both this module and the next module have the same criteria (both can be null)
			 */
			String directBindingKey = "producer." + BusProperties.DIRECT_BINDING_ALLOWED;
			String directBindingValue = properties.get(directBindingKey);
			if (directBindingValue != null && !"false".equalsIgnoreCase(properties.get(directBindingKey))) {
				logger.warn(
						"Only 'false' is allowed as an explicit value for the {} property,  but the value was: '{}'",
						directBindingKey, directBindingValue);
			}
			if (!"false".equalsIgnoreCase(properties.get(directBindingKey))) {
				ModuleDeploymentProperties nextProperties = deploymentPropertiesProvider.propertiesForDescriptor(streamModules.get(moduleIndex + 1));
				if (properties.getCount() == 0 && nextProperties.getCount() == 0) {
					String criteria = properties.getCriteria();
					if ((criteria == null && nextProperties.getCriteria() == null)
							|| (criteria != null && criteria.equals(nextProperties.getCriteria()))) {
						properties.put(directBindingKey, Boolean.toString(true));
					}
				}
			}
		}
		return properties;
	}

	/**
	 * Return {@code true} if the provided properties include a property
	 * used to extract a partition key.
	 *
	 * @param properties properties to examine for a partition key property
	 * @return true if the properties contain a partition key property
	 */
	private boolean hasPartitionKeyProperty(ModuleDeploymentProperties properties) {
		return (properties.containsKey("producer.partitionKeyExpression") || properties.containsKey("producer.partitionKeyExtractorClass"));
	}

	/**
	 * Validate the value of {@code count} for the purposes of partitioning.
	 * The value of the string must consist of an integer > 1.
	 *
	 * @param count       value to validate
	 * @param descriptor  module descriptor this {@code count} property
	 *                    is associated with
	 *
	 * @throws IllegalArgumentException if the value of the string
	 *         does not consist of an integer > 1
	 */
	private void validateCountProperty(String count, ModuleDescriptor descriptor) {
		Assert.hasText(count, String.format("'count' property is required " +
				"in properties for module '%s' in order to support partitioning", descriptor));

		try {
			Assert.isTrue(Integer.parseInt(count) > 1,
					String.format("'count' property for module '%s' must contain an " +
							"integer > 1, current value is '%s'", descriptor, count));
		}
		catch (NumberFormatException e) {
			throw new IllegalArgumentException(String.format("'count' property for " +
					"module %s does not contain a valid integer, current value is '%s'",
					descriptor, count));
		}
	}

}
