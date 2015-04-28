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

import org.springframework.xd.module.ModuleDefinition;
import org.springframework.xd.module.ModuleDescriptor;

/**
 * Factory for {@link StreamDefinition} that parses the stream definition
 * DSL string and creates instances populated with {@link ModuleDescriptor}s
 * representing the stream modules.
 *
 * @author Patrick Peralta
 */
public class StreamDefinitionFactory {

	/**
	 * DSL parser.
	 */
	private final XDParser parser;

	/**
	 * Construct a {@link StreamDefinitionFactory}.
	 *
	 * @param parser parser for DSL string
	 */
	public StreamDefinitionFactory(XDParser parser) {
		this.parser = parser;
	}

	/**
	 * Create a new instance of {@link StreamDefinition} with its
	 * {@link ModuleDescriptor} list populated based on the stream
	 * definition DSL string.
	 *
	 * @param name  stream name
	 * @param dsl   stream definition DSL
	 *
	 * @return StreamDefinition
	 */
	public StreamDefinition createStreamDefinition(String name, String dsl) {
		List<ModuleDescriptor> moduleDescriptors = parser.parse(name, dsl, ParsingContext.stream);
		return new StreamDefinition(name, dsl, createModuleDefinitions(moduleDescriptors));
	}

	/**
	 * For a list of {@link ModuleDescriptor}, return a list of {@link ModuleDefinition}.
	 *
	 * @param moduleDescriptors list of module descriptors
	 *
	 * @return list of module definitions
	 */
	protected List<ModuleDefinition> createModuleDefinitions(List<ModuleDescriptor> moduleDescriptors) {
		List<ModuleDefinition> moduleDefinitions = new ArrayList<ModuleDefinition>(moduleDescriptors.size());
		for (ModuleDescriptor moduleDescriptor : moduleDescriptors) {
			moduleDefinitions.add(moduleDescriptor.getModuleDefinition());
		}
		return moduleDefinitions;
	}

}
