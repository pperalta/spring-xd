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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.pivotal.receptor.client.ReceptorClient;
import io.pivotal.receptor.commands.DesiredLRPCreateRequest;
import io.pivotal.receptor.support.EnvironmentVariable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.server.admin.deployment.zk.DeploymentLoader;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.module.ModuleDescriptor;

/**
 * @author Patrick Peralta
 */
public class ReceptorDeploymentHandler implements DeploymentHandler {
	public static final String DOCKER_PATH = "docker://192.168.59.103:5000/xd-module";

	public static final String BASE_ADDRESS = "192.168.11.11.xip.io";

	@Autowired
	protected ZooKeeperConnection zkConnection;

	@Autowired
	private StreamFactory streamFactory;

	private final ReceptorClient receptorClient = new ReceptorClient();

	@Override
	public void deploy(String deploymentUnitName) throws Exception {
		Stream stream = DeploymentLoader.loadStream(zkConnection.getClient(), deploymentUnitName, streamFactory);
		if (stream != null) {
			for (Iterator<ModuleDescriptor> descriptors = stream.getDeploymentOrderIterator(); descriptors.hasNext(); ) {
				deploy(descriptors.next());
			}
		}
	}

	public void deploy(ModuleDescriptor descriptor) {
		String guid = guid(descriptor);
		DesiredLRPCreateRequest request = new DesiredLRPCreateRequest();
		request.setProcessGuid(guid);
		request.setRootfs(DOCKER_PATH);
		request.runAction().setPath("java");
		request.runAction().addArg("-Djava.security.egd=file:/dev/./urandom");
		request.runAction().addArg("-jar");
		request.runAction().addArg("/xd-module.jar");
		List<EnvironmentVariable> environmentVariables = new ArrayList<EnvironmentVariable>();
		Collections.addAll(environmentVariables, request.getEnv());
		environmentVariables.add(new EnvironmentVariable("XD_MODULE", path(descriptor)));
		Map<String, String> parameters = descriptor.getParameters();
		if (parameters != null && parameters.size() > 0) {
			for (Map.Entry<String, String> option : parameters.entrySet()) {
				environmentVariables.add(new EnvironmentVariable("OPTION_" + option.getKey(), option.getValue()));
			}
		}
		request.setEnv(environmentVariables.toArray(new EnvironmentVariable[environmentVariables.size()]));
		request.setPorts(new int[] {8080, 9000});
		request.addRoute(8080, guid + "." + BASE_ADDRESS, guid + "-8080." + BASE_ADDRESS);
		request.addRoute(9000, guid + "-9000." + BASE_ADDRESS);
		receptorClient.createDesiredLRP(request);
	}

	private String guid(ModuleDescriptor descriptor) {
		return "xd-" + descriptor.getGroup() + "-" + descriptor.getModuleName() + "-" + descriptor.getIndex();
	}

	private String path(ModuleDescriptor descriptor) {
		return descriptor.getGroup() + "." + descriptor.getType() + "." + descriptor.getModuleName() + "." + descriptor.getIndex();
	}

	@Override
	public void undeploy(String deploymentUnitName) throws Exception {
		Stream stream = DeploymentLoader.loadStream(zkConnection.getClient(), deploymentUnitName, streamFactory);
		if (stream != null) {
			for (ModuleDescriptor descriptor : stream.getModuleDescriptors())
				undeploy(descriptor);
		}
	}

	public void undeploy(ModuleDescriptor descriptor) {
		receptorClient.deleteDesiredLRP(guid(descriptor));
	}

}
