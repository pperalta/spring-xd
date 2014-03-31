/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.xd.dirt.stream;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.integration.test.SingleNodeIntegrationTestSupport;
import org.springframework.xd.dirt.module.ModuleDefinitionRepository;
import org.springframework.xd.dirt.module.ModuleDeployer;
import org.springframework.xd.dirt.server.SingleNodeApplication;
import org.springframework.xd.dirt.server.TestApplicationBootstrap;
import org.springframework.xd.module.core.CompositeModule;
import org.springframework.xd.module.core.Module;


/**
 * @author David Turanski
 */
public class StreamTestSupport {

	private static StreamDeployer streamDeployer;

	private static ModuleDeployer moduleDeployer;

	private static ModuleDefinitionRepository moduleDefinitionRepository;

	private static SingleNodeApplication application;

	private static ConfigurableApplicationContext adminContext;

	@BeforeClass
	public static void startXDSingleNode() throws Exception {
		application = new TestApplicationBootstrap().getSingleNodeApplication().run("--analytics", "memory",
				"--store", "memory");
		// Explicitly set this to true since RandomConfigurationSupport disables JMX by default.
		System.setProperty("XD_JMX_ENABLED", "true");
		adminContext = application.adminContext();
		SingleNodeIntegrationTestSupport integrationTestSupport = new SingleNodeIntegrationTestSupport(application,
				"classpath:/testmodules/");

		streamDeployer = integrationTestSupport.streamDeployer();

		Object md = application.containerContext().getBean("moduleDeployer", Object.class);
		moduleDeployer = (ModuleDeployer) (AopUtils.isJdkDynamicProxy(md) ? ((Advised) md).getTargetSource().getTarget()
				: md);
		moduleDefinitionRepository = adminContext.getBean(ModuleDefinitionRepository.class);
	}

	protected static void deployStream(String name, String config) {
		streamDeployer.save(new StreamDefinition(name, config));
		streamDeployer.deploy(name);
		while (moduleDeployer.getDeployedModules().get(name) == null) {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	protected static void undeployStream(String name) {
		streamDeployer.undeploy(name);
	}

	protected static void deleteStream(String name) {
		streamDeployer.delete(name);
	}

	protected static Module getDeployedModule(String streamName, int index) {
		Map<Integer, Module> streamModules = getStreamModules(streamName);
		return streamModules.get(index);
	}

	protected static Module getDeployedSource(String streamName) {
		Map<Integer, Module> streamModules = getStreamModules(streamName);
		return streamModules.get(0);
	}

	protected static Module getDeployedSink(String streamName) {
		Map<Integer, Module> streamModules = getStreamModules(streamName);
		return streamModules.get(streamModules.size() - 1);
	}

	protected static Map<Integer, Module> getStreamModules(String streamName) {
		Map<String, Map<Integer, Module>> deployedModules = moduleDeployer.getDeployedModules();
		Assert.notNull(deployedModules.get(streamName), "Stream '" + streamName + "' apparently is not deployed");
		return deployedModules.get(streamName);
	}

	protected static MessageChannel getSourceOutputChannel(String streamName) {
		Module source = getDeployedSource(streamName);
		if (source instanceof CompositeModule) {
			source = (Module) TestUtils.getPropertyValue(source, "modules", List.class).get(0);
		}
		return source.getComponent("output", MessageChannel.class);
	}

	protected static SubscribableChannel getSinkInputChannel(String streamName) {
		Module sink = getDeployedSink(streamName);
		// Should be a publish-subscribe-channel
		if (sink instanceof CompositeModule) {
			@SuppressWarnings("unchecked")
			List<Module> modules = TestUtils.getPropertyValue(sink, "modules", List.class);
			sink = modules.get(modules.size() - 1);
		}
		return sink.getComponent("input", SubscribableChannel.class);
	}

	protected static ConfigurableApplicationContext getAdminContext() {
		return adminContext;
	}

	protected static ModuleDefinitionRepository getModuleDefinitionRepository() {
		return moduleDefinitionRepository;
	}

	protected void sendMessageAndVerifyOutput(String streamName, Message<?> message, MessageTest test) {
		Assert.notNull(streamName, "streamName cannot be null");
		Assert.notNull(test, "test cannot be null");
		Assert.notNull(message, "message cannot be null");

		MessageChannel producer = getSourceOutputChannel(streamName);
		SubscribableChannel consumer = getSinkInputChannel(streamName);
		consumer.subscribe(test);
		producer.send(message);
		assertTrue(test.getMessageHandled());
	}

	protected void sendPayloadAndVerifyOutput(String streamName, Object payload, MessageTest test) {
		Assert.notNull(payload, "payload cannot be null");
		sendMessageAndVerifyOutput(streamName, new GenericMessage<Object>(payload), test);
	}

	protected void sendPayloadAndVerifyTappedOutput(String streamName, Object payload, String moduleToTap,
			MessageTest test) {
		Assert.notNull(payload, "payload cannot be null");
		sendMessageAndVerifyTappedOutput(streamName, new GenericMessage<Object>(payload), moduleToTap, test);
	}

	protected void sendMessageAndVerifyTappedOutput(String streamName, Message<?> message, String moduleToTap,
			MessageTest test) {
		Assert.notNull(streamName, "streamName cannot be null");
		Assert.notNull(test, "test cannot be null");
		Assert.notNull(message, "message cannot be null");

		String tapName = streamName + "Tap";
		String tapChannel = "tap:stream:" + streamName;
		if (moduleToTap != null) {
			tapChannel = tapChannel + "." + moduleToTap;
		}

		deployStream(
				tapName,
				tapChannel + " > sink");

		MessageChannel producer = getSourceOutputChannel(streamName);
		SubscribableChannel consumer = getSinkInputChannel(tapName);
		SubscribableChannel streamConsumer = getSinkInputChannel(streamName);

		// Add a dummy consumer to the stream in case there is none
		streamConsumer.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
			}
		});

		consumer.subscribe(test);
		producer.send(message);
		assertTrue(test.getMessageHandled());

		undeployStream(tapName);
	}

	protected static abstract class MessageTest implements MessageHandler {

		private boolean messageHandled;

		public boolean getMessageHandled() {
			return this.messageHandled;
		}

		@Override
		public final void handleMessage(Message<?> message) throws MessagingException {
			this.test(message);
			messageHandled = true;
		}

		protected abstract void test(Message<?> message);

		protected void waitForCompletion(int maxtime) {
			int time = 0;
			while (time < maxtime && !getMessageHandled()) {
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
				time += 100;
			}
		}

	}

	@AfterClass
	public static void cleanUp() {
		if (application != null) {
			application.close();
		}
	}
}
