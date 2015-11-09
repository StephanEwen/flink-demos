/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mythingy.streaming;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class EventGenerator extends RichParallelSourceFunction<ProgressEvent> {

	private static final long DELAY_BETWEEN_EVENTS = 100;
	private static final long MIN_EVENT_TIME_SPAN = 2000;
	private static final long MAX_EVENT_TIME_SPAN = 6000;

	private volatile boolean running = true;


	@Override
	public void run(final SourceContext<ProgressEvent> sourceContext) throws Exception {
		
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		final Random rnd = new Random();
		
		try {
			while (running) {
				// schedule the next sequence of events
				
				final UUID uuid = UUID.randomUUID();
				final String imageName = generateImageName();
				final String[] types = generateEventTypeSequence();
				
				final long delay = MIN_EVENT_TIME_SPAN + rnd.nextLong() % (MAX_EVENT_TIME_SPAN - MIN_EVENT_TIME_SPAN);
				final long now = System.currentTimeMillis();

				// emit the first event
				sourceContext.collect(new ProgressEvent(types[0], imageName, now, uuid));
				
				// schedule the successive events
				for (int i = 1; i < types.length; i++) {
					long thisDelay = delay / (types.length - 1) * i;
					ProgressEvent evt = new ProgressEvent(types[i], imageName, now + thisDelay, uuid);
					executor.schedule(new EventEmitter(sourceContext, evt), thisDelay, TimeUnit.MILLISECONDS);
				}
				
				// throttle the throughput
				Thread.sleep(DELAY_BETWEEN_EVENTS);
			}
		}
		finally {
			executor.shutdownNow();
			executor.awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	// ------------------------------------------------------------------------
	
	private static String generateImageName() {
		return "trove-mysql-5.6";
	}
	
	private static String[] generateEventTypeSequence() {
		return new String[] {
				"compute.instance.create.start",
				"compute.instance.create.end",
				"trove.instance.create"
		};
	}
	
	// ------------------------------------------------------------------------
	
	private static final class EventEmitter implements Runnable {
		
		private final SourceContext<ProgressEvent> context;
		private final ProgressEvent event;

		private EventEmitter(SourceContext<ProgressEvent> context, ProgressEvent event) {
			this.context = context;
			this.event = event;
		}
		
		@Override
		public void run() {
			context.collect(event);
		}
	}
}
