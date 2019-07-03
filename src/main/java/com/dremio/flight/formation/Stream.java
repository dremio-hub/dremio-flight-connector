/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.flight.formation;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class Stream {
  private static final Logger logger = LoggerFactory.getLogger(Stream.class);
  private final Consumer consumer;
  private final Producer producer;
  private final BlockingQueue<ArrowRecordBatch> exchanger = new ArrayBlockingQueue<>(1024 * 1024);
  private FlightDescriptor descriptor;

  public Stream(BufferAllocator allocator, FlightDescriptor descriptor) {
    this.descriptor = descriptor;
    this.consumer = new Consumer(allocator, descriptor, exchanger);
    this.producer = new Producer(allocator, descriptor, exchanger);
  }


  public void setSchema(Schema schema) {
    logger.debug("got a schema, setting on consumer for {}", descriptor);
    consumer.setSchema(schema);
    producer.start(schema);
  }

  public boolean cancel() {
    producer.cancel();
    return consumer.cancel();
  }

  public Producer getProducer() {
    return producer;
  }

  public Consumer getConsumer() {
    return consumer;
  }

  public static class Producer implements AutoCloseable {

    private final BufferAllocator allocator;
    private final FlightDescriptor descriptor;
    private final BlockingQueue<ArrowRecordBatch> exchanger;

    public Producer(BufferAllocator allocator, FlightDescriptor descriptor, BlockingQueue<ArrowRecordBatch> exchanger) {
      this.allocator = allocator.newChildAllocator("producer", 0, Long.MAX_VALUE);
      this.descriptor = descriptor;
      this.exchanger = exchanger;
    }

    public void add(ArrowRecordBatch batch) throws InterruptedException {
      logger.debug("try to put a batch onto exchanger for {}", descriptor);
      exchanger.put(batch.cloneWithTransfer(this.allocator));
      logger.debug("put into exchanger, new size is {} for {}", exchanger.size(), descriptor);
    }

    public void start(Schema schema) {

      logger.debug("started listener and loader. all good for {}", descriptor);
    }

    private int size() {
      return exchanger.stream().map(ArrowRecordBatch::getLength).mapToInt(Integer::intValue).sum();
    }
    
    @Override
    public void close() {
      logger.debug("close producer {}", descriptor);
      int counter = 0;
      while (size() > 0 && counter < 10) {
        logger.warn("Trying to close producer with allocated memory still in exchanger. Will sleep for a few");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        counter++;
      }
      if (size() > 0) {
        logger.info("exchanger is drained, can close");
      } else {
        logger.warn("there are still {} items left in the exchanger, prepare for an error", exchanger.size());
      }
      try {
        exchanger.put(new ArrowRecordBatch(-1, ImmutableList.of(), ImmutableList.of()));
      } catch (InterruptedException e) {
        logger.error("could not send stop message for {}", descriptor);
      }
      allocator.close();
    }

    public void abort() {
      logger.debug("abort producer {}", descriptor);
    }

    public void cancel() {
      close();
    }
  }

  public static class Consumer implements AutoCloseable {

    private AtomicBoolean isDone = new AtomicBoolean(false);
    private BufferAllocator allocator;
    private final FlightDescriptor descriptor;
    private final BlockingQueue<ArrowRecordBatch> exchanger;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private ServerStreamListener listener;
    private VectorLoader loader;
    private VectorSchemaRoot root;

    public Consumer(BufferAllocator allocator, FlightDescriptor descriptor, BlockingQueue<ArrowRecordBatch> exchanger) {
      this.allocator = allocator.newChildAllocator("consumer", 0, Long.MAX_VALUE);
      this.descriptor = descriptor;
      this.exchanger = exchanger;
    }

    public void start(ServerStreamListener listener) throws InterruptedException {
      logger.debug("trying to start, waiting for schema for {}", descriptor);
      countDownLatch.await();
      if (root == null) {
        logger.warn("root was not set for {}, not starting listener properly", descriptor);
        root = VectorSchemaRoot.create(new Schema(ImmutableList.of()), allocator);
      }
      listener.start(root);
      this.listener = listener;
    }

    @Override
    public void close() {
      if (!exchanger.isEmpty()) {
        logger.warn("closing {} but it isn't empty! There are {} elements left in queue", descriptor, exchanger.size());
      }
      if (listener != null) {
//        try {
//          exchanger.put(new ArrowRecordBatch(-1, ImmutableList.of(), ImmutableList.of()));
//        } catch (InterruptedException e) {
//          logger.error("could not send close message for {}", descriptor);
//        }
        listener.completed();
      }
      if (root != null) {
        root.close();
      }
      isDone.set(true);
      allocator.close();
    }

    public boolean drain() throws InterruptedException {
      if (isDone.get()) {
        logger.debug("already done, returning. {}", descriptor);
        return true;
      }
      logger.debug("it is not done. try to take from exchanger for {}", descriptor);
      ArrowRecordBatch batch = exchanger.poll(1, TimeUnit.SECONDS);
      if (batch == null) {
        logger.debug("timed out waiting, {}", descriptor);
        return false;
      }
      logger.debug("got batch, send to loader. exchanger size is {} for {}", exchanger.size(), descriptor);
      if (batch.getLength() == -1) {
        logger.debug("{} is done, stopping it", descriptor);
        return true;
      }
      loader.load(batch);
      logger.debug("tell listener to put next for {}", descriptor);
      listener.putNext();
      batch.close();
      logger.debug("got a batch and sent to listener for {}", descriptor);
      return false;
    }

    public void setSchema(Schema schema) {
      root = VectorSchemaRoot.create(schema, allocator);
      loader = new VectorLoader(root);
      countDownLatch.countDown();
    }

    public boolean cancel() {
      if (countDownLatch.getCount() != 0) {
        logger.debug("did not get data on this stream...cancelling {}", descriptor);
        countDownLatch.countDown();
        close();
        return true;
      }
      return false;
    }
  }

}



