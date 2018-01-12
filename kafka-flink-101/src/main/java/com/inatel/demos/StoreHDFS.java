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

package com.inatel.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StoreHDFS {

  public static void main(String[] args) throws Exception {

    DateFormat dateFormat = new SimpleDateFormat("HH-mm-ss");
    Date date = new Date();
    
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env.addSource(new FlinkKafkaConsumer09<>("flink-demo", new SimpleStringSchema(), properties));

    String basePath = "/hdfsroot"; // Here is you output path

    BucketingSink<String> HadoopSink = new BucketingSink<>(basePath);
    
    HadoopSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HH"));
    HadoopSink.setPendingPrefix(dateFormat.format(date));
    HadoopSink.setInactiveBucketCheckInterval(3000);
    HadoopSink.setInactiveBucketThreshold(5000);

    stream.print();
    stream.addSink(HadoopSink);

    env.execute();
  }
}
