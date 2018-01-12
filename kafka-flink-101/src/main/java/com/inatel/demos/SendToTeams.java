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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class SendToTeams {

    public static String RabbitMQTopic = "default_topic";
    public static String CarNumberFilter = "3";
    
    public static void main(String[] args) throws Exception {


        try {
            RabbitMQTopic = args[0];
            CarNumberFilter = args[1];
        }
        catch (ArrayIndexOutOfBoundsException e){
            System.out.println("Please, enter a queue name and a car number as argument.");
        }
        System.out.println("Using queue name: " + args[0]);
        System.out.println("Using Car Number filter: " + args[1]);
        
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
        .setHost("localhost")
        .setPort(5672)
        .setVirtualHost("/")
        .setUserName("guest")
        .setPassword("guest")
        .build();

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("flink-demo", 
            new SimpleStringSchema(), 
            properties)
    );
 
    
    RMQSink<String> TeamSink = new RMQSink<String>(
        connectionConfig,               // config for the RabbitMQ connection
        RabbitMQTopic,                  // name of the RabbitMQ queue to send messages to
        new SimpleStringSchema());      // serialization schema to turn Java objects to messages
    
    stream
        .flatMap(new TelemetryJsonParser())
        .addSink(TeamSink);
    
    env.execute();

    }

    // FlatMap Function - Json Parser
    // Receive JSON data from Kafka broker and parse car number, speed and counter
    
    // {"Car": 9, "time": "52.196000", "telemetry": {"Vaz": "1.270000", "Distance": "4.605865", "LapTime": "0.128001", 
    // "RPM": "591.266113", "Ay": "24.344515", "Gear": "3.000000", "Throttle": "0.000000", 
    // "Steer": "0.207988", "Ax": "-17.551264", "Brake": "0.282736", "Fuel": "1.898847", "Speed": "34.137680"}}

    static class TelemetryJsonParser implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String telemetry, Collector<String> out) throws Exception {
            ObjectNode jsonTelemetry = new ObjectMapper().readValue(telemetry, ObjectNode.class);
            int carNumber = jsonTelemetry.get("Car").asInt();
			if(carNumber == Integer.parseInt(CarNumberFilter))
            {
                out.collect(telemetry);
            }
        }
    }

}
