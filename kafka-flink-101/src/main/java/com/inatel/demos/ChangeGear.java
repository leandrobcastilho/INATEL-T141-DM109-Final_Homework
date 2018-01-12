package com.inatel.demos;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class ChangeGear {
	
    public static String CarNumberFilter = "3";
    public static Integer delay = 3;
    public static boolean activeDebug = false;
    
	public static void main(String[] args) throws Exception {
		
		System.out.println("===========================================================");
		
        try {
        	CarNumberFilter = args[0];
        	System.out.println("############ Using Car Number filter: " + CarNumberFilter);
        	if( args[1] != null ){
        		delay = Integer.valueOf(args[1]);
        	}
        	System.out.println("############ delay: " + delay);
        	if( args[2] != null ){
        		activeDebug = Boolean.valueOf(args[2]);
        	}
        	System.out.println("############ activeDebug: " + activeDebug);
        }
        catch (ArrayIndexOutOfBoundsException e){
            System.out.println("Please, enter a car number as argument.");
        }
        
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "flink_consumer");
		
		DataStream dataStream = env.addSource(
				new FlinkKafkaConsumer09<>("flink-demo", 
						new JSONDeserializationSchema(), 
						properties)
				);
		
		
		dataStream.flatMap(new TelemetryGearJsonParser())
		.keyBy(0)
		.timeWindow(Time.seconds(delay))
		.reduce(new CountReducer())
		//.flatMap(new CountMapper())
		.map(new CountPrinter())
		.print();
		
		env.execute();
	}
	
	// FlatMap Function - Json Parser
	// Receive JSON data from Kafka broker and parse car number, speed and counter
	
	// {"Car": 9, "time": "52.196000", "telemetry": {"Vaz": "1.270000", "Distance": "4.605865", "LapTime": "0.128001", 
	// "RPM": "591.266113", "Ay": "24.344515", "Gear": "3.000000", "Throttle": "0.000000", 
	// "Steer": "0.207988", "Ax": "-17.551264", "Brake": "0.282736", "Fuel": "1.898847", "Speed": "34.137680"}}
	
	static class TelemetryGearJsonParser implements FlatMapFunction<ObjectNode, Tuple4<String, Float, Integer, Integer>> {
		@Override
		public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple4<String, Float, Integer, Integer>> out) throws Exception {
			String carDescNumber = "car" + jsonTelemetry.get("Car").asText();
			int carNumber = jsonTelemetry.get("Car").asInt();
			if(carNumber == Integer.parseInt(CarNumberFilter))
            {
				Integer gear = jsonTelemetry.get("telemetry").get("Gear").intValue(); 
				float time = jsonTelemetry.get("time").floatValue();
				int count = 0;
				out.collect(new Tuple4<>(carDescNumber, time, gear, count));
            }
		}
	}
	
	// Reduce Function - Sum samples and count
	// This funciton return, for each car, the sum of two speed measurements and increment a conter.
	// The counter is used for the average calculation.
	static class CountReducer implements ReduceFunction<Tuple4<String, Float, Integer, Integer>> {
		@Override
		public Tuple4<String, Float, Integer, Integer> reduce(Tuple4<String, Float, Integer, Integer> value1, Tuple4<String, Float, Integer, Integer> value2) {
			String car = value1.f0;
			float time1 = value1.f1;
			float time2 = value2.f1;
			Integer gear1 = value1.f2; 
			Integer gear2 = value2.f2; 
			int count = value1.f3;
			if(  gear1 != gear2 )
				count = count+1;
			if( activeDebug )
				System.out.println(System.currentTimeMillis()/1000 + " : time " + time2 + " gear " + gear2 + " count " + count);
			return new Tuple4<>(car, time2, gear2, count);
		}
	}
	
	// FlatMap Function - Average
	// Calculates the average
	static class CountMapper implements FlatMapFunction<Tuple4<String, Float, Integer, Integer>, Tuple4<String, Float, Integer, Integer>> {
		@Override
		public void flatMap(Tuple4<String, Float, Integer, Integer> carInfo, Collector<Tuple4<String, Float, Integer, Integer>> out) throws Exception {
			String car = carInfo.f0;
			float time = carInfo.f1;
			Integer gear = carInfo.f2; 
			int count = carInfo.f3;
			out.collect(  new Tuple4<>( car , time, gear, count )  );
		}
	}
	
	// Map Function - Print average    
	static class CountPrinter implements MapFunction<Tuple4<String, Float, Integer, Integer>, String> {
		@Override
		public String map(Tuple4<String, Float, Integer, Integer> avgEntry) throws Exception {
			String car = avgEntry.f0;
			float time = avgEntry.f1;
			Integer gear = avgEntry.f2; 
			int count = avgEntry.f3;
			return  String.format("[%s] - %s - Current gear: %2d - Time: %.2f - Changes gear(%2ds): %2d ", System.currentTimeMillis()/1000, car , gear, time, delay, count ) ;
		}
	}
	
}
