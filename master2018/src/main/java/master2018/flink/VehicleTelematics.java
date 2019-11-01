package master2018.flink;

import java.util.Iterator;
import java.util.Objects;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class VehicleTelematics {

	public static void main(String[] args) {

		// Configure the properties needed. Read the file with parallelism equal to 1.
		
		final StreamExecutionEnvironment environ = StreamExecutionEnvironment.getExecutionEnvironment();
		environ.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		String inFile = args[0];
		String outFolder = args[1];
		DataStream<String> data = environ.readTextFile(inFile).setParallelism(1);

		// Exercise 1: Speed radar, cars with 'speed' higher than 90 mph.
		// Output Format (Time, VID, XWay, Segment, Direction, Speed)

		SingleOutputStreamOperator<Tuple6<String, String, String, String, String, Short>> speedFines = data.flatMap(new FlatMapFunction
				<String, Tuple6<String, String, String, String, String, Short>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(String input, Collector<Tuple6<String, String, String, String, String, Short>> collector) throws Exception {
				
				String[] wordArray = input.split(",");
				if (wordArray.length != 8) {
					throw new Exception("Format not proper");
				}
				
				Short speed = Short.parseShort(wordArray[2]);
				if (speed > 90) {
					Tuple6<String, String, String, String, String, Short> outputRow = new Tuple6
							<String, String, String, String, String, Short>(wordArray[0], wordArray[1], wordArray[3], 
									wordArray[6], wordArray[5], speed);
					collector.collect(outputRow);
				}
			}
		});

		// Exercise 2: Average speed radar between segments 52 and 56, more than 60 mph.
		// Output Format (Time1, Time2, VID, XWay, Direction, AvgSpeed)

		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Byte, Byte, Double>> avgSpeedFines = data.flatMap(new FlatMapFunction
				<String, Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(String input, Collector<Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer>> tuple) throws Exception {
				
				String[] array = input.split(",");
				if (array.length != 8) {
					throw new Exception("Format not proper");
				}
				
				short segm = Short.parseShort(array[6]);
				if ((segm >= 52) && (segm <= 56)){
					Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer> ret = new Tuple8<>
						(Integer.parseInt(array[0]),Integer.parseInt(array[0]),Integer.parseInt(array[1]),
								Byte.parseByte(array[2]),Byte.parseByte(array[3]),Byte.parseByte(array[5]),
								Byte.parseByte(array[6]),Integer.parseInt(array[7]));
					tuple.collect(ret);
				}
			}

		}).setParallelism(10).assignTimestampsAndWatermarks(new AscendingTimestampExtractor
				<Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer> tuple) {

				return tuple.f0 * 1000;
			}
		}).setParallelism(10).keyBy(2,4,5).window(EventTimeSessionWindows.withGap(Time.seconds(180))).apply(new WindowFunction
				<Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer>,
				Tuple6<Integer, Integer, Integer, Byte, Byte, Double>, Tuple, TimeWindow>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void apply(Tuple key, TimeWindow window,
							  Iterable<Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer>> tuple,
							  Collector<Tuple6<Integer, Integer, Integer, Byte, Byte, Double>> out)
					throws Exception {

				int vid = 0, time = 0,time1 = 0, time2 = 0;
				int position = 0, pos1 = 0, pos2 = 0;
				Byte xway = 0, direction = 0, segment = 0;
				Byte segments = 0b00000;

				for (Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer> value : tuple) {

					time = value.f0;
					vid = value.f2;
					xway = value.f4;
					direction = value.f5;
					segment = value.f6;
					position = value.f7;

					segments = (byte) (segments | 0b10000 >> (segment - 52));

					if (pos1 == 0) {
						pos1 = position;
						time1 = time;
					} else  if (position < pos1 && direction == 0) {
						pos1 = position;
						time1 = time;
					} else  if (position < pos1 && direction == 1) {
						pos1 = position;
						time2 = time;
					}

					if (position > pos2 && direction == 0) {
						pos2 = position;
						time2 = time;
					} else if (position > pos2 && direction == 1) {
						pos2 = position;
						time1 = time;
					}

				}

				if (segments == 0b11111) {
					double average_speed = (double) ((pos2 - pos1) * 2.23694 / ((time2 - time1)));
					if (average_speed > 60.0000) {
						out.collect(new Tuple6<Integer, Integer, Integer, Byte, Byte, Double>
							(time1,time2,vid,xway,direction,average_speed));
					}
				}

			}
			
		});

		// Ex3: Accident Reporter, cars in the same position in 4 consecutive events (90 seconds)
		// Output Format (Time1, Time2, VID, XWay, Segment, Direction, Position)

		final SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Byte, Byte, Byte, Integer>> accidents  = data.flatMap(new FlatMapFunction
				<String, Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(String input, Collector<Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, 
					Integer>> tuple) throws Exception {
				
				String[] array = input.split(",");
				if (array.length != 8) {
					throw new Exception("Format not proper");
				}
				
				short spdd = Short.parseShort(array[2]);
				if (spdd == 0){
					Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer> ret = new Tuple8<>
						(Integer.parseInt(array[0]),Integer.parseInt(array[0]),Integer.parseInt(array[1]),
								Byte.parseByte(array[2]),Byte.parseByte(array[3]),Byte.parseByte(array[5]),
								Byte.parseByte(array[6]),Integer.parseInt(array[7]));
					tuple.collect(ret);
				}
			}

		}).setParallelism(1).keyBy(2,3,5).countWindow(4,1).apply(new WindowFunction
				<Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer>, 
				Tuple7<Integer, Integer, Integer, Byte, Byte, Byte, Integer>, Tuple, GlobalWindow>() {

					private static final long serialVersionUID = 1L;

					@Override
		            public void apply(Tuple tuple, GlobalWindow window, 
		            		Iterable<Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer>> input, 
		            		Collector<Tuple7<Integer, Integer, Integer, Byte, Byte, Byte, Integer>> out) throws Exception {

		                Iterator<Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer>> iter = input.iterator();
		                Tuple8<Integer, Integer, Integer, Byte, Byte, Byte, Byte, Integer> tempTuple = iter.next();
		                Integer pos = tempTuple.f7;
		                Integer count = 1;
		                
		                while (iter.hasNext()) {
		                    tempTuple = iter.next();
		                    if (Objects.equals(pos, tempTuple.f7)) {
		                        count++;
		                    } else {
		                        break;
		                    }
		                }
		                
		                if (count == 4)  {
		                    tempTuple.f0 = (int)(tempTuple.f1 - 90);
		                    out.collect(new Tuple7<Integer, Integer, Integer, Byte, Byte, Byte, Integer>
		                    	(tempTuple.f0,tempTuple.f1,tempTuple.f2,tempTuple.f4,tempTuple.f6,tempTuple.f5,tempTuple.f7));
		                }
		                
		            }
					
		        });
		
		// Write on the corresponding 'csv' files in the output folder with parallelism equal to 1.

		speedFines.writeAsCsv(outFolder + "speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		avgSpeedFines.writeAsCsv(outFolder + "avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		accidents.writeAsCsv(outFolder + "accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		try {
			environ.execute("Flink project: Vehicle Telematics");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}