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

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * The "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesExercise {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

    /** Creates a job using the source and sink provided. */
    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source);

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy
                        .<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // create the pipeline
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        LongRidesExercise job =
                new LongRidesExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    // Long = type of key
    // TaxiRide = type of input
    // Long = type of output
    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {
        private transient MapState<Long, Long> rideIdTimeStamp;
        private static long MAX_DURATION = Duration.ofHours(2).toMillis();

        // Called once during initialization.
        @Override
        public void open(Configuration config) throws Exception {
            MapStateDescriptor<Long, Long> pendingRide =
                    new MapStateDescriptor<>("pendingRide", Long.class, Long.class);

            rideIdTimeStamp = getRuntimeContext().getMapState(pendingRide);
        }

        // Called as each ride arrives to be processed.
        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out) throws Exception {

            long rideId = ride.rideId;
            long eventTime = ride.getEventTimeMillis();
            TimerService timerService = context.timerService();

            if (eventTime <= timerService.currentWatermark()) {

            } else {
                System.out.println("EVENTTIME: " + eventTime + "WATERMARK  " + timerService.currentWatermark());
                if (ride.isStart) {
                    System.out.println("The id for the start event is: " + rideId);
                    if (rideIdTimeStamp.get(rideId) != null) {
                        System.out.println("The id is: " + rideId + "and it's already in the map");
                        // retrieve rideID already present, end already arrived
                        if ((rideIdTimeStamp.get(rideId) - eventTime) >= MAX_DURATION) {
                            System.out.println("The id is: " + rideId + " and has passed more than two hours");
                            // it's more than two hours
                            out.collect(rideId);
                        }
                        System.out.println("The id is: " + rideId + " and I'm deleting it");
                        // in both case, I'll remove the entry
                        System.out.println("The rideStamp contains the key: " + rideIdTimeStamp.contains(rideId));
                        rideIdTimeStamp.remove(rideId);
                        System.out.println("The rideStamp contains the key: " + rideIdTimeStamp.contains(rideId));
                        System.out.println("The rideStamp is: " + rideIdTimeStamp.isEmpty());
                    } else {
                        System.out.println("The start event has arrived before the end: " + rideId);
                        // the start event is arrived before the end event
                        System.out.println("The start event has arrived before the end and rideStamp contains the key: " + rideIdTimeStamp.contains(rideId));
                        rideIdTimeStamp.put(rideId, eventTime);
                        System.out.println("The start event has arrived before the end and rideStamp contains the key: " + rideIdTimeStamp.contains(rideId));
                        // schedule a callback for when 2 hours have passed
                        timerService.registerEventTimeTimer(MAX_DURATION);
                    }
                } else {
                    System.out.println("The id for the end event is: " + rideId);
                    // if the event arrives and it's already present the start event, I'll check if are passed two hours
                    if (rideIdTimeStamp.get(rideId) != null) {
                        System.out.println("The id is: " + rideId + " and it's already in the map");
                        // retrieve rideID already present
                        if ((eventTime - rideIdTimeStamp.get(rideId)) >= MAX_DURATION) {
                            System.out.println("The id is: " + rideId + "and has passed more than two hours");
                            // it's more than two hours
                            out.collect(rideId);
                        }
                        System.out.println("The id is: " + rideId + " and I'm deleting it");
                        // in both case, I'll remove the entry
                        System.out.println("The rideStamp contains the key: " + rideIdTimeStamp.contains(rideId));
                        rideIdTimeStamp.remove(rideId);
                        System.out.println("The rideStamp contains the key: " + rideIdTimeStamp.contains(rideId));
                        System.out.println("The rideStamp is: " + rideIdTimeStamp.isEmpty());
                    } else {
                        // if it's a ride end event, I'll put it in my map
                        // and I'll wait for the start event
                        System.out.println("The rideStamp does not contains the key: " + rideIdTimeStamp.contains(rideId));
                        rideIdTimeStamp.put(rideId, eventTime);
                        System.out.println("The rideStamp contains the key now: " + rideIdTimeStamp.contains(rideId));
                    }
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out) throws Exception{
            // after two hours, I'll forward the warning
            long rideId = context.getCurrentKey();

            rideIdTimeStamp.remove(rideId);
            out.collect(rideId);
        }
    }
}
