package com.iot.fitness.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.cloud.teleport.templates.TextToPubsub;

/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import com.google.cloud.teleport.templates.TextToPubsub.Options;
import com.iot.fitness.gcp.WordCount.CountWords;
import com.iot.fitness.gcp.WordCount.ExtractWordsFn;
import com.iot.fitness.gcp.common.ExampleUtils;

import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvToPubSub extends TextToPubsub {
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);
	private static final Logger log = LoggerFactory.getLogger(TextToPubsub.class);

	/**
	 * Main entry-point for the pipeline. Reads in the command-line arguments,
	 * parses them, and executes the pipeline.
	 *
	 * @param args
	 *            Arguments passed in from the command-line.
	 */
	public static void main(String[] args) {

		// Parse the user options passed from the command-line
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		run(options);
	}

	private static class csvToNdJson extends DoFn<String, String> {
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			log.info("HIT csvToNdJson");
			CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
			CsvMapper csvMapper = new CsvMapper();

			Object readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(c.element()).readAll();

			String[] elementArray = c.element().split(",");
			String jsonReturn = "{\"Member_ID\":" + elementArray[0] + "," + "\"First_Name\":\"" + elementArray[1]
					+ "\"," + "\"Last_Name\":\"" + elementArray[2] + "\"," + "\"Gender\":\"" + elementArray[3] + "\","
					+ "\"Birth_Date\":\"" + elementArray[4] + "\"," + "\"Height\":" + elementArray[5] + ","
					+ "\"Weight\":" + elementArray[6] + "," + "\"Hours_Sleep\":" + elementArray[7] + ","
					+ "\"Calories_Consumed\":" + elementArray[8] + "," + "\"Exercise_Calories_Burned\":"
					+ elementArray[9] + "," + "\"Date\":\"" + elementArray[10] + "\"}";

			log.info("JSON RETURN = " + jsonReturn);

			log.info(("c = " + c));
			log.info(("c.toString = " + c.toString()));
			log.info(("c.element = " + c.element()));
			ObjectMapper mapper = new ObjectMapper();
			log.info("readall = " + readAll);

			String jsonString = "";
			jsonString = mapper.writeValueAsString(readAll);

			log.info("JSON STRING" + jsonString);
			System.out.println("JSON STRING" + jsonString);
			c.output(jsonReturn);
		}
	}

	/**
	 * Executes the pipeline with the provided execution parameters.
	 *
	 * @param options
	 *            The execution parameters.
	 */
	public static PipelineResult run(Options options) {
		// Create the pipeline.
		Pipeline pipeline = Pipeline.create(options);

		log.info(("FILE PATTERN" + options.getInputFilePattern()));
		/*
		 * Steps: 1) Read from the text source. 2) Write each text record to
		 * Pub/Sub
		 */
		pipeline.apply("Read Text Data",
				TextIO.read().from(options.getInputFilePattern()).watchForNewFiles(DEFAULT_POLL_INTERVAL,
						Watch.Growth.never()))
				.apply("Transform csv to ND Json", ParDo.of(new csvToNdJson()))
				.apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));
		log.info("file patter = " + options.getInputFilePattern());
		return pipeline.run();
	}
}