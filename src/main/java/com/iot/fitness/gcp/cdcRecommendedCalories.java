package com.iot.fitness.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.templates.TextToPubsub;
import com.iot.fitness.gcp.CsvToBQCdcCal.FormatForBigquery;

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

import com.iot.fitness.gcp.WordCount.CountWords;
import com.iot.fitness.gcp.WordCount.ExtractWordsFn;
import com.iot.fitness.gcp.common.ExampleUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
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
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class cdcRecommendedCalories extends TextToPubsub {
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);
	private static final Logger log = LoggerFactory.getLogger(TextToPubsub.class);
	private static final Logger LOG = LoggerFactory.getLogger(CsvToBQPipeline.class);
	private static String HEADERS = "Gender,Min_Age,Max_Age,Sedentary,Moderately_Active,Active";

	private static class FormatForBigquery extends DoFn<String, TableRow> {
		
		private String[] columnNames = HEADERS.split(",");

		@ProcessElement
		public void processElement(ProcessContext c) {
			TableRow row = new TableRow();
			String[] parts = c.element().split(",");

			if (!c.element().contains(HEADERS)) {
				for (int i = 0; i < parts.length; i++) {
					// No typy conversion at the moment.
					row.set(columnNames[i], parts[i]);
				}
				c.output(row);
			}
	}
		
		/** Defines the BigQuery schema used for the output. */

		static TableSchema getSchema() {
			List<TableFieldSchema> fields = new ArrayList<>();
			// Currently store all values as String
			fields.add(new TableFieldSchema().setName("Gender").setType("STRING"));
			fields.add(new TableFieldSchema().setName("Min_Age").setType("INTEGER"));
			fields.add(new TableFieldSchema().setName("Max_Age").setType("INTEGER"));
			fields.add(new TableFieldSchema().setName("Sedentary").setType("INTEGER"));
			fields.add(new TableFieldSchema().setName("Moderately_Active").setType("INTEGER"));
			fields.add(new TableFieldSchema().setName("Active").setType("INTEGER"));
			return new TableSchema().setFields(fields);
		}
	}
	
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

	/**
	 * Executes the pipeline with the provided execution parameters.
	 *
	 * @param options
	 *            The execution parameters.
	 */
	public static PipelineResult run(Options options) {
		// Create the pipeline.
		boolean isStreaming = false;
		Pipeline pipeline = Pipeline.create(options);
		TableReference tableRef = new TableReference();
		tableRef.setProjectId("iot-fitness-demo-barrord");
		tableRef.setDatasetId("Fitness_Data");
		tableRef.setTableId("CDC_Cals_Needed_Data");

		pipeline.apply("Read CSV File", TextIO.read().from(options.getInputFilePattern()))
				.apply("Log messages", ParDo.of(new DoFn<String, String>() {
					@ProcessElement
					public void proscessElement(ProcessContext c) {
						LOG.info("Processing row: " + c.element());
						c.output(c.element());
					}
				})).apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))
				.apply("Write into BigQuery",
						BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatForBigquery.getSchema())
								.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
								.withWriteDisposition(isStreaming ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
										: BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		pipeline.run().waitUntilFinish();
		return pipeline.run();
	}
}