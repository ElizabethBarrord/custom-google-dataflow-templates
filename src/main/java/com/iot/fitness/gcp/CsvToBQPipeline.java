package com.iot.fitness.gcp;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */

public class CsvToBQPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(CsvToBQPipeline.class);
	private static String HEADERS = "ID,Code,Value,Date";

	public static class FormatForBigquery extends DoFn<String, TableRow> {

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
			fields.add(new TableFieldSchema().setName("ID").setType("STRING"));
			fields.add(new TableFieldSchema().setName("Code").setType("STRING"));
			fields.add(new TableFieldSchema().setName("Value").setType("STRING"));
			fields.add(new TableFieldSchema().setName("Date").setType("STRING"));

			return new TableSchema().setFields(fields);
		}
	}

	public static void main(String[] args) throws Throwable {
		// Currently hard-code the variables, this can be passed into as
		// parameters
		String sourceFilePath = "gs://staging-bucket-test-barrord/temp/sample.csv";
		String tempLocationPath = "gs://staging-bucket-test-barrord/temp/bq";
		boolean isStreaming = false;
		TableReference tableRef = new TableReference();
		// Replace this with your own GCP project ids
		tableRef.setProjectId("iot-fitness-data-demo-barrord");
		tableRef.setDatasetId("sample");
		tableRef.setTableId("sample");

		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		// This is required for BigQuery
		options.setTempLocation(tempLocationPath);
		options.setJobName("csvtobq");
		Pipeline p = Pipeline.create(options);

		p.apply("Read CSV File", TextIO.read().from(sourceFilePath))
				.apply("Log messages", ParDo.of(new DoFn<String, String>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						LOG.info("Processing row: " + c.element());
						c.output(c.element());
					}
				})).apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))
				.apply("Write into BigQuery",
						BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatForBigquery.getSchema())
								.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
								.withWriteDisposition(isStreaming ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
										: BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		p.run().waitUntilFinish();

	}

}
