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

/**
 * The {@code TextToPubsubStream} is a streaming version of {@code TextToPubsub} pipeline that
 * publishes records to Cloud Pub/Sub from a set of files. The pipeline continuously polls for new
 * files, reads them row-by-row and publishes each record as a string message. The polling interval
 * is fixed and equals to 10 seconds. At the moment, publishing messages with attributes is
 * unsupported.
 *
 * <p>Example Usage:
 *
 * <pre>
 * {@code mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.TextToPubsubStream \
-Dexec.args=" \
--project=${PROJECT_ID} \
--stagingLocation=gs://${STAGING_BUCKET}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
--tempLocation=gs://${STAGING_BUCKET}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
--runner=DataflowRunner \
--inputFilePattern=gs://path/to/*.csv \
--outputTopic=projects/${PROJECT_ID}/topics/${TOPIC_NAME}"
 * }
 * </pre>
 *
 */
public class CsvToPubSub extends TextToPubsub {
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);
  private static final Logger log = LoggerFactory.getLogger(TextToPubsub.class);
  /**
   * Main entry-point for the pipeline. Reads in the
   * command-line arguments, parses them, and executes
   * the pipeline.
   *
   * @param args  Arguments passed in from the command-line.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options = PipelineOptionsFactory
      .fromArgs(args)
      .withValidation()
      .as(Options.class);

    run(options);
  }

  
  static class ExtractWordsFn extends DoFn<String, String> {
	    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
	    private final Distribution lineLenDist = Metrics.distribution(
	        ExtractWordsFn.class, "lineLenDistro");

	    @ProcessElement
	    public void processElement(@Element String element, OutputReceiver<String> receiver) {
	      lineLenDist.update(element.length());
	      if (element.trim().isEmpty()) {
	        emptyLines.inc();
	      }

	      // Split the line into words.
	      String[] words = element.split(ExampleUtils.TOKENIZER_PATTERN, -1);

	      // Output each word encountered into the output PCollection.
	      for (String word : words) {
	        if (!word.isEmpty()) {
	          receiver.output(word);
	        }
	      }
	    }
	  }
  
  
  
  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   *
   * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
   * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
   * modular testing, and an improved monitoring experience.
   */
  public static class convertToJson extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

      return wordCounts;
    }
  }
  
  
  /** A SimpleFunction that converts a Word and Count into a printable string. */
  public static class FormatAsText extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }
  
  private static class csvToNdJson extends DoFn<String, String> {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
    	  log.info("HIT csvToNdJson");
    	  CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
          CsvMapper csvMapper = new CsvMapper();
    	  
          // Read data from CSV file
//          List<Object> readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(c.element()).readAll();
          Object readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(c.element()).readAll();
          
          String[] elementArray = c.element().split(",");
          String jsonReturn = "{\"Member_ID\":\""+elementArray[0] + "\","+
          "\"First_Name\":\"" + elementArray[1] + "\","+
        		  "\"Last_Name\":\""+ elementArray[2] + "\","+
          "\"Gender\":\"" + elementArray[3] + "\"," +
        		  "\"Birth_Date\":\"" + elementArray[4] + "\","+
          "\"Height\":\""+elementArray[5] + "\","+
        		  "\"Weight\":\""+elementArray[6] + "\"," +
          "\"Hours_Sleep\":\""+elementArray[7] + "\"," +
          "\"Calories_Consumed\":\"" + elementArray[8] + "\"," +
        		  "\"Exercise_Calories_Burned\":\"" + elementArray[9] + "\"," +
          "\"Date\":\"" + elementArray[10] + "\"}";
          
          log.info("JSON RETURN = " + jsonReturn);
          
          log.info(("c = " + c));
          log.info(("c.toString = " + c.toString()));
          log.info(("c.element = " + c.element()));
          ObjectMapper mapper = new ObjectMapper();
          log.info("readall = " + readAll);
          
          String jsonString = "";
          jsonString = mapper.writeValueAsString(readAll);
//          for (Object obj : readAll) {
//          	jsonString += mapper.writeValueAsString(obj);
//          	jsonString += "\n";
//          }
          log.info("JSON STRING" + jsonString);
          System.out.println("JSON STRING" + jsonString);
          c.output(jsonReturn);
      }
  }
  
//  /** A SimpleFunction that converts a Word and Count into a printable string. */
//  public static class csvToNdJsonMap extends SimpleFunction<String,String> {
//	  @Override
//	  public String apply(<String,String> input) {
////      return input.getKey() + ": " + input.getValue();
//	  
//	  CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
//      CsvMapper csvMapper = new CsvMapper();
//	  
//      // Read data from CSV file
//      List<Object> readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(input.toString()).readAll();
//      ObjectMapper mapper = new ObjectMapper();
//      
//      String jsonString = "";
//      for (Object obj : readAll) {
//      	jsonString += mapper.writeValueAsString(obj);
//      	jsonString += "\n";
//      }
//      System.out.println("JSON STRING" + jsonString);
//      return jsonString;
//    }
//  }
  
  

  /**
   * Executes the pipeline with the provided execution
   * parameters.
   *
   * @param options The execution parameters.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    log.info(("FILE PATTERN" + options.getInputFilePattern()));
    /*
     * Steps:
     *  1) Read from the text source.
     *  2) Write each text record to Pub/Sub
     */
    pipeline
      .apply(
        "Read Text Data",
        TextIO.read()
        .from(options.getInputFilePattern())
        .watchForNewFiles(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))
      .apply("Transform csv to ND Json", ParDo.of(new csvToNdJson()))
//      .apply(MapElements.via(new csvToNdJsonMap()))
//      .apply(new convertToJson())
//      .apply(MapElements.via(new FormatAsText()))
      .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

    return pipeline.run();
  }
}