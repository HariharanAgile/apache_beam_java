package com.dataflow.sample;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.text.ParseException;
import java.util.Arrays;

public class Solo {
    public interface Options extends DataflowPipelineOptions {
    }

    public static void main(String[] args) {
       Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//               create().as(Options.class);

        options.setTempLocation("gs://my-project-25411.appspot.com/mybucket");
        options.setProject("my-project-25411");
        options.setRunner(DataflowRunner.class);
        options.setRegion("us-central1");
        Pipeline pipeline = Pipeline.create(options);

        TableSchema schema = new TableSchema().setFields(
                Arrays.asList(
//                        new TableFieldSchema().setName("date").setType("DATE").setMode("REQUIRED"),
                        new TableFieldSchema().setName("state").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("positive").setType("STRING").setMode("REQUIRED")
                )
        );
        PCollection<String> fetch = pipeline.apply(TextIO.read().from("gs://my-project-25411.appspot.com/dataproc_use_case/sample_test.csv"));
        PCollection<TableRow> collection1 = fetch.apply(ParDo.of(new ViewClass()));
        collection1.apply(BigQueryIO.writeTableRows().to("my-project-25411:agile_labs.details")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withoutValidation().withSchema(schema));
        pipeline.run().waitUntilFinish();


    }

    public static class ViewClass extends DoFn<String, TableRow> {
        @ProcessElement
        public void processing(ProcessContext processContext) throws ParseException {

            String s1 = processContext.element();
            String[] data = s1.split(",");
            String state = data[0];
            String positive = data[1];
            TableRow row = new TableRow().set("state", state).set("positive", positive);
            processContext.output(row);
            }
        }
    }

