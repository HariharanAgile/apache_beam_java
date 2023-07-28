package com.dataflow.sample;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.Arrays;

public class Two {
    public interface Options extends DataflowPipelineOptions {
    }
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setTempLocation("gs://my-project-25411.appspot.com/mybucket");
        options.setProject("my-project-25411");
        options.setRunner(DataflowRunner.class);
        options.setRegion("us-central1");

        TableSchema schema = new TableSchema().setFields(
                Arrays.asList(
                        new TableFieldSchema().setName("review_id").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("user_id").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("business_id").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("stars").setType("FLOAT").setMode("REQUIRED"),
                        new TableFieldSchema().setName("useful").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("funny").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("cool").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("text").setType("STRING").setMode("REQUIRED")

                )
        );

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> abc = pipeline.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/OneDrive_1_12-06-2023/yelp_academic_dataset_review.json"));
        PCollection<GenericRecord> def=abc.apply(ParDo.of(new JsonToAvroFn()));//.setCoder(AvroCoder.of(.class));
        PCollection<TableRow>FinalTable= def.apply(ParDo.of(new GenericRecordToTableRowFn()));

        FinalTable.apply(BigQueryIO.writeTableRows().to("my-project-25411:agile_labs.my_test")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withoutValidation().withSchema(schema));
        pipeline.run().waitUntilFinish();





    }
}


class JsonToAvroFn extends DoFn<String, GenericRecord> {
    @ProcessElement
    public void processElement(ProcessContext c) throws ParseException {
        String json = c.element();
        GenericRecord record = convertJsonToAvro(json);

        c.output(record);
    }
    public GenericRecord convertJsonToAvro(String json) {
        JSONObject jsonObject = new JSONObject(json);
        Schema avroSchema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"review_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"business_id\",\"type\":\"string\"},{\"name\":\"stars\",\"type\":\"float\"},{\"name\":\"useful\",\"type\":\"int\"},{\"name\":\"funny\",\"type\":\"int\"},{\"name\":\"cool\",\"type\":\"string\"},{\"name\":\"text\",\"type\":\"string\"}]}");
        GenericRecord record = new GenericData.Record(avroSchema);
        for (Schema.Field field : avroSchema.getFields()) {
            String fieldName = field.name();
            Object fieldValue = jsonObject.get(fieldName);
            if (fieldValue != null) {
                if (fieldValue instanceof String) {
                    record.put(fieldName, new Utf8((String) fieldValue));
                } else {
                    record.put(fieldName, fieldValue);
                }
            }
        }

        return record;
    }
}


class GenericRecordToTableRowFn extends DoFn<GenericRecord, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        GenericRecord genericRecord = c.element();

        try {
            TableRow tableRow = convertGenericRecordToTableRow(genericRecord);
            c.output(tableRow);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private TableRow convertGenericRecordToTableRow(GenericRecord genericRecord) {
        TableRow tableRow = new TableRow();
        for (Schema.Field field : genericRecord.getSchema().getFields()) {
            String fieldName = field.name();
            Object fieldValue = genericRecord.get(fieldName);
            tableRow.set(fieldName, fieldValue);
        }
        return tableRow;
    }
}







