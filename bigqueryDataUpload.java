import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class bigqueryDataUpload {
    public interface Options extends DataflowPipelineOptions {

    }
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.create().as(Options.class);
        options.setTempLocation("gs://my-project-25411.appspot.com/mybucket");
        Pipeline pipeline = Pipeline.create(options);

        TableSchema schema = new TableSchema().setFields(
                Arrays.asList(
                        new TableFieldSchema().setName("emp_id").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("emp_name").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("emp_dept").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("emp_sal").setType("INTEGER").setMode("REQUIRED")
                )
        );

        PCollection<String> collection = pipeline
                .apply(TextIO.read().from("gs://my-project-25411.appspot.com/mybucket/sample_New1_emp_details.csv"));

        PCollection<TableRow> collection1=collection.apply(ParDo.of(new ViewClass()));



        collection1.apply(BigQueryIO.writeTableRows().to("my-project-25411:hello.emp_details")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withoutValidation().withSchema(schema));


        pipeline.run().waitUntilFinish();
    }

    public static class ViewClass extends DoFn<String,TableRow> {
        @ProcessElement
        public void processing(ProcessContext processContext){
            String s1 = processContext.element();
            System.out.println(s1);
            if (!s1.contains("emp")){
                String[] data = s1.split(",");
                int emp_id = Integer.parseInt(data[0]);
                String name = data[1];
                String dept = data[2];
                int salary = Integer.parseInt(data[3]);

                TableRow row = new TableRow().set("emp_id",emp_id).set("emp_name",name).set("emp_dept",dept).set("emp_sal",salary);
                processContext.output(row);
            }
        }
    }
}
