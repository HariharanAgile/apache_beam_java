import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class Bq {
    public static void main(String[] args) {
//       DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
//        pipelineOptions.setProject("my-project-25411");
//        pipelineOptions.setRegion("us (multiple regions in United States)");
//        pipelineOptions.setRunner(DataflowRunner.class);
//        pipelineOptions.setGcpTempLocation("gs://my-project-25411.appspot.com/mybucket");
//        pipelineOptions.setStagingLocation("gs://my-project-25411.appspot.com/mybucket");
        bigqueryDataUpload.Options options = PipelineOptionsFactory.create().as(bigqueryDataUpload.Options.class);
        options.setTempLocation("gs://my-project-25411.appspot.com/mybucket");
        Pipeline pipe = Pipeline.create(options);

        TableSchema tblschema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("userId").setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName("orderId").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("name").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("productId").setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName("Amount").setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName("order_date").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("country").setType("STRING").setMode("REQUIRED")
        )
        );

        PCollection<String> pInput = pipe.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/user_.csv"));
        pInput.apply(ParDo.of(new DoFn<String, TableRow>(){
            @ProcessElement
            public void processElement(ProcessContext c){
                String[] abc =c.element().split(",");
                if(abc.length==7){
                    if(abc[6].equalsIgnoreCase("India")){
                        TableRow row = new TableRow();
                        row.set("userId",abc[0]);
                        row.set("orderId",abc[1]);
                        row.set("name",abc[2]);
                        row.set("productId",abc[3]);
                        row.set("Amount",abc[4]);
                        row.set("order_date",abc[5]);
                        row.set("country",abc[6]);
                        c.output(row);
                    }
                }
            }
        })).apply(BigQueryIO.writeTableRows().to("my-project-25411:hello.my_table").withSchema(tblschema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND).withoutValidation());
        pipe.run().waitUntilFinish();
    }
}
