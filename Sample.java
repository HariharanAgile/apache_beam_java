import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
public class Sample {
    public static  void main(String[] args){

        Pipeline pipeline = Pipeline.create();
        PCollection<String> output = pipeline.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/sales.csv")); //PTransform
        output.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/output_sales_table.csv").withNumShards(1).withSuffix(".csv")); //PTransform
        pipeline.run();


    }
}




