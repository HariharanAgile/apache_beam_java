import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Flat {
    public static void main(String[] args) {
        Pipeline pipes = Pipeline.create();
        PCollection<String> pCust1 = pipes.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/cust1 - Sheet1.csv"));
        PCollection<String> pCust2 = pipes.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/cust2 - Sheet1.csv"));
        PCollection<String> pCust3 = pipes.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/cust3 - Sheet1.csv"));
        PCollectionList<String> list=PCollectionList.of(pCust1).and(pCust2).and(pCust3);
        PCollection<String> merged=list.apply(Flatten.pCollections());
        merged.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/final_cust_table.csv").withNumShards(1));
        pipes.run().waitUntilFinish();
    }
}
