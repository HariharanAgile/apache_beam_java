import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

class MyPart implements Partition.PartitionFn<String> {
    public int partitionFor(String elem,int numPartitions){
        String arr[] = elem.split(",");
        if(arr[3].equals("Los Angeles")){
            return 0;
        }
        else if(arr[3].equals("Phoenix")){
            return 1;
        }
        else{
            return 2;
        }

    }
}

public class PartitionExample {
    public static void main(String[] args) {
        Pipeline mypipe= Pipeline.create();
        PCollection<String> cust=mypipe.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/Partition.csv"));
        PCollectionList<String> part=cust.apply(Partition.of(3, new MyPart()));
        PCollection<String> p0=part.get(0);
        PCollection<String> p1=part.get(1);
        PCollection<String> p2=part.get(2);

        p0.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/p0.csv").withNumShards(1).withSuffix(".csv"));
        p1.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/p1.csv").withNumShards(1).withSuffix(".csv"));
        p2.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/p2.csv").withNumShards(1).withSuffix(".csv"));

        mypipe.run().waitUntilFinish();

    }
}
