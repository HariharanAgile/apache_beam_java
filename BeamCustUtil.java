import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class Cust {
    public static Schema getSchema(){
        String SCHEMA_STRING ="{\"namespace\":\"training\",\n"+ " \"type\":\"record\",\n"
                +"\"name\":\"Example\",\n"
                +" \"fields\": [\n"
                +"   {\"name\": \"SessionId\" , \"type\": \"string\"}, \n"
                +"   {\"name\": \"UserId\",\"type\": \"string\"}, \n"
                +"   {\"name\": \"UserName\",\"type\": \"string\"}, \n"
                + "   {\"name\": \"VideoId\",\"type\": \"string\"}, \n"
                +"   {\"name\": \"Duration\",\"type\": \"string\"}, \n"
                +"   {\"name\": \"total_time\",\"type\": \"string\"}, \n"
                +"   {\"name\": \"sex\",\"type\": \"string\"} \n"
                + " ]\n"
                + "}";
        Schema SCHEMA =new Schema.Parser().parse(SCHEMA_STRING);
        return SCHEMA;
    }
}

class ConvertCsvtoGeneric extends SimpleFunction<String, GenericRecord>{
    @Override
    public GenericRecord apply(String input){
        String[] a= input.split(",");
        Schema schema = Cust.getSchema();
        GenericRecord record  = new GenericData.Record(schema);
        record.put("SessionId",a[0]);
        record.put("UserId",a[1]);
        record.put("UserName",a[2]);
        record.put("VideoId",a[3]);
        record.put("Duration",a[4]);
        record.put("total_time",a[5]);
        record.put("sex",a[6]);

        return record;
    }
 }
public class BeamCustUtil{
    public static void main(String[] args) {
        Pipeline pipes = Pipeline.create();
        Schema schema = Cust.getSchema();
        PCollection<GenericRecord> pOut =pipes.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/user-2.csv")).apply(MapElements.via(new ConvertCsvtoGeneric()))
                .setCoder(AvroCoder.of(GenericRecord.class,schema));
        pOut.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(schema)).to("/Users/hariharanthirumurugan/Downloads/parq").withNumShards(1).withSuffix(".parquet"));
        pipes.run().waitUntilFinish();
    }
}