import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class BeamTwo {
    static List<CustomerEntity> getCust(){
        CustomerEntity c1=new CustomerEntity("hariharan",22);
        CustomerEntity c2=new CustomerEntity("varun",25);
        CustomerEntity c3=new CustomerEntity("prabhakar",24);
        List<CustomerEntity> list = new ArrayList<CustomerEntity>();
        list.add(c1);
        list.add(c2);
        list.add(c3);
        System.out.println(c1);

        return list;

    }
    public static void main(String[] args) {
        Pipeline pipes=Pipeline.create();
        PCollection<CustomerEntity> out=pipes.apply(Create.of(getCust()));
      //  PCollection<String> def=out.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity cus) -> cus.getAge()).via((CustomerEntity cus) -> cus.getAge()));

        PCollection<String> abc=out.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity cust) -> cust.getName()).via((CustomerEntity cust) -> cust.getName()));

        abc.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/output_cust_table.csv").withNumShards(1).withSuffix(".csv"));
        pipes.run();
    }


}
