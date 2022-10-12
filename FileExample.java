import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
public class FileExample{

public static void main(String[]args) throws IOException {

    File file = new File("/Users/hariharanthirumurugan/Downloads/temp_new.csv");
    FileWriter outputfile = new FileWriter(file);
    CSVWriter writer = new CSVWriter(outputfile);
    String[] header = { "Name", "Class", "Marks" };
    writer.writeNext(header);
    String[] data1 = { "Aman", "10", "620" };
    writer.writeNext(data1);
    String[] data2 = { "Suraj", "10", "630" };
    writer.writeNext(data2);
    writer.close();


}
    }

    /*HashMap<String, String> hash_map = new HashMap<String, String>();
    hash_map.put("Name", "Hari");
    hash_map.put("Age", "22");
    System.out.println(hash_map); */



   /* Pipeline p=Pipeline.create();
    hash_map.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/sales_output.csv").withHeader("A,B").withNumShards(1).withSuffix(".csv"));
    p.run();*/





//
//public class FileExample {
//    public static void main(String[] args){
//
//        Pipeline p = Pipeline.create();
//        PCollection<CustomerEntity> pList = p.apply(Create.of(getCustomers()));
//        PCollection<String> Pstrlist1=pList.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity cust) -> cust.getName()));
//        Pstrlist1.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/InputOutput1.csv").withHeader("Name").withNumShards(1).withSuffix(".csv"));
//        p.run();
//
//    }
//  static List<CustomerEntity> getCustomers(){


//        CustomerEntity c1= new CustomerEntity("Hari",22);
//        CustomerEntity c2= new CustomerEntity("Praveen",27);
//        List<CustomerEntity> list = new ArrayList<CustomerEntity>();
//        list.add(c1);
//        list.add(c2);
//      PCollection<String> lines = CustomerEntity;
//      lines.apply(ParDo.of(new DoFn<String, String>() {
//          {@literal @}ProcessElement
//          public void processElement(ProcessContext c) {
//              String line = c.element();
//              for (String word : line.split("[^a-zA-Z']+")) {
//                  c.output(word);
//              }
//          }}));
//
//        return list;
//    }
//}
