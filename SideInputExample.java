import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import java.util.Map;

public class SideInputExample {
    public static void main(String[] args) {
        Pipeline p =Pipeline.create();
        PCollection<KV<String,String>> pReturn = p.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/return.csv")).apply(ParDo.of(new DoFn<String, KV<String, String>>() {
        @ProcessElement
        public void process(ProcessContext c){
        String arr[]=c.element().split(",");
        c.output(KV.of(arr[0],arr[1]));
       }
   }));
        PCollectionView<Map<String,String>> pMap=pReturn.apply(View.asMap());
        PCollection<String> pCustList =p.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/cust_order.csv"));
        pCustList.apply(ParDo.of(new DoFn<String,Void>(){
            @ProcessElement
            public void process(ProcessContext c){
                Map<String,String> pSideInput = c.sideInput(pMap);
                String arr[] = c.element().split(",");
                String custName = pSideInput.get(arr[0]);
                if(custName==null){
                    System.out.println(c.element());
                }
            }
        }).withSideInputs(pMap));
    }
}
