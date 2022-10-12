import org.apache.avro.data.Json;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

import static org.apache.avro.data.Json.parseJson;

public class Rest implements Serializable {

    public static void main(String[] args) throws IOException {

        URL url = new URL("https://www.googleapis.com/youtube/v3/videos/?part=snippet,statistics,status&id=B1Xiyne6fUw&list=PLmgF-MUnMxU4F6bnaw4rH-Yb7BUJcifDC&index=3&key=AIzaSyABJGUu9H4Cgr0bVFVshRexJdUrTiRsHYo");

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.connect();


        int responseCode = conn.getResponseCode();

        if (responseCode != 200) {
            throw new RuntimeException("HttpResponseCode: " + responseCode);
        } else {

            StringBuilder informationString = new StringBuilder();
            Scanner scanner = new Scanner(url.openStream());

            while (scanner.hasNext()) {
                informationString.append(scanner.nextLine());
            }

            scanner.close();
            String singleString = informationString.toString();
            System.out.println(singleString);


            Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
            Pipeline p = Pipeline.create();
            PCollection<String> p1 = pipeline.apply(Create.of(singleString));
            PCollection<Json> p2 =p1.apply(ParDo.of(new SampleClass()));
            pipeline.run().waitUntilFinish();
        }
    }

    public static class SampleClass extends DoFn<String , Json> implements Serializable{
        @ProcessElement
        public void processing(ProcessContext processContext) {
            String s1= processContext.element();
            processContext.output((Json) parseJson(s1));
        }
    }



}