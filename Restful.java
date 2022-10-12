import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Restful {

    public static void main(String[] args) {

        /*
        Maven dependency for JSON-simple:
            <dependency>
                <groupId>com.googlecode.json-simple</groupId>
                <artifactId>json-simple</artifactId>
                <version>1.1.1</version>
            </dependency>
         */

        try {
            //Public API:
            //https://www.metaweather.com/api/location/search/?query=<CITY>
            //https://www.metaweather.com/api/location/44418/

            URL url = new URL("https://www.googleapis.com/youtube/v3/videos/?part=snippet,statistics&id=B1Xiyne6fUw&list=PLmgF-MUnMxU4F6bnaw4rH-Yb7BUJcifDC&index=3&key=AIzaSyABJGUu9H4Cgr0bVFVshRexJdUrTiRsHYo");

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.connect();

            //Check if connect is made
            int responseCode = conn.getResponseCode();

            // 200 OK
            if (responseCode != 200) {
                throw new RuntimeException("HttpResponseCode: " + responseCode);
            } else {

                StringBuilder informationString = new StringBuilder();
                Scanner scanner = new Scanner(url.openStream());

                while (scanner.hasNext()) {
                    informationString.append(scanner.nextLine());
                }
                //Close the scanner
                scanner.close();
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
                LocalDateTime now = LocalDateTime.now();

             //   System.out.println(informationString);
                String singleString = informationString.toString();
             //   System.out.println(singleString.getClass().getSimpleName());
               System.out.println(singleString);
                JSONObject jsonObject = new JSONObject(singleString.toString());
                JSONArray s = (JSONArray) jsonObject.get("items");
                JSONObject obj1 = new JSONObject(s.get(0).toString());
                JSONObject obj2 =(JSONObject) obj1.get("snippet");
                JSONObject obj3 =(JSONObject) obj2.get("localized");
                JSONObject obj4=(JSONObject)  obj1.get("statistics");


                String description=obj3.get("description").toString();
                String title=obj3.get("title").toString();
                String viewcount= obj4.get("viewCount").toString();
                String likecount= obj4.get("likeCount").toString();
                String commentcount =obj4.get("commentCount").toString();
                String date =dtf.format(now);
                List<String> list = new ArrayList<String>();
                List<String> list1 = Arrays.asList(description,title,viewcount,likecount,commentcount,date);
                list.add(description);
                list.add(title);
                list.add(viewcount);
                list.add(likecount);
                list.add(commentcount);
                list.add(date);
                System.out.println(list);
                Connection connection =null;
                Statement statement=null;
                Class.forName("org.postgresql.Driver");
                connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "postgres", "root12345");
               String query= String.format("insert into Per (description,title,viewcount,likecount,commentcount,date_)  values('%1$s','%2$s','%3$s','%4$s','%5$s','%6$s')",description,title,viewcount,likecount,commentcount,date);
             //   String query2="UPDATE cid set cname='india' where cid='c-5'";

             //  String filepath="/Users/hariharanthirumurugan/Downloads/temp_new.csv";
                System.out.println(query);
                statement=connection.createStatement();
                statement.executeUpdate(query);

              //  JSONArray result = jsonObject.getJSONArray("items").getJSONArray("");

             //   JSONObject getSth = jsonObject.getJSONObject("LanguageLevels");


             //   Object level = getSt.get("2");
              //  Object a  = jsonObj.get("items");
              //  System.out.println(a.getClass("description"));

               // System.out.println(jsonObj.get("items"));
              //  System.out.println(a.get("")


                Pipeline pipeline = Pipeline.create();
                PCollection<String> p1 = pipeline.apply(Create.of(list1));
                p1.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/abcde.csv").withNumShards(1).withSuffix(".csv")); //PTransform
                PCollection<String> p6 =pipeline.apply(Create.of(list1));
                p6.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/charm.csv").withNumShards(1).withSuffix(".csv"));
                pipeline.run().waitUntilFinish();
             /*   JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject) parser.parse(singleString);
                System.out.println(json);
                System.out.println(json.getClass().getSimpleName()); */
               /* //JSON simple library Setup with Maven is used to convert strings to JSON
                JSONParser parse = new JSONParser();
                JSONArray dataObject = (JSONArray) parse.parse(String.valueOf(informationString));

                //Get the first JSON object in the JSON array
                System.out.println(dataObject.get(0));

                JSONObject countryData = (JSONObject) dataObject.get(0);

                System.out.println(countryData.get("woeid")); */

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



}