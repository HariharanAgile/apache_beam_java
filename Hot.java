import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class User extends SimpleFunction<String,String> {
    @Override
    public String apply(String input){
        String arr[] = input.split(",");
        String SId=arr[0];
        String UId=arr[1];
        String Uname=arr[2];
        String VId=arr[3];
        String duration=arr[4];
        String startTime=arr[5];
        String sex=arr[6];

        String output="";
        if(sex.equals("1")){

            output=SId+","+UId+","+Uname+","+VId+","+duration+","+startTime+","+"M";
        }
        else if(sex.equals("2")){
            output=SId+","+UId+","+Uname+","+VId+","+duration+","+startTime+","+"F";}
        else{
            output=input;
        }

        return output;

    }
}


public class Hot{
    public static void main(String[] args){
        Pipeline p= Pipeline.create();
        PCollection<String> pUserList=p.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/filename.txt"));
        pUserList.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/new_file.txt").withNumShards(1).withSuffix(".csv"));
        p.run();



    }
}
