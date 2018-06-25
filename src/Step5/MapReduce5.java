package Step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapReduce5 {
    private static String inPath = "/user/root/ItemCF/step4_output";
    private static String outPath = "/user/root/ItemCF/step5_output";
    private static String cache = "/user/root/ItemCF/step1_output/part-r-00000";

    public int run(){
        try{
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "ItemCF_Step5");
            job.addCacheFile(new URI(cache + "#itemUserScore3"));

            job.setJarByClass(MapReduce5.class);
            job.setMapperClass(Mapper5.class);
            job.setReducerClass(Reducer5.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);

            Path inputPath = new Path(inPath);
            if(fs.exists(inputPath)){
                FileInputFormat.addInputPath(job, inputPath);
            }

            Path outputPath = new Path(outPath);
            if(fs.exists(outputPath)){
                fs.delete(outputPath);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true) ? 0 : -1;
        }catch (IOException e){
            e.printStackTrace();
        }catch (InterruptedException e) {
            e.printStackTrace();
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args){
        int result = new MapReduce5().run();
        if(result == 0){
            System.out.println("Step5 Successful!");
        }else{
            System.out.println("Step5 Fail!");
        }
    }
}
