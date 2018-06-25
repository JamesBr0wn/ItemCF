package Step2;

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

public class MapReduce2 {
    private static String inPathStr = "/user/root/ItemCF/step1_output";
    private static String outPathStr = "/user/root/ItemCF/step2_output";
    private static String cacheStr = "/user/root/ItemCF/step1_output/part-r-00000";

    public int run(){
        try{
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "ItemCF_Step2");
            job.addCacheFile(new URI(cacheStr + "#itemUserScore1"));

            job.setJarByClass(MapReduce2.class);
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer2.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);

            Path inputPath = new Path(inPathStr);
            if(fs.exists(inputPath)){
                FileInputFormat.addInputPath(job, inputPath);
            }

            Path outputPath = new Path(outPathStr);
            if(fs.exists(outputPath)){
                fs.delete(outputPath);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true) ? 0 : -1;
        }catch (IOException | InterruptedException | ClassNotFoundException | URISyntaxException e){
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args){
        int result = new MapReduce2().run();
        if(result == 0){
            System.out.println("Step2 Successful!");
        }else{
            System.out.println("Step2 Fail!");
        }
    }
}
