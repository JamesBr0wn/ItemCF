package Step4;

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

public class MapReduce4 {
    private static String inPathStr = "/user/root/ItemCF/step2_output";
    private static String outPathStr = "/user/root/ItemCF/step4_output";
    private static String cacheStr = "/user/root/ItemCF/step3_output/part-r-00000";

    public int run(){
        try{
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "ItemCF_Step4");
            job.addCacheFile(new URI(cacheStr + "#itemUserScore2"));

            job.setJarByClass(MapReduce4.class);
            job.setMapperClass(Mapper4.class);
            job.setReducerClass(Reducer4.class);

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
        int result = new MapReduce4().run();
        if(result == 0){
            System.out.println("Step4 Successful!");
        }else{
            System.out.println("Step4 Fail!");
        }
    }
}
