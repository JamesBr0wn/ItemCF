package Step1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce1 {
    private static String inPathStr = "/user/root/ItemCF/step1_input";
    private static String outPathStr = "/user/root/ItemCF/step1_output";

    public int run(){
        try{
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "ItemCF_Step1");

            job.setJarByClass(MapReduce1.class);
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);

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

        }catch (IOException | InterruptedException | ClassNotFoundException e){
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args){
        int result = new MapReduce1().run();
        if(result == 0){
            System.out.println("Step1 Successful!");
        }else{
            System.out.println("Step1 Fail!");
        }
    }
}
