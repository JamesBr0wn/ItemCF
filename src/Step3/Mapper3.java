package Step3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
        String row = value.toString().split("\t")[0];
        String[] lines = value.toString().split("\t")[1].split(",");

        for(String line: lines){
            String col = line.split("_")[0];
            String val = line.split("_")[1];

            outKey.set(col);
            outValue.set(row + "_" + val);

            context.write(outKey, outValue);
        }

    }
}
