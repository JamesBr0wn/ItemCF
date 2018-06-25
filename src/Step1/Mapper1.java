package Step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws  IOException, InterruptedException{
        String[] record = value.toString().split(",");
        String userID = record[0];
        String itemID = record[1];
        String itemScore = record[2];

        outKey.set(itemID);
        outValue.set(userID + "_" + itemScore);

        context.write(outKey, outValue);
    }
}
