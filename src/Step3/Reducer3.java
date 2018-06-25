package Step3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer3 extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
        StringBuilder strBuilder = new StringBuilder();
        for(Text text: values){
            strBuilder.append(text + ",");
        }

        if(strBuilder.toString().endsWith(",")){
            String line = strBuilder.substring(0, strBuilder.length() - 1);

            outKey.set(key);
            outValue.set(line);

            context.write(outKey, outValue);
        }
    }
}
