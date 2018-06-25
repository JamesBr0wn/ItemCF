package Step1;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> actions, Context context)
            throws IOException, InterruptedException{
        String itemID = key.toString();

        Map<String, Double> map = new HashMap<>();
        for(Text action: actions){
            String userID = action.toString().split("_")[0];
            String itemScore = action.toString().split("_")[1];

            if(map.get(userID) == null){
                map.put(userID, Double.valueOf(itemScore));
            }else{
                map.put(userID, map.get(userID) + Double.valueOf(itemScore));
            }
        }

        StringBuilder strBuilder = new StringBuilder();
        for(Map.Entry<String, Double> entry: map.entrySet()){
            String  userID = entry.getKey();
            String itemScore = String.valueOf(entry.getValue());
            strBuilder.append(userID + "_" + itemScore + ",");
        }

        if(strBuilder.toString().endsWith(",")){
            String line = strBuilder.substring(0, strBuilder.length() - 1);

            outKey.set(itemID);
            outValue.set(line);

            context.write(outKey, outValue);
        }
    }
}
