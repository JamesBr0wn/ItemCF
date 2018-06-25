package Step6;

import java.io.IOException;
import java.util.*;

import javafx.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper6 extends Mapper<LongWritable, Text, Text, Text>{
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws  IOException, InterruptedException{
        String userID = value.toString().split("\t")[0];
        String[] item_score_array = value.toString().split("\t")[1].split(",");

        List<Pair<Integer, Double>> store = new ArrayList<>();
        for(String item_score: item_score_array){
            int itemID = Integer.valueOf(item_score.split("_")[0]);
            double score = Double.valueOf(item_score.split("_")[1]);
            store.add(new Pair<>(itemID, score));
        }

        Collections.sort(store, new Comparator<Pair<Integer, Double>>() {
            public int compare(Pair<Integer, Double> a,Pair<Integer, Double> b)
            {
                if(a.getValue() > b.getValue()){
                    return -1;
                }else if(a.getValue() < b.getValue()){
                    return 1;
                }else{
                    return 0;
                }
            }
        });

        StringBuilder strBuilder = new StringBuilder();
        for(int i = 0; i < 5 && i < store.size(); i++){
            strBuilder.append(Integer.toString(store.get(i).getKey()) + "_" + Double.toString(store.get(i).getValue()) + ",");
        }

        if(strBuilder.toString().endsWith(",")){
            String line = strBuilder.substring(0, strBuilder.length() - 1);

            outKey.set(userID);
            outValue.set(line);

            context.write(outKey, outValue);
        }
    }

}