package Step4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Mapper4 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    private List<String> cacheList = new ArrayList<String>();
    private DecimalFormat decFormater = new DecimalFormat("0.00");


    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        try{
            FileReader fr = new FileReader("itemUserScore2");
            BufferedReader br = new BufferedReader(fr);

            String line;
            while((line = br.readLine()) != null){
                cacheList.add(line);
            }

            br.close();
            fr.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
        String row_matrix1 = value.toString().split("\t")[0];
        String[] col_value_array_matrix1 = value.toString().split("\t")[1].split(",");

        for(String line: cacheList){
            String row_matrix_2 = line.split("\t")[0];
            String[] col_value_array_matrix2 = line.split("\t")[1].split(",");

            double result = 0;
            for(String col_value_matrix1: col_value_array_matrix1){
                String col_matrix1 = col_value_matrix1.split("_")[0];
                String value_matrix1 = col_value_matrix1.split("_")[1];

                for(String col_value_matrix2: col_value_array_matrix2){
                    if(col_value_matrix2.startsWith(col_matrix1 + "_")){
                        String value_matrix2 = col_value_matrix2.split("_")[1];
                        result += Double.valueOf(value_matrix1) *  Double.valueOf(value_matrix2);
                        break;
                    }
                }
            }

            if(result == 0){
                continue;
            }

            outKey.set(row_matrix1);
            outValue.set(row_matrix_2 + "_" + decFormater.format(result));

            context.write(outKey, outValue);
        }
    }
}