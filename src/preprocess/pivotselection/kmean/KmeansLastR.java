package preprocess.pivotselection.kmean;

import java.io.IOException;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Reducer;  
  
public class KmeansLastR extends Reducer<IntWritable,Text,IntWritable,Text> {  
      
    public void reduce(IntWritable key,Iterable<Text> values,Context context)throws InterruptedException, IOException{  
  
        //  output the data directly  
        for(Text val:values){  
            context.write(key, val);  
        }  
          
    }  
}  