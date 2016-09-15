package templates;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRTemplate {
	
	private static String input;
	private static String output;
	
	  public static void main(String[] args) throws Exception {
		  try{
			  input = args[0];
			  output = args[1];
		  }
		  catch(Exception e){
			  e.printStackTrace();
			  System.exit(0);
		  }
		  
		    Configuration conf = new Configuration();
		    Job job = new Job(conf, "word count");
		    job.setJarByClass(MRTemplate.class);
		    job.setMapperClass(MRMapper.class);
		    job.setReducerClass(MRReducer.class);
//		    job.setCombinerClass(MRReducer.class);
//		    job.setPartitionerClass(MRPartioner.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(input));
		    FileOutputFormat.setOutputPath(job, new Path(output));

		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

  public static class MRMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class MRReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


}
