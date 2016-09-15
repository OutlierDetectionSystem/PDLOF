package templates;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MR_TEMPLATE extends Configured implements Tool {
	
	private String input;
	private String output;
	private int numReducers;
	private boolean local = false;
	private Configuration conf;

	// usage: input, output, numReducers, template. *optional local flag
	public int run(String[] args) throws Exception {

//		process args
		input = args[0];
		output = args[1];	
		if(args[2].equals("local")){
			local = true;
			conf = new Configuration();
		}
		else{
			numReducers = Integer.parseInt(args[2]);
			conf = getConf();
		}
//		delete old output directories if they exist
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
		}
		
//		set up job
		Job job = new Job(conf);
		if(!local){
			job.setNumReduceTasks(numReducers);
		}
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(MYMAPPERCLASS.class);
		job.setReducerClass(MYREDUCERCLASS.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Submit the job, then poll for progress until the job is complete
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {

		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(),
				new MR_TEMPLATE(), args);
		System.exit(res);
	}

	
	public static class MYMAPPERCLASS extends
			Mapper<LongWritable, Text, Text, Text> {
		
		static Text	skey = new Text();	
		static Text	label = new Text();
		
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			//input line: x, y, class
			String line = value.toString();
			String[] values  = line.split(",");
			skey.set(key.toString());
			label.set(values[0]);
			context.write(skey, label);
		}
	}
	
	public static class MYREDUCERCLASS extends
	Reducer<Text, Text, Text, Text> {

		static Text newFeature = new Text();
		
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			for(Text v : values){
				newFeature.set(v.toString());
				context.write(key, newFeature);
			}
		}
	}

}

