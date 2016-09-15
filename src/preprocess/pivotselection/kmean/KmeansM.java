package preprocess.pivotselection.kmean;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansM extends Mapper<LongWritable, Text, IntWritable, DataPro> {
	private static Log log = LogFactory.getLog(KmeansM.class);

	private double[][] centers;
	private int dimention_m; // this is the k
	private int dimention_n; // this is the features

	static enum Counter {
		Fansy_Miss_Records
	};

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		URI[] cacheFiles = context.getCacheArchives();
		if (cacheFiles == null || cacheFiles.length < 1) {
			System.out.println("not enough cache files");
			return;
		}
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(new Path(cacheFiles[0]));
		for (int s = 0; s < stats.length; ++s) {
			System.err.println("Cache file path: " + stats[s].getPath().toString());
			FSDataInputStream currentStream;
			BufferedReader currentReader;
			currentStream = fs.open(stats[s].getPath());
			currentReader = new BufferedReader(new InputStreamReader(currentStream));
			
			
			String line;
			List<ArrayList<Double>> temp_centers = new ArrayList<ArrayList<Double>>();
			ArrayList<Double> center = null;
			// get the file data
			while ((line = currentReader.readLine()) != null) {
				center = new ArrayList<Double>();
				String[] str = line.split(",");
				// String[] str=line.split("\\s+");
				for (int i = 0; i < str.length; i++) {
					center.add(Double.parseDouble(str[i]));
					// center.add((double)Float.parseFloat(str[i]));
				}
				temp_centers.add(center);
			}
			try {
				currentReader.close();
				currentStream.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// fill the centers
			@SuppressWarnings("unchecked")
			ArrayList<Double>[] newcenters = temp_centers.toArray(new ArrayList[] {});
			dimention_m = temp_centers.size();
			dimention_n = newcenters[0].size();
			System.err.println("K = " + dimention_m);
			System.err.println("Dimension: "+ dimention_n);
			centers = new double[dimention_m][dimention_n];
			for (int i = 0; i < dimention_m; i++) {
				Double[] temp_double = newcenters[i].toArray(new Double[] {});
				for (int j = 0; j < dimention_n; j++) {
					centers[i][j] = temp_double[j];
					// System.out.print(temp_double[j]+",");
				}
				// System.out.println();
			}
		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		// String[] values=value.toString().split("\\s+");
//		if (values.length != dimention_n + 1) {
//			context.getCounter(Counter.Fansy_Miss_Records).increment(1);
//			return;
//		}
		if(values.length == dimention_n){
			double[] temp_double = new double[values.length];
			for (int i = 0; i < values.length; i++) {
				temp_double[i] = Double.parseDouble(values[i]);
			}
			// set the index
			double distance = Double.MAX_VALUE;
			double temp_distance = 0.0;
			int index = 0;
			for (int i = 0; i < dimention_m; i++) {
				double[] temp_center = centers[i];
				temp_distance = getEnumDistance(temp_double, temp_center);
				if (temp_distance < distance) {
					index = i;
					distance = temp_distance;
				}
			}
			DataPro newvalue = new DataPro();
			
			newvalue.set(new Text(values[0] + "," + values[1]), new IntWritable(1));
			// System.out.println("the map out:"+index+","+value);
			context.write(new IntWritable(index), newvalue);
		}else{
			double[] temp_double = new double[values.length-1];
			for (int i = 1; i < values.length; i++) {
				temp_double[i-1] = Double.parseDouble(values[i]);
			}
			// set the index
			double distance = Double.MAX_VALUE;
			double temp_distance = 0.0;
			int index = 0;
			for (int i = 0; i < dimention_m; i++) {
				double[] temp_center = centers[i];
				temp_distance = getEnumDistance(temp_double, temp_center);
				if (temp_distance < distance) {
					index = i;
					distance = temp_distance;
				}
			}
			DataPro newvalue = new DataPro();
			
			newvalue.set(new Text(values[1] + "," + values[2]), new IntWritable(1));
			// System.out.println("the map out:"+index+","+value);
			context.write(new IntWritable(index), newvalue);
		}
	}

	public static double getEnumDistance(double[] source, double[] other) { // get
																			// the
																			// distance
		double distance = 0.0;
		if (source.length != other.length) {
			return Double.MAX_VALUE;
		}
		for (int i = 0; i < source.length; i++) {
			distance += (source[i] - other[i]) * (source[i] - other[i]);
		}
		distance = Math.sqrt(distance);
		return distance;
	}
}