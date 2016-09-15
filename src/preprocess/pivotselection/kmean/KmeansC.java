package preprocess.pivotselection.kmean;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansC extends Reducer<IntWritable,DataPro,IntWritable,DataPro> {
	private static int dimension=2;
	
	private static Log log =LogFactory.getLog(KmeansC.class);

	// // the main purpose of the sutup() function is to get the dimension of
	// the original data
	// public void setup(Context context) throws IOException{
	// Configuration conf = context.getConfiguration();
	// URI[] cacheFiles = context.getCacheArchives();
	//
	// if (cacheFiles == null || cacheFiles.length < 1) {
	// System.out.println("not enough cache files");
	// return;
	// }
	// BufferedReader br=new BufferedReader(new
	// FileReader(cacheFiles[0].toString()));
	// String line;
	// while((line=br.readLine())!=null){
	// String[] str=line.split(",");
	// // String[] str=line.split("\\s+");
	// dimension=str.length;
	// break;
	// }
	// try {
	// br.close();
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	//
	//
	public void reduce(IntWritable key,Iterable<DataPro> values,Context context)throws InterruptedException, IOException{	
		double[] sum=new double[dimension];
		int sumCount=0;
		// operation two
		for(DataPro val:values){
			String[] datastr=val.getCenter().toString().split(",");
	//		String[] datastr=val.getCenter().toString().split("\\s+");
			sumCount+=val.getCount().get();
			for(int i=0;i<dimension;i++){
				sum[i]+=Double.parseDouble(datastr[i]);
			}
		}
		//  calculate the new centers
//		double[] newcenter=new double[dimension];
		StringBuffer sb=new StringBuffer();
		for(int i=0;i<dimension;i++){
			sb.append(sum[i]+",");
		}
	//	System.out.println("combine text:"+sb.toString());
	//	System.out.println("combine sumCount:"+sumCount);
		DataPro newvalue=new DataPro();
		newvalue.set(new Text(sb.toString()), new IntWritable(sumCount));
		context.write(key, newvalue);
	}
}
