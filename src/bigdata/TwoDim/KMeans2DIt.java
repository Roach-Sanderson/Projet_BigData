package bigdata.TwoDim;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class KMeans2DIt{
	public static class KMeans2DItMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
		}
	}
	
	public static class KMeans2DItReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
			
		}
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Projet");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(KMeans2DIt.class);
	    job.setMapperClass(KMeans2DItMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(KMeans2DItReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    //FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(null, args));
	}
}
