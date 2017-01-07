package bigdata.TwoDim;


import java.awt.geom.Point2D;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class KMeans1DIt{
	
	public static class KMeans1DItMapper extends Mapper<NullWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws Exception {
			if (key.get() == 0 ) return;
			String tokens[] = value.toString().split(",");
			if (tokens[4].length()==0) return;
			Configuration conf = context.getConfiguration();
			int colonne = Integer.parseInt(conf.get("numColonne"));
			int nbCluster = Integer.parseInt(conf.get("nbCluster"));
			Double[] keys = new Double[nbCluster];
			String position = tokens[colonne];
			IntWritable current = new IntWritable((int) context.getCounter("Progress", "current").getValue());
			if(current.get() < nbCluster){
				context.getCounter(current.toString(), tokens[colonne]).increment(Long.parseLong(position));
			}
			context.getCounter("Progress", "current").increment(1);
			Double minValue = keys[0];
			int newkey = 0;
			for (int i = 1 ; i < keys.length ; i++)
			{
				Double tmp = Math.abs(Math.abs(Double.parseDouble(position)) - Math.abs(keys[i]));
				if (tmp < minValue)		newkey = i;
			}
			IntWritable nkey = new IntWritable(newkey);
			context.getCounter(nkey.toString(), "totalpos").increment(Long.parseLong(position));
			context.getCounter(nkey.toString(), "totalelem").increment(1);
			context.write(nkey, value);
		}
	}
	
	public static class KMeans1DCombiner extends Reducer<DoubleWritable, Text, DoubleWritable, String[]>
	{

		public void combine (DoubleWritable key, Iterable<Text> value, Context context)
		{		
			
		}
	}
	
	public static class KMeans1DItReducer extends Reducer<DoubleWritable, Text, Text, Text> {

		private long[] tot = null;
		private long[] elem = null;
		private DoubleWritable[] keys = null;
		private int clusters = 0;
		
		public void init(Context context)
		{
			clusters = Integer.parseInt(context.getConfiguration().get("nbCluster"));
			keys = new DoubleWritable[clusters];
			context
		}
		
		
		public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			context.
		}
	}
	
	public static List<Double> centroids = null;
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Projet");
	    try {
		    FileInputFormat.addInputPath(job, new Path(args[0]));
	    	job.getConfiguration().set("nbCluster", args[1]);
	    	job.getConfiguration().set("numColonne", args[2]);
	    }
	    catch (Exception e)
	    {
	    	System.out.println(" bad arguments, waiting for 3 arguments [inputURI] [Integer] [Integer]");
	    }
	    job.setNumReduceTasks(1);
	    job.setJarByClass(KMeans1DIt.class);
	    job.setMapperClass(KMeans1DItMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(KMeans1DItReducer.class);
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
