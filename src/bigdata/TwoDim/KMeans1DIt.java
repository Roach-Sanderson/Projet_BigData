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
	
	public static class KMeans1DItMapper extends Mapper<NullWritable, Text, NullWritable, Text> {
		
		private double[] totalPosPerCluster = null;
		private int[] totalElemPerCluster = null;
		public int column = 0;
		private int nbClusters = 0;
		private double[] keys;
		private Set<String[]> clusters;
		
		private boolean check()
		{
			for (int i = 0 ; i < nbClusters ; i++)
			{
				double tmp2 = Math.abs((totalPosPerCluster[i] / totalElemPerCluster[i]) - keys[i]);
				if (tmp2 > 0.1)
				{
					return false;
				}
			}
			return true;
		}
		
		public void setup (Context context)
		{
			nbClusters = Integer.parseInt(context.getConfiguration().get("nbCluster"));
			column = Integer.parseInt(context.getConfiguration().get("numColonne"));
			totalPosPerCluster = new double[nbClusters];
			totalElemPerCluster = new int[nbClusters];
			keys = new double[nbClusters];
			for (int i = 0 ; i < nbClusters ; i++)
			{
				totalPosPerCluster[i] = 0;
				keys[i] = 0;
				totalElemPerCluster[i] = 0;
			}			
			
		}
		public void map(LongWritable key, Text value, Context context) throws Exception {
			if (key.get() == 0 ) return;
			String tokens[] = value.toString().split(",");
			if (tokens[column].isEmpty()) return;
			Double position = Double.parseDouble(tokens[column]);
			IntWritable current = new IntWritable((int) context.getCounter("Progress", "current").getValue());
			if(current.get() < nbClusters){
				keys[current.get()] = position;
				context.getCounter("Progress", "current").increment(1);
			}
			Double minValue = keys[0];
			int newkey = 0;
			for (int i = 1 ; i < keys.length ; i++)
			{
				Double tmp = Math.abs(Math.abs(position) - Math.abs(keys[i]));
				if (tmp < minValue)		
					newkey = i;
			}
			totalPosPerCluster[newkey] += position;
			totalElemPerCluster[newkey]++;
			String[] elem = new String[2];
			elem[0] = new IntWritable(newkey).toString();
			elem[1] = new DoubleWritable(position).toString();
			clusters.add(elem);
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			while(!check())
			{
				for (int i = 0 ; i < nbClusters ; i++)
				{
					keys[i] = totalPosPerCluster[i] / totalElemPerCluster[i];
				}
				for (String[] elem : clusters)
				{
					double dist = Math.abs(Math.abs(Double.parseDouble(elem[2])) - Math.abs(keys[0]));
					elem[0] = new IntWritable(0).toString();
					elem[1] = new DoubleWritable(dist).toString();
					for (int i = 1 ; i < nbClusters ; i++)
					{
						double tmp = Math.abs(Math.abs(Double.parseDouble(elem[2])) - Math.abs(keys[i]));
						if (tmp < dist)
						{
							elem[1] = new IntWritable(i).toString();
						}
					}
				}
			}
			for (String[] elem : clusters)
			{
				context.write(NullWritable.get(), new Text (elem[1] + ", " + elem[0]));
			}
		}
	}

	public static class KMeans1DItReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	
		
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text elem : values)
			{
				context.write(NullWritable.get(), elem);
			}
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
	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(KMeans1DItReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	   
	    //FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(null, args));
	}
}
