package bigdata.TwoDim;


import java.awt.geom.Point2D;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
	public static class KMeans1DItMapper extends Mapper<NullWritable, Text, Set<Point2D>, DoubleWritable> {
		public void map(LongWritable key, Text value, Context context) throws Exception {
			if (key.get() == 0 ) return;
			String tokens[] = value.toString().split(",");
			if (tokens[4].length()==0) return;
			Configuration conf = context.getConfiguration();
			URI uri = null;
			try {
				uri = new URI("cache");
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			uri = uri.normalize();
			FileSystem fs = FileSystem.get(uri, conf, "hadoop");
			Path outputPath = new Path(uri.getPath());
			if (fs.exists(outputPath)) {
				throw new Exception("output already exists");
			}
			OutputStream os = fs.create(outputPath);
			//InputStream is = fs.create(null);
			for(int i = 0; i < Integer.parseInt(conf.get("nbCluster")); i++){
			}
			
		}
	}
	
	public static class KMeans1DCombiner extends Reducer<Set<Point2D>, DoubleWritable, Set<Point2D>, DoubleWritable>
	{
		public void combine (Set<Point2D> key, DoubleWritable value, Context context)
		{			
		}
	}
	
	public static class KMeans1DItReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
			
		}
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Projet");
	    try {
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    job.getConfiguration().set("input", args[0]);
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
