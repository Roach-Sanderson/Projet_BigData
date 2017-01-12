package bigdata.TwoDim;


import java.awt.geom.Point2D;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class KMeans1DIt extends Configured implements Tool{
	

	public static class KMeans1DItMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		private double[] totalPosPerCluster = null;
		private int[] totalElemPerCluster = null;
		public int column = 0;
		private int nbClusters = 0;
		private double[] keys;
		private Set<String[]> clusters = null;
		public Path input;
		
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
		
		public void setup (Context context) throws IOException
		{
			try
			{
				nbClusters = Integer.parseInt(context.getConfiguration().get("nbCluster"));
				column = Integer.parseInt(context.getConfiguration().get("numColonne"));
				input = new Path(context.getConfiguration().get("path"));
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return;
			}
			totalPosPerCluster = new double[nbClusters];
			totalElemPerCluster = new int[nbClusters];
			keys = new double[nbClusters];
			for (int i = 0 ; i < nbClusters ; i++)
			{
				totalPosPerCluster[i] = 0;
				keys[i] = 0;
				totalElemPerCluster[i] = 0;
			}
			clusters = new HashSet<String[]>();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			InputStreamReader isr = new InputStreamReader(fs.open(input));
			BufferedReader br = new BufferedReader(isr);
			String line;
			line = br.readLine();
			int nbLignes = 0;
			while(line != null && nbLignes < nbClusters){
				String tokens[] = line.split(",");
				boolean isValid = true;
				for (int l = 0 ; l < tokens.length ; l++)
				{
					if (tokens[l].isEmpty())
						isValid = false;
				}
				if (isValid)
				{
					try {
						keys[nbLignes] = Double.parseDouble(tokens[column]);
					}
					catch (Exception e)
					{
						e.printStackTrace();
						return;
					}
					nbLignes++ ;
				}
				line = br.readLine();
				
			}
		}

		public void map(LongWritable key, Text value, Context context) {
			String tokens[] = value.toString().split(",");
			for (int i = 0 ; i < tokens.length ; i++)
			{
				if (tokens[i].isEmpty()) 
					return;
			}
			Double position;
			try
			{
				position = Double.parseDouble(tokens[column]);
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return;
			}
			String elem[] = new String[4];
			int newkey = 0;
			double dist = Math.abs(position - keys[0]);
			for (int i = 1 ; i < nbClusters ; i++)
			{
				double tmp = Math.abs(position - keys[i]);
				if (tmp < dist)
				{
					
					newkey = i;
					dist = tmp;
				}
			}
			totalPosPerCluster[newkey] += position;
			totalElemPerCluster[newkey] += 1;
			elem[0] = new IntWritable(newkey).toString();
			elem[1] = new DoubleWritable(position).toString();
			elem[2] = value.toString();
			elem[3] = key.toString(); 		// Pour garde rle fichier trié
			clusters.add(elem);
		}

		public void cleanup(Context context) throws IOException, InterruptedException
		{
			while(!check())
			{
				for (int i = 0 ; i < nbClusters ; i++)
				{
					keys[i] = totalPosPerCluster[i] / totalElemPerCluster[i];
					totalPosPerCluster[i] = 0;
					totalElemPerCluster[i] = 0;
				}
				for(String[] elem : clusters)
				{
					double dist = Math.abs(Double.parseDouble(elem[1]) - keys[0]);
					elem[0] = new IntWritable(0).toString();
					for (int i = 1 ; i < nbClusters ; i++)
					{
						double tmp = Math.abs(Double.parseDouble(elem[1]) - keys[i]);
						if (tmp < dist)
						{
							elem[0] = new IntWritable(i).toString();
							dist = tmp;
						}
					}
					totalElemPerCluster[Integer.parseInt(elem[0])] += 1;
					totalPosPerCluster[Integer.parseInt(elem[0])] += Double.parseDouble(elem[1]);
				}
			}
			for (String[] elem : clusters)
			{
				context.write(new IntWritable(Integer.parseInt(elem[3])), new Text (elem[2] + ", " + elem[0]));
			}

		}
	}
	
	public static class KMeans1DItReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

	
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
		    job.getConfiguration().set("path", args[0]);
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    	job.getConfiguration().set("nbCluster", args[2]);
	    	job.getConfiguration().set("numColonne", args[3]);
	    }
	    catch (Exception e)
	    {
	    	System.out.println(" bad arguments, waiting for 3 arguments [inputURI] [Integer] [Integer]");
	    }
	    job.setNumReduceTasks(1);
	    job.setJarByClass(KMeans1DIt.class);
	    job.setMapperClass(KMeans1DItMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(KMeans1DItReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new KMeans1DIt(), args));
	}

}
