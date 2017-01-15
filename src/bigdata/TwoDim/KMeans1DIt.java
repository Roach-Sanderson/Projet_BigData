package bigdata.TwoDim;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class KMeans1DIt extends Configured implements Tool{
	

	public static class KMeans1DItMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		private double[] totalPosPerCluster = null; // Array which contains the sum of the positions of each element in one cluster.
		private int[] totalElemPerCluster = null; // Array which contains the number of elements for each cluster.
		public int column = 0; // Array which contains the number of the column which contains coordinates of each point.
		private int nbClusters = 0; // Number of clusters, specified in args.
		private double[] keys; // Array which contains the coordinates of each pivot.
		private Set<String[]> clusters = null; // Set which contains all lines of the input file with its associated pivot, position and key.
		public Path input; // The input in args.
		private URI[] uris; // Array which contains the URI of all the cache files.
		@SuppressWarnings("unused")
		private File file; // The input in args contained in a cache file.
		
		private boolean check() /* Checks if we have to stop the algorithm */
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
		
		private boolean notIn(Double d) /* Check for key duplication */
		{
			for (int i = 0 ; i < keys.length ; i++)
			{
				if (keys[i] == d)
				{
					return false;
				}
			}
			return true;
		}
		
		public void setup (Context context) throws IOException
		{
			try /* args parsing */
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
			for (int i = 0 ; i < nbClusters ; i++) /* array initialization */
			{
				totalPosPerCluster[i] = 0;
				keys[i] = 0;
				totalElemPerCluster[i] = 0;
			}
			clusters = new HashSet<String[]>();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0)
				uris = context.getCacheFiles();
			file = new File("tmp_results"); /* get the cache file */
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open((new Path((uris[0]).getPath()))))); /*reader of the cache file */
			Stream<String>lines = reader.lines();
			int nbLignes = 0;
			int mauLignes = 0;
			Iterator<String> it = lines.iterator();
			while(it.hasNext() && nbLignes < nbClusters){ /* Fill up the key array w/o side effects */
				String tokens[] = it.next().split(",");
				boolean isValid = true;
				for (int l = 0 ; l < tokens.length ; l++)
				{
					if (tokens[l].isEmpty())
						isValid = false;
				}
				Double pos = 0.0;
				try {
					pos = Double.parseDouble(tokens[column]);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				if (isValid && notIn(pos))
				{
					keys[nbLignes - mauLignes] = pos;
					nbLignes++;
				}
			}
		}

		public void map(LongWritable key, Text value, Context context) {
			String tokens[] = value.toString().split(",");
			for (int i = 0 ; i < tokens.length ; i++) // Check if the line is valid (not empty column(s))
			{
				if (tokens[i].isEmpty()) 
					return;
			}
			Double position;
			try  // Tries to parse the position given by column and keeps it.
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
			for (int i = 1 ; i < nbClusters ; i++) /* Decides which point belongs to which cluster */
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
			elem[0] = new IntWritable(newkey).toString(); // Number of the cluster in which the line belongs
			elem[1] = new DoubleWritable(position).toString(); // Position of the point
			elem[2] = value.toString();	// Whole line
			elem[3] = key.toString(); 	// Position in file
			clusters.add(elem);
		}

		public void cleanup(Context context) throws IOException, InterruptedException
		{
			while(!check()) /* While we haven't find a good convergence value */
			{
				for (int i = 0 ; i < nbClusters ; i++) /* Calculate the average of keys position */
				{
					keys[i] = totalPosPerCluster[i] / totalElemPerCluster[i];
					totalPosPerCluster[i] = 0;
					totalElemPerCluster[i] = 0;
				}
				for(String[] elem : clusters) /* Recalculate the values with the new clusters for each point. Same way as map does.*/
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
				context.write(new IntWritable(Integer.parseInt(elem[3])), new Text (elem[2] // The whole line of the input file
						+ ", " 
						+ elem[0])); // The cluster in which the line belongs 
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
		job.addCacheFile(new Path(args[0]).toUri());
	  try {		// Parse all arguments
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