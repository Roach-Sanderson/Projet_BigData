package bigdata.TwoDim;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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


public class KMeansHier extends Configured implements Tool{
	
	public enum UpdateCounter {
		  UPDATED
		 }
	
	public static class KMeansHierMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		private double[] totalPosPerCluster = null;
		private int[] totalElemPerCluster = null;
		public String[] column = null;
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
		
		private boolean notIn(Double d)
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
			try
			{
				nbClusters = Integer.parseInt(context.getConfiguration().get("nbCluster"));
				column = context.getConfiguration().get("numColonne").split(",");
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
			Stream<String>lines = br.lines();
			int nbLignes = 0;
			Iterator<String> it = lines.iterator();
			while(it.hasNext() && nbLignes < nbClusters){
				String tokens[] = it.next().split(",");
				boolean isValid = true;
				for (int l = 0 ; l < tokens.length ; l++)
				{
					if (tokens[l].isEmpty())
						isValid = false;
				}
				Double pos = 0.0;
				try {
					for (int l = 0 ; l < column.length ; l++)
					{
						pos += (Double.parseDouble(column[l]) * Double.parseDouble(column[l]));	
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				pos = Math.sqrt(pos);
				if (isValid && notIn(pos))
				{
					keys[nbLignes] = pos;
					nbLignes++;
				}
			}
		}

		public void map(LongWritable key, Text value, Context context) {
			String tokens[] = value.toString().split(",");
			for (int i = 0 ; i < tokens.length ; i++)
			{
				if (tokens[i].isEmpty()) 
					return;
			}
			Double position = 0.0;
			try
			{
				for (int i = 0 ; i < column.length ; i++)
					position += (Double.parseDouble(column[i]) * Double.parseDouble(column[i]));
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return;
			}
			position = Math.sqrt(position);
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
	
	public static class KMeansHierReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

	
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text elem : values)
			{
				context.write(NullWritable.get(), elem);
			}

		}
	}
	
	public static List<Double> centroids = null;
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int recursions = Integer.parseInt(args[3]);
		int result = 0;
		String inputPath = "";
		String outputPath = "";
		for (int rec = 0 ; rec < recursions ; rec++)
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Projet");
			if (rec == 0)
			{
				inputPath = args[0];
			}
			else
			{
				if (rec >= 1)
				{
					FileSystem sf = FileSystem.get(job.getConfiguration());
					sf.delete(new Path(inputPath), true);
				}
				inputPath = outputPath;
			}
			if (rec < (recursions - 1))
			{
				outputPath = args[1] + new IntWritable(rec).toString();
			}
			else
			{
				outputPath = args[1];
			}
			try {
				FileInputFormat.addInputPath(job, new Path(inputPath));
				StringBuffer path = new StringBuffer(outputPath);
				job.getConfiguration().set("path", args[0]);
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				job.getConfiguration().set("nbCluster", args[2]);
				StringBuffer s = new StringBuffer();
				for (int i = 4 ; i < args.length ; i++)
				{
					if (i != 4)
					{
						s.append(", ");
					}
					s.append(args[i]);
				}		    		
				job.getConfiguration().set("numColonne", s.toString());
			}
			catch (Exception e)
			{
				System.out.println(" bad arguments, waiting for 3 arguments [inputURI] [Integer] [Integer]");
			}
			job.setNumReduceTasks(1);
			job.setJarByClass(KMeansHier.class);
			job.setMapperClass(KMeansHierMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(KMeansHierReducer.class);
	    	job.setOutputKeyClass(NullWritable.class);
	    	job.setOutputValueClass(Text.class);
	    	job.setOutputFormatClass(TextOutputFormat.class);
	    	result = job.waitForCompletion(true) ? 0 : 1;
	    	Path p = FileOutputFormat.getOutputPath(job);
		}
		return result;
	}
	
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new KMeansHier(), args));
	}
}