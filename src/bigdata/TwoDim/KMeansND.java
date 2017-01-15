package bigdata.TwoDim;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
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


public class KMeansND extends Configured implements Tool{
	

	public static class KMeansNDMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		private double[] totalPosPerCluster = null;
		private int[] totalElemPerCluster = null;
		public String[] column = null;
		private int nbClusters = 0;
		private double[] keys;
		private BufferedWriter bw = null;
		private BufferedReader br = null;
		private File cached_input;
		private URI[] cached_uris;
		
		private boolean check()
		{
			for (int i = 0 ; i < nbClusters ; i++)
			{
				double convergence;
				if(totalElemPerCluster[i] == 0)
					convergence = 0.0;
				else
					convergence = Math.abs((totalPosPerCluster[i] / totalElemPerCluster[i]) - keys[i]);
				if (convergence > 0.1)
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
			Configuration conf = context.getConfiguration();
			try
			{
				nbClusters = Integer.parseInt(conf.get("nbCluster"));
				column = conf.get("numColonne").split(",");
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
				keys[i] = 0.0;
				totalElemPerCluster[i] = 0;
			}
			FileSystem fs = FileSystem.get(conf);
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0)
				cached_uris = context.getCacheFiles();
			cached_input = new File("tmp_results");
			if (!(cached_input.exists()))
				cached_input.createNewFile();
			OutputStream out = fs.create(new Path(cached_input.getPath()), true);
			BufferedReader cached_reader = new BufferedReader(
					new InputStreamReader(fs.open(
							(new Path((cached_uris[0]).getPath()))
							)));			
			bw = new BufferedWriter(new OutputStreamWriter(out));
			Stream<String> lines = cached_reader.lines();
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
				Double position = 0.0;
				Double currentColumn = 0.0;
				
					for (int l = 0 ; l < column.length ; l++)
					{
						try {
						position *= position;
						currentColumn = Double.parseDouble(tokens[Integer.parseInt(column[l])]);
						position += (currentColumn * currentColumn);
						position = Math.sqrt(Math.abs(position));
						}
						catch (Exception e)
						{
							e.printStackTrace();
						}
					}
				if (isValid && notIn(position))
				{			
					keys[nbLignes] = position;
					nbLignes++;
				}
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException {
			String tokens[] = value.toString().split(",");
			Double position = 0.0;
			try
			{
				for (int i = 0 ; i < column.length ; i++)
					position += (Double.parseDouble(tokens[Integer.parseInt(column[i])]) * Double.parseDouble(tokens[Integer.parseInt(column[i])]));
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return;
			}
			position = Math.sqrt(Math.abs(position));
			int newKey = 0;
			double distance = Math.abs(position - keys[0]);
			for (int i = 1 ; i < nbClusters ; i++)
			{
				double currentDistance = Math.abs(position - keys[i]);
				if (currentDistance < distance)
				{
					
					newKey = i;
					distance = currentDistance;
				}
			}
			totalPosPerCluster[newKey] += position;
			totalElemPerCluster[newKey] += 1;
			bw.write(key.toString() + "," + value + "," + new DoubleWritable(position).toString() + "," + new IntWritable(newKey).toString());
			bw.newLine();
		}

		public void cleanup(Context context) throws IOException, InterruptedException
		{
			FileSystem fs = FileSystem.get(context.getConfiguration());
			int cpt = 0;
			while(!(check()))
			{
				cpt++;
				File currentResults = new File("tmp_results"+new IntWritable(cpt).toString());
				if (!(currentResults.exists()))
					currentResults.createNewFile();
				OutputStream out = fs.create(new Path(currentResults.getPath()), true);
				bw = new BufferedWriter(new OutputStreamWriter(out));
				br = new BufferedReader(new InputStreamReader(fs.open((new Path((cached_uris[0]).getPath())))));;
				for (int i = 0 ; i < nbClusters ; i++)
				{
					if (totalElemPerCluster[i] == 0)
						keys[i] = totalPosPerCluster[i];
					else
						keys[i] = totalPosPerCluster[i] / totalElemPerCluster[i];
					totalPosPerCluster[i] = 0;
					totalElemPerCluster[i] = 0;
				}
				String line = br.readLine();
				while(line != null)
				{
					String[] tokens = line.split(",");
					try
					{
						double distance = Math.abs(Double.parseDouble(tokens[tokens.length - 2]) - keys[0]);					
						String newCluster = new IntWritable(0).toString();
						for (int i = 1 ; i < nbClusters ; i++)
						{
							double currentDistance = Math.abs(Double.parseDouble(tokens[tokens.length - 2]) - keys[i]);
							if (currentDistance < distance)
							{
								newCluster = new IntWritable(i).toString();
								distance = currentDistance;
							}
						}
						totalElemPerCluster[Integer.parseInt(newCluster)] += 1;
						totalPosPerCluster[Integer.parseInt(newCluster)] += distance;
						StringBuffer sb = new StringBuffer("");
						for (int c = 0 ; c < tokens.length - 1 ; c++)
						{
							if (c != 0)
							{
								sb.append(",");
							}
							sb.append(tokens[c]);
						}
						sb.append(","+newCluster);
						bw.write(sb.toString());
					}
					catch (Exception e){
						e.printStackTrace();
						bw.write(tokens.toString());
					}
					bw.newLine();
					line = br.readLine();
				}
			}
			InputStreamReader isr = new InputStreamReader(fs.open(new Path("tmp_results"+cpt)));
			
			br = new BufferedReader(isr);
			String line = br.readLine();
			while(line != null)
			{
				String[] tokens = line.split(",");
				StringBuffer value = new StringBuffer("");
				for (int i = 1 ; i < tokens.length - 2 ; i++)
				{
					if (i != 1)
					{
						value.append(",");
					}
					value.append(tokens[i]);
				}
				try
				{
					context.write(new IntWritable(Integer.parseInt(tokens[0])), new Text (value.toString() + ", " + Integer.parseInt(tokens[tokens.length - 1])));				
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				line = br.readLine();
			}
			if(cached_input.exists())
				fs.delete(new Path(cached_input.getPath()), true);
			for(;cpt > 0; cpt--){
				fs.delete(new Path("tmp_results"+cpt), true);
			}
		}
	}
	
	public static class KMeansNDItReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

	
		
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
	    try {
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    	job.getConfiguration().set("nbCluster", args[2]);
	    	job.addCacheFile(new Path(args[0]).toUri());
	    	StringBuffer s = new StringBuffer();
	    	for (int i = 3 ; i < args.length ; i++)
	    	{
	    		if (i != 3)
	    		{
	    			s.append(",");
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
	    job.setJarByClass(KMeansND.class);
	    job.setMapperClass(KMeansNDMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(KMeansNDItReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new KMeansND(), args));
	}
}