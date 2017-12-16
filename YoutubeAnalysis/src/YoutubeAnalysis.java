import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YoutubeAnalysis 
{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration con = new Configuration();
		Job j = new Job(con,"Youtube data analysis");
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		j.setMapperClass(MyMapper.class);
		j.setNumReduceTasks(0);
		j.setJarByClass(YoutubeAnalysis.class);
		j.setJobName("Changing delimiter of youtube data file");
		j.setOutputKeyClass(NullWritable.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j,input);
		FileOutputFormat.setOutputPath(j,output);
		System.exit(j.waitForCompletion(true)?1:0);
	}
	
	public static class MyMapper extends Mapper<LongWritable,Text,NullWritable,Text>
	{
		public void map(LongWritable offset,Text line,Context con) throws IOException, InterruptedException
		{
			String record,related_ids="";
			String[] previous,arr;
			arr = line.toString().split("\t");
			previous = Arrays.copyOfRange(arr,0,9);
			StringBuilder sb = new StringBuilder();
			for(String word:previous)
			{
				sb.append(word);
				sb.append("\t");
			}
			record=sb.toString();
			for(int i=9;i<arr.length;i++)
			{
				if(i==9)
				{
					related_ids=related_ids+arr[i].toString();
				}
				else
				{
					related_ids=related_ids+"~"+arr[i].toString();
				}
			}
			record=record+related_ids;
			con.write(NullWritable.get(),new Text(record));
		}
	}
	

}
