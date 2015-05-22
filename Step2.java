
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2 {

	/**
	 * @param args
	 */
	public static class Step2Mapper extends Mapper<LongWritable, Text, Text, Text>{
		Text newKey = new Text();
		Text newValue = new Text();
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			//Text newValue = new Text("");
			String splits[] = value.toString().split("\t");
			newKey.set(splits[0]);
			String nodes[] = splits[1].split(",");
			newValue.set("start"+splits[1]);
			context.write(newKey, newValue);
			//if (nodes.length > 1){
			
				for (int i=0; i < (nodes.length - 1); i++){
					for (int j = i + 1; j < nodes.length; j++){
						String a = nodes[i];
						String b = nodes[j];
						String strKey = "",strValue = "";
						// a>b
						if (a.length() > b.length() || (a.length() == b.length() && a.compareTo(b) > 0)){
							strKey= b + "";
							strValue = a + "";
							newKey.set(strKey);
							newValue.set(strValue);
							context.write(newKey, newValue);
						}
						else if (a.length() < b.length() || (a.length() == b.length() && a.compareTo(b) < 0)){
							strKey = a + "";
							strValue = b + "";
							newKey.set(strKey);
							newValue.set(strValue);
							context.write(newKey, newValue);
						}
						
					}
				}
			//}
			
			
		}
	}
	
	public static class Step2Reducer extends Reducer<Text, Text, IntWritable, Text>{
		static IntWritable number = new IntWritable();
		static int n = 0;
		static Text value = new Text("");
//protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
			ArrayList<String> findingNodes = new ArrayList<String>();
			HashMap<String, Boolean> linkNodes = new HashMap<String, Boolean>();
			for (Text val : values){
				String str = val.toString();
				if (str.startsWith("start")){
					String nodes[] = str.substring(5).split(",");
					for (int i = 0; i < nodes.length; i++){
						linkNodes.put(nodes[i], true);
					}
				}
				else{
					findingNodes.add(str);
				}
			}
			//finding the findingNodes in linkNodes
			for (int i = 0; i < findingNodes.size(); i++){
				String strNode = findingNodes.get(i);
				if (linkNodes.containsKey(strNode)){
					n++;
				}
			}
			number.set(n);
		}
		protected void cleanup(Reducer<Text, Text, IntWritable, Text>.Context context)throws IOException, InterruptedException{
			//number.set(n);
			context.write(number, value);
		}
		
	}

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job2= new Job(conf, "job2");
		job2.setNumReduceTasks(16);
		job2.setJarByClass(Step2.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(Step2Mapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(Step2Reducer.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.waitForCompletion(true);
		
	}

}
