package Equijoin.Equijoin;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class equijoin {
    
    public static class equijoinMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text values = new Text();
        private Text keys = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            
            String line = value.toString();
			String[] str = line.split(",");
            String keyjoin = str[1];   
            keys.set(keyjoin);
            values.set(line);
            context.write(keys,values);   
        } 
    }
    
    public static class equijoinReducer extends Reducer<Text, Text, Text, Text>
    {  
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
        {
            
            List<String> tablename1 = new ArrayList<String>();
            List<String> tablename2 = new ArrayList<String>();
            String firstTableName = "";
            Text output = new Text();
            String output_string = new String();
			boolean flag = true;
            
            for (Text each : values)
            {
                String value = each.toString();
                String[] valueSplit = value.split(",");
                if (flag == true) 
                {
                    firstTableName = valueSplit[0];
					flag = false;
                }
                if (firstTableName.equals(valueSplit[0]) ) {
                    tablename1.add(value);
                }
                else 
                {
                    tablename2.add(value);
                }
            }
        Text ref = new Text("");
	    if ( tablename1.size() == 0 || tablename2.size() == 0)
	    {
	    	key.clear();
	    }
	    else
            {
            	for (int i =0; i<tablename1.size(); i++) 
            	{
                	for (int j=0; j<tablename2.size(); j++) 
                	{
                			
                    		output_string = tablename1.get(i) + ", " + tablename2.get(j);
                    		output.set(output_string);
                    		context.write(ref,output);              
                	}  
                
            	}  
            }
        }
    }
    

    public static void main(String[] args) throws Exception
    {
         Configuration conf = new Configuration();
         Job job = Job.getInstance(conf, "equijoin");
         job.setJarByClass(equijoin.class);
         job.setMapperClass(equijoinMapper.class);
         job.setReducerClass(equijoinReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}