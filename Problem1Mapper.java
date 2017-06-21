/**
 * <h1>Problem1Mapper</h1>
 * Mapper program to find the number of ratings given by each user.
 * This class will take input as (Key,Value) pair from a given file and will
 * produce the output as (Key,Value) pair.
 * */
package mapreduce.assignment4.problem1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Problem1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text outkey = new Text();
	IntWritable outvalue = new IntWritable(); 
	public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
		//every line will be split based on tab and will be stored in String array
		String line[] = value.toString().split("\\t"); 
		//will set mapper output key as userid
		//and mapper output value as 1
		if(line.length>0){ 
			outkey.set(line[0]); 
			outvalue.set(1);
		} 
		
		context.write(outkey, outvalue); 
	} 
}
