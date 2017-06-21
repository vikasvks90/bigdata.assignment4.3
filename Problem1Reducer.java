/**
 * <h1>Problem1Reducer</h1>
 * Reducer program to find the number of ratings given by each user.
 * This class will take input as (Key,Value) pair from output of mapper class
 * value will be a combined list for all the values for a given key
 * */
package mapreduce.assignment4.problem1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Problem1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException { 
		//will add mapper output value to get the number of ratings given by each user
		int Sum = 0; 
		for(IntWritable val: value) { 
			Sum += val.get(); 
		} 
		
		context.write(key, new IntWritable(Sum)); 
	} 
}
