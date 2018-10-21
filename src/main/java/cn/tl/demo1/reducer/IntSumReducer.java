package cn.tl.demo1.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class IntSumReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		System.out.println("key is :" + key + ",values size:"
				+ Lists.newArrayList(values).size());
		context.write(key, result);
	}
}
