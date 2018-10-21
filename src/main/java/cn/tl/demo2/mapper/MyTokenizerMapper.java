package cn.tl.demo2.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	private IntWritable one = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] s = value.toString().split(",");
		for (String string : s) {
			word.set(string);
			context.write(word, one);
		}
	}
}
