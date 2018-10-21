package cn.tl.demo1.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map1(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strArr = value.toString().split("");
		for (String str : strArr) {
			word.set(str);
			context.write(word, one);
		}
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer strt = new StringTokenizer(value.toString());
		while (strt.hasMoreTokens()) {
			word.set(strt.nextToken());
			context.write(word, one);
		}
		// word.set(value.toString());
		// context.write(word, one);
	}
}
