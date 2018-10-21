package cn.tl.demo9;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author zhangxin 8在 hdfs 目录/tmp/table/student 中存在 student.txt 文件，按 tab 分隔，
 *         字段名为(学号，姓名，课程号，班级名称），hdfs 目录/tmp/table/student_location 中存在
 *         student_location.txt 文件，按 tab 分隔，字段名为（学号，省份，城市，区名）， 在 Map 任务中用
 *         student_location.txt 文件中的学号过滤 student.txt 中的学号字段， 输出 student.txt
 *         中的存在交集的记录， 输出结果结构按 tab 分隔后的四个字段为（学号，姓名，课程号，班级名称）。
 */
public class TwoStudentCounts {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		GenericOptionsParser op = new GenericOptionsParser(conf, args);
		String[] remainArgs = op.getRemainingArgs();
		if (remainArgs.length != 3) {
			System.out
					.println("参数不符，请指明输入输出路径：<in_studentLocation> <in_student> <out>");
		}
		conf.set("locationPath", remainArgs[0]);
		Job job = Job.getInstance(conf);
		job.setJarByClass(TwoStudentCounts.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);// reducer输入输出一致时可写
		job.setReducerClass(MyReducer.class);
		// job.setMapOutputKeyClass(Text.class);//mapper和reducer输出不一致时要写
		// job.setMapOutputValueClass(IntWritable.class);//mapper和reducer输出不一致时要写
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(remainArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(remainArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

class MyMapper extends Mapper<Object, Text, Text, Text> {
	private Text nk = new Text();
	private Text nv = new Text();
	private Set<String> set = new HashSet<String>();

	private void initWhite(Context context, Set<String> set) throws IOException {
		Configuration conf = context.getConfiguration();
		String filePath = conf.get("locationPath");
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream fsis = fs.open(new Path(filePath));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsis));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] strArr = line.split("\\t");
			if (!line.isEmpty() && strArr.length == 4) {
				set.add(strArr[0]);
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		initWhite(context, set);
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strArr = value.toString().split("\\t");
		if (set.contains(strArr[0])) {
			nk.set(strArr[0]);
			nv.set(strArr[1] + "\t" + strArr[2] + "\t" + strArr[3]);
			context.write(nk, nv);
		}
	}
}

class MyReducer extends Reducer<Text, Text, Text, Text> {
	StringBuffer sb = new StringBuffer();

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
		for (Text st : arg1) {
			arg2.write(arg0, st);
		}
	}
}
