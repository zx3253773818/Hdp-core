package cn.tl.demo__5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author zhangxin 5、给定如下图的学生成绩表，表结构为（学号，姓名，课程名称，成绩），其 hdfs 路径为
 *         /tmp/table/student_score.txt，请使用 MR
 *         框架支持二次排序方法，实现按学号第一顺序，相同学号情况下按成绩倒序排列
 *         ，输出到个人用户的家目录下，结果字段为（学号，姓名，课程名称，成绩），按 tab 分隔即可。
 */
public class MRSrotStudent {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] remainingArgs = gop.getRemainingArgs();
		if (remainingArgs.length != 2) {
			System.err
					.println("Usage: yarn jar jar_path main_class_path -D 参数列表 <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "counts");
		job.setJarByClass(MRSrotStudent.class);
		job.setMapperClass(MyMapper.class);
//		job.setCombinerClass(MyReducer.class);// reducer输入输出一致时可写
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(2); // 指定reduce数量
		job.setPartitionerClass(MyPartitoner.class);// 指定分区规则
		job.setGroupingComparatorClass(MyGroupSortComparator.class);// 指定分组规则
		job.setMapOutputKeyClass(NewStudenInfo.class);// mapper和reducer输出不一致时要写
		job.setMapOutputValueClass(Text.class);// mapper和reducer输出不一致时要写
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

class MyMapper extends Mapper<Object, Text, NewStudenInfo, Text> {
	private NewStudenInfo nk = null;
	private Text nv = new Text();

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strArr = value.toString().split("\\t");
		if (strArr.length == 4) {
			nk = new NewStudenInfo(strArr[0], Integer.parseInt(strArr[3]));
			nv.set(strArr[1] + "\t" + strArr[2] + "\t" + strArr[3]);
			context.write(nk, nv);
		} else {
			try {
				throw new Exception("数据格式不规则");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

class MyReducer extends Reducer<NewStudenInfo, Text, Text, Text> {
	private Text nv = new Text();

	@Override
	protected void reduce(NewStudenInfo arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
		for (Text st : arg1) {
			nv.set(arg0.getId());
			arg2.write(nv, st);
			// break; //简单去重
		}

	}
}

class NewStudenInfo implements WritableComparable<NewStudenInfo> {
	private String id;
	private Integer score;

	public NewStudenInfo(String id, Integer score) {
		super();
		this.id = id;
		this.score = score;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getScore() {
		return score;
	}

	public void setScore(Integer score) {
		this.score = score;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeInt(score);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readUTF();
		score = in.readInt();
	}

	@Override
	public int compareTo(NewStudenInfo o) {
		int val = this.getId().compareTo(o.getId());
		if (val == 0) {
			val = this.getScore().compareTo(o.getScore());
		}
		return val;
	}

}

class MyPartitoner extends Partitioner<NewStudenInfo, Text> {

	@Override
	public int getPartition(NewStudenInfo arg0, Text arg1, int arg2) {

		return (arg0.getId().hashCode() & Integer.MAX_VALUE) % arg2;
	}

}

class MyGroupSortComparator extends WritableComparator {

	public MyGroupSortComparator() {
		super(NewStudenInfo.class, true);
	}

	@Override
	public int compare(Object a, Object b) {
		NewStudenInfo a1 = (NewStudenInfo) a;
		NewStudenInfo b1 = (NewStudenInfo) b;
		return b1.getId().compareTo(a1.getId());
	}
}