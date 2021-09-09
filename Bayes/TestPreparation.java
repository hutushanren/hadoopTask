package com.Bayes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
public class TestPreparation extends Configured implements Tool {
    public static class TestPreparation_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text key_out = new Text();
        private Text value_out = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            InputSplit inputsplit = context.getInputSplit();
            //获取类名
            String className = ((FileSplit)inputsplit).getPath().getParent().getName();
            //获取文档名
            String fileName = ((FileSplit)inputsplit).getPath().getName();

            key_out.set(className + "\t" +fileName);
            value_out.set(value.toString());
            context.write(key_out, value_out);
        }
    }
    public static class TestPreparation_Reducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private StringBuffer stringBuffer;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            stringBuffer = new StringBuffer();
            for (Text value : values){
                stringBuffer = stringBuffer.append(value.toString() + " ");
            }
            result.set(stringBuffer.toString());
            context.write(key, result);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job3 = Job.getInstance(conf, "TestPreparation");

        FileSystem fileSystem = FileSystem.get(conf);
        Path outputPath3 = new Path(Utils.Test_Preparation);
        if(fileSystem.exists(outputPath3))
            fileSystem.delete(outputPath3, true);
        //设置jar加载路径
        job3.setJarByClass(TestPreparation.class);
        //设置map和reduce类
        job3.setMapperClass(TestPreparation_Mapper.class);
        job3.setCombinerClass(TestPreparation_Reducer.class);
        job3.setReducerClass(TestPreparation_Reducer.class);
        //设置map reduce输出格式
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        //设置输入和输出路径
        FileInputFormat.setInputDirRecursive(job3,true);
        FileInputFormat.addInputPath(job3, new Path(Utils.TEST_DATA_PATH));
//        FileInputFormat.setInputPaths(job2, new Path(Utils.TRAIN_DATA_PATH));
        FileOutputFormat.setOutputPath(job3, new Path(Utils.Test_Preparation));
        boolean result = job3.waitForCompletion(true);
        return (result ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TestPreparation(), args);
        System.exit(res);
    }
}
