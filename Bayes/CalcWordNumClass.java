package com.Bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class CalcWordNumClass extends Configured implements Tool {
    /*
     * 第二个MapReduce用于统计每个类下单词的数量
     */
    public static class CalcWordNum_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text key_out = new Text();
        private IntWritable one = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String className = ((FileSplit)inputSplit).getPath().getParent().getName();
            String line_context = value.toString();
            key_out.set(className + '\t' + line_context);
            context.write(key_out, one);
        }
    }
    public static class CalcWordNum_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (IntWritable value: values){
                num += value.get();
            }
            result.set(num);
            context.write(key, result);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job2 = Job.getInstance(conf, "CalcWordNum");
        FileSystem fileSystem = FileSystem.get(conf);
        Path outputPath2 = new Path(Utils.WordNumClass);
        if(fileSystem.exists(outputPath2))
            fileSystem.delete(outputPath2, true);
        //设置jar加载路径
        job2.setJarByClass(CalcWordNumClass.class);
        //设置map和reduce类
        job2.setMapperClass(CalcWordNum_Mapper.class);
        job2.setCombinerClass(CalcWordNum_Reducer.class);
        job2.setReducerClass(CalcWordNum_Reducer.class);
        //设置map reduce输出格式
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        //设置输入和输出路径
        FileInputFormat.setInputDirRecursive(job2,true);
        FileInputFormat.addInputPath(job2, new Path(Utils.TRAIN_DATA_PATH));
//        FileInputFormat.setInputPaths(job2, new Path(Utils.TRAIN_DATA_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(Utils.WordNumClass));
        boolean result = job2.waitForCompletion(true);
        return (result ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CalcWordNumClass(), args);
        System.exit(res);
    }
}
