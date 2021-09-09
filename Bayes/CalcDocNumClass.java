package com.Bayes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

public class CalcDocNumClass  extends Configured implements Tool {
    /*
     * 第一个MapReduce用于统计每个类对应的文件数量
     * 为计算先验概率准备:
     */
    public static class CalcDocNumClassMap extends Mapper<Text, BytesWritable, Text, IntWritable> {
//        private Text newKey = new Text();
        private final static IntWritable one = new IntWritable(1);
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException{
            context.write(key, one);
        }
    }
    public static class CalcDocNumClassReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value:values){
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        FileSystem hdfs = FileSystem.get(conf);

        Path outputPath1 = new Path(Utils.DocNumClass);
        if(hdfs.exists(outputPath1))
            hdfs.delete(outputPath1, true);

        Job job1 =Job.getInstance(conf, "CalcDocNum");
        job1.setJarByClass(CalcDocNumClass.class);
        //设置输入输出格式
        job1.setInputFormatClass(WholeFileInputFormat.class);
        job1.setMapperClass(CalcDocNumClassMap.class);
        job1.setCombinerClass(CalcDocNumClassReduce.class);
        job1.setReducerClass(CalcDocNumClassReduce.class);

        FileInputFormat.setInputDirRecursive(job1,true);
        job1.setOutputKeyClass(Text.class);                 //reduce阶段的输出的key
        job1.setOutputValueClass(IntWritable.class);        //reduce阶段的输出的value
        FileInputFormat.addInputPath(job1, new Path(Utils.TRAIN_DATA_PATH));
        FileOutputFormat.setOutputPath(job1, new Path(Utils.DocNumClass));
        return job1.waitForCompletion(true) ? 0 : 1;
    }

    public static class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable>{

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;   //文件输入的时候不再切片
        }

        @Override
        public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            WholeFileRecordReader reader = new WholeFileRecordReader();
            reader.initialize(inputSplit, taskAttemptContext);
            return reader;
        }

    }

    public static class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
        private FileSplit fileSplit;           //保存输入的分片，它将被转换成一条（key，value）记录
        private Configuration conf;     //配置对象
        private Text key = new Text();        //key对象，初始值为空
        private BytesWritable value = new BytesWritable(); //value对象，内容为空
        private boolean isRead = false;   //布尔变量记录记录是否被处理过

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) split;  	     //将输入分片强制转换成FileSplit
            this.conf = context.getConfiguration();  //从context获取配置信息
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!isRead) {  //如果记录有没有被处理过
                //定义缓存区
                byte[] contents = new byte[(int) fileSplit.getLength()];
                FileSystem fs = null;
                FSDataInputStream fis = null;
                try {
                    //获取文件系统
                    Path path = fileSplit.getPath();
                    fs = path.getFileSystem(conf);
                    //读取数据
                    fis = fs.open(path);
                    //读取文件内容
                    IOUtils.readFully(fis, contents, 0, contents.length);
                    //输出内容文件
                    value.set(contents, 0, contents.length);
                    //获取文件所属类名称
                    String classname = fileSplit.getPath().getParent().getName();
                    key.set(classname);
                }catch (Exception e){
                    System.out.println(e);
                }finally {
                    IOUtils.closeStream(fis);
                }
                isRead = true;   //将是否处理标志设为true，下次调用该方法会返回false
                return true;
            }
            else {
                return false;   //如果记录处理过，返回false，表示split处理完毕
            }
        }
        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }
        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }
        @Override
        public float getProgress() throws IOException {
            return isRead ? 1.0f : 0.0f;
        }
        @Override
        public void close() throws IOException {
        }
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CalcDocNumClass(), args);
        System.exit(res);
    }
}
