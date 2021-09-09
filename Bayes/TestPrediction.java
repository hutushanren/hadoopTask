package com.Bayes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
public class TestPrediction extends Configured implements Tool {
    private static HashMap<String, Double> priorProbability = new HashMap<String, Double>(); // 类的先验概率
    private static HashMap<String, Double> conditionalProbability = new HashMap<>(); // 每个单词在类中的条件概率
    //计算类的先验概率
    public static void Get_PriorProbability() throws IOException {
        Configuration conf = new Configuration();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineValue = null;
        HashMap<String, Double> temp = new HashMap<>(); //暂存类名和文档数
        double sum = 0;     //文档总数量
        try {
            FileSystem fs = FileSystem.get(URI.create(Utils.DocNumClass + "part-r-00000"), conf);
            fsr = fs.open(new Path( Utils.DocNumClass + "/part-r-00000"));
            bufferedReader = new BufferedReader(new InputStreamReader(fsr)); //文档读入流
            while ((lineValue = bufferedReader.readLine()) != null) { //按行读取
                // 分词：将每行的单词进行分割,按照"  \t\n\r\f"(空格、制表符、换行符、回车符、换页)进行分割
                StringTokenizer tokenizer = new StringTokenizer(lineValue);
                String className = tokenizer.nextToken(); //类名
                String num_C_Tmp = tokenizer.nextToken(); //文档数量
                double numC = Double.parseDouble(num_C_Tmp);
                temp.put(className, numC);
                sum = sum + numC; //文档总数量
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            bufferedReader.close();     //关闭资源
        }

        Iterator<Map.Entry<String, Double>> it = temp.entrySet().iterator();
        while (it.hasNext()){          //遍历计算先验概率
            Map.Entry val = (Map.Entry)it.next();
            String key = val.getKey().toString();
            double value = Double.parseDouble(val.getValue().toString());
            value /= sum;
            priorProbability.put(key, value);
        }
    }
    public static void Get_ConditionProbability() throws IOException {
        String filePath =Utils.WordNumClass + "/part-r-00000";
        Configuration conf = new Configuration();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineValue = null;
        HashMap<String,Double> wordSum=new HashMap<String, Double>(); //存放的为<类名，单词总数>

        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            fsr = fs.open(new Path(filePath));
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));
            while ((lineValue = bufferedReader.readLine()) != null) { //按行读取
                // 分词：将每行的单词进行分割,按照"  \t\n\r\f"(空格、制表符、换行符、回车符、换页)进行分割
                StringTokenizer tokenizer = new StringTokenizer(lineValue);
                String className = tokenizer.nextToken();
                String word =tokenizer.nextToken();
                String numWordTmp = tokenizer.nextToken();
                double numWord = Double.parseDouble(numWordTmp);
                if(wordSum.containsKey(className))
                    wordSum.put(className,wordSum.get(className)+numWord+1.0);//加1.0是因为每一次都是一个不重复的单词
                else
                    wordSum.put(className,numWord+1.0);
            }
            fsr.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // 现在来计算条件概率
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            fsr = fs.open(new Path(filePath));
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));
            while ((lineValue = bufferedReader.readLine()) != null) { //按行读取
                // 分词：将每行的单词进行分割,按照"  \t\n\r\f"(空格、制表符、换行符、回车符、换页)进行分割
                StringTokenizer tokenizer = new StringTokenizer(lineValue);
                String className = tokenizer.nextToken();
                String word =tokenizer.nextToken();
                String numWordTmp = tokenizer.nextToken();
                double numWord = Double.parseDouble(numWordTmp);
                String key=className+"\t"+word;
                conditionalProbability.put(key,(numWord+1.0)/wordSum.get(className));
                //System.out.println(className+"\t"+word+"\t"+wordsProbably.get(key));
            }
            fsr.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // 对测试集中出现的新单词定义概率
        Iterator iterator = wordSum.entrySet().iterator();	//获取key和value的set
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();	//把hashmap转成Iterator再迭代到entry
            Object key = entry.getKey();		//从entry获取key
            conditionalProbability.put(key.toString(),1.0/Double.parseDouble(entry.getValue().toString()));
        }
    }
    public static class Prediction_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void setup(Context context)throws IOException{
            Get_PriorProbability(); //先验概率
            Get_ConditionProbability(); //条件概率
        }
        private Text newKey = new Text();
        private Text newValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineValues = value.toString().split("\\s");    //分词，按照空白字符切割
            String class_Name = lineValues[0];   //得到类名
            String fileName = lineValues[1];    //得到文件名
            for(Map.Entry<String, Double> entry : priorProbability.entrySet()){
                String className = entry.getKey();
                newKey.set(class_Name + "\t" + fileName);//新的键值的key为<类明 文档名>
                double tempValue = Math.log(entry.getValue());//构建临时键值对的value为各概率相乘,转化为各概率取对数再相加
                for(int i=2; i<lineValues.length; i++){
                    String tempKey = className + "\t" + lineValues[i];//构建临时键值对<class_word>,在wordsProbably表中查找对应的概率
                    if(conditionalProbability.containsKey(tempKey)){
                        //如果测试文档的单词在训练集中出现过，则直接加上之前计算的概率
                        tempValue += Math.log(conditionalProbability.get(tempKey));
                    }
                    else{//如果测试文档中出现了新单词则加上之前计算新单词概率
                        tempValue += Math.log(conditionalProbability.get(className));
                    }
                }
                newValue.set(className + "\t" + tempValue);//新的键值的value为<类名  概率>
                context.write(newKey, newValue);//一份文档遍历在一个类中遍历完毕,则将结果写入文件,即<docName,<class  probably>>
                System.out.println(newKey + "\t" +newValue);
            }
        }
    }
    public static class Prediction_Reduce extends Reducer<Text, Text, Text, Text> {
        Text newValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            boolean flag = false;//标记,若第一次循环则先赋值,否则比较若概率更大则更新
            String tempClass = null;
            double tempProbably = 0.0;
            for(Text value:values){
                System.out.println("value......."+value.toString());
                String[] result = value.toString().split("\\s");
                String className=result[0];
                String probably=result[1];
                if(flag != true){//循环第一次
                    tempClass = className;//value.toString().substring(0, index);
                    tempProbably = Double.parseDouble(probably);
                    flag = true;
                }else{//否则当概率更大时就更新tempClass和tempProbably
                    if(Double.parseDouble(probably) > tempProbably){
                        tempClass = className;
                        tempProbably = Double.parseDouble(probably);
                    }
                }
            }
            newValue.set(tempClass + "\t" +tempProbably);
            //newValue.set(tempClass+":"+values.iterator().next());
            context.write(key, newValue);
            System.out.println(key + "\t" + newValue);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        FileSystem hdfs = FileSystem.get(conf);
        Path outputPath2 = new Path(Utils.Test_Prediction);
        if(hdfs.exists(outputPath2))
            hdfs.delete(outputPath2, true);
        Job job4 =Job.getInstance(conf, "Prediction");
        job4.setJarByClass(TestPrediction.class);
        job4.setMapperClass(Prediction_Mapper.class);
        job4.setCombinerClass(Prediction_Reduce.class);
        job4.setReducerClass(Prediction_Reduce.class);
        FileInputFormat.setInputDirRecursive(job4,true);
        job4.setOutputKeyClass(Text.class);//reduce阶段的输出的key
        job4.setOutputValueClass(Text.class);//reduce阶段的输出的value
        FileInputFormat.addInputPath(job4, new Path(Utils.Test_Preparation));
        FileOutputFormat.setOutputPath(job4, new Path(Utils.Test_Prediction));
        boolean result = job4.waitForCompletion(true);
        return (result ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TestPrediction(), args);
        System.exit(res);
    }
}
