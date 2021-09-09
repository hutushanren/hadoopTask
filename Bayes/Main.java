package com.Bayes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
public class Main {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //统计每个类中文件数目
        CalcDocNumClass computeDocNumInClass = new CalcDocNumClass();
        ToolRunner.run(configuration, computeDocNumInClass, args);
        //统计每个类中出现单词总数
        CalcWordNumClass calcWordNumClass = new CalcWordNumClass();
        ToolRunner.run(configuration, calcWordNumClass, args);
        //测试集数据预处理
        TestPreparation testPreparation = new TestPreparation();
        ToolRunner.run(configuration, testPreparation, args);
        //预测测试集文件类别
        TestPrediction testPrediction = new TestPrediction();
        ToolRunner.run(configuration, testPrediction, args);
        //评估测试效果，计算precision，recall，F1
        Evaluation evaluation = new Evaluation();
        ToolRunner.run(configuration, evaluation, args);
    }
}
