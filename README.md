# 前言
###### 你需要一個 hadoop 集群運行 mapreduce 程式。如果你還不知道如何搭建 hadoop 集群，可以參考這篇文章 
[用 Doker 搭建 Hadoop 3 Node 集群](http://codingstory.com.cn/yong-doker-da-jian-hadoop-3-node-ji-qun/)
###### 這篇文章僅介紹提取每日夜晚的最高溫度。我相信你在讀完這篇文章之後，如何提取每日夜晚最低溫度甚至進階到更複雜的操作，是水到渠成。

***

###### “大數據與基礎實務應用”課程期末專題。
要求從 2016.5月 到 2017.5月 一整年全台灣氣象觀測站測定的氣象數據中，分析出夏天與冬天，每日太陽落山後的溫度變化情況，以及冬夏溫度變化情況的差異。  

###### .txt 資料檔內容
該資料檔中存儲了不同地區，從 2016.5 月到 2017.5 月一年的時間裏，每天 24 小時的溫度記錄。
**即，一個地區，一天，有 24 筆溫度記錄。**



###### 資料格式：  
**下圖僅僅列出了 2016-12-28 1：00a.m. 這個時段部分地區的溫度記錄**

觀測地區 ID *substring（ 0, 4 ）* + 日期時間*substring ( 5,30 )* + 溫度*substring ( 31, 34 )*
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170604172038-1.png)

###### 鍵值對構造
**將 觀測地區 ID、date 作為 key ( *捨棄具體時間* )， 溫度作為 value。**構造這樣的鍵值對處理氣溫數據

例如： 第一行 Text，將被構造為
```
C0M7302016-12-28T01:00:00+08:0014.2

key = C0M7302016-12-28; 
value = 14.2;

``` 
###### 實現思路

* Map 程式。提取出每個地區，每天，**夜晚時段**的所有溫度記錄。即提取出同一把 key 的多個 value。

e.g.
```
key = C0M7302016-12-28; 
value[] = 14.2, 15.2, 14.8...

```
* Reduce 程式。從 Map 程式的輸出中，找出每個地區，每天，在夜晚时段的所有温度记录中的最高溫度記錄。即，一個 key 對應的最大 value。

e.g.
```
key = C0M7302016-12-28; 
value = 15.2

```
* 將 Reduce 程式的輸出結果，粘貼到 Excel 中進行分析處理。

***


# 環境
    Platform          Ubuntu 16.04 LTS， x64
    Jdk               java version "1.8.0_131"
    Hadoop            hadoop 2.8.0
    Maven             Apache Maven 3.5.0
    IDE               IntelliJ IDEA 172.2465.6
    

***

# 開始
* 1、編寫 Map 程式
* 2、編寫 Reduce 程式
* 3、編寫 Main 程式
* 4、編譯，打成 jar 包
* 5、上傳待處理 .txt 檔至 HDFS
* 6、使用 hadoop 執行 jar 包，處理 .txt 檔
* 7、將結果複製到 Excel 中分析

***
新建 maven 工程
###### pom.xml 添加如下依賴
```
<dependencies>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>2.8.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-common</artifactId>
      <version>2.8.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-hdfs</artifactId>
      <version>2.8.0</version>
    </dependency>
  </dependencies>
```

### 1、編寫 Map 程式
######　以這１行 Text 為例
```
C0M7302016-12-28T01:00:00+08:0014.2

``` 
分析當前行 Text。判斷當前 Text 資料時間範圍是否在太陽落山後。**( 18:00p.m. -- 5:00a.m. )**

   * 若是，將 stationID、date 作為 key ( *捨棄具體時間* )， 溫度作為 value。寫到 context 中，即 map 程式的 output 。
   * 若否，跳過當前行， 對下一行 Text 進行相同操作。

**時間為 1：00a.m.，在太陽落山後。那麼 map 程式會將這一行解析為**
``` 
key = C0M7302016-12-28; 
value = 14.2;
``` 
寫入到 context 中。（ *key 資料型別為 Text， value 資料型別為 FloatWritable* ）

然後繼續對下一行 Text 進行相同操作。

***

###### 以這６行 Text 為例, 注意 觀測地區 ID 、時間、 溫度均有所不同
```
C0M7302016-12-28T01:00:00+08:0014.2
C0M6902016-12-28T01:00:00+08:0013.9
C0M7302016-12-28T02:00:00+08:0014.1
C0M6902016-12-28T02:00:00+08:0013.8
C0M7302016-12-28T03:00:00+08:0014.0
C0M6902016-12-28T03:00:00+08:0013.7

``` 
###### map 程式的輸出結果
```
key = C0M7302016-12-28
value = 14.2
key = C0M6902016-12-28
value = 13.9
key = C0M7302016-12-28
value = 14.1
key = C0M6902016-12-28
value = 13.8
key = C0M7302016-12-28
value = 14.0
key = C0M6902016-12-28
value = 13.7
```

###### 程式碼如下
```language-python line-numbers
 public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {  // here need to extends FloatWritable type
      @Override
      public void map( LongWritable key, Text value, Context context )
          throws IOException, InterruptedException {

        String line = value.toString( );   //  transfer current Text to String type
        String locationDay = line.substring( 0, 16 ); // extract current key
        String currentTimeString;
        //String year = line.substring(0,4);
        float airTemperature; // temperature , type is float
        Date currentTimeDate = new Date( );
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat( "HH:mm:ss" ); // transfer string represent Date to really Date type, for determining whether current record time between 18:00:00 and 05:00:00

        try {
          line.concat( "0" ); // because string to float will lose accuracy, we need to avoid lose accuracy according to concat one character 
          airTemperature = Float.parseFloat( line.substring( 31, line.length() ).trim( ) ); // transfer string type temperature to float type 

          currentTimeString = line.substring( 17, 24 );  // extract current time string
          //System.out.println( currentTimeString );
          currentTimeDate = simpleDateFormat.parse( currentTimeString ); // transfer current time variable type from string to Date
          if ( currentTimeDate.after( simpleDateFormat.parse( "18:00:00" ) ) && currentTimeDate // determine whether the time is between 18:00:00 and 05:00:00 next  
              .before( simpleDateFormat.parse( "23:59:59" ) ) ) {
            //System.out.println( "Date compare success!" );
            context.write( new Text( locationDay ), new FloatWritable( airTemperature ) );
          }else if( currentTimeDate.after( simpleDateFormat.parse( "00:00:00" ) ) && currentTimeDate
              .before( simpleDateFormat.parse( "05:00:00" ) ) ){
            context.write( new Text( locationDay ), new FloatWritable( airTemperature ) );  // write to the context
          }else{ }

        } catch ( Exception e ) {
          e.printStackTrace( );
        }
      }
    }
```

***

### 1、編寫 Reduce 程式
###### 以這6對 map 程式的輸出為例
```
key = C0M7302016-12-28
value = 14.2
key = C0M6902016-12-28
value = 13.9
key = C0M7302016-12-28
value = 14.1
key = C0M6902016-12-28
value = 13.8
key = C0M7302016-12-28
value = 14.0
key = C0M6902016-12-28
value = 13.7
```
Reduce 程式接收 Map 程式傳來的 key， value。（ *i.e. 某個地點，當天，夜晚的多條溫度數據。* ）Reduce 程式用 key 把對應的 value 區分， 處理同一個 key 下的不同 value。把 key 與其對應的 value 中最大的 value 輸出到 context 中。那麼，在 Reduce 程式運行完後，就能得到從 Map 程式傳過來的所有 key 對應的最大 value。

###### reduce 程式的輸出
```
key = C0M7302016-12-28
value = 14.2
key = C0M6902016-12-28
value = 13.9
```
###### 程式碼如下
```language-python line-numbers
public static class MaxTemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> { // here need to extends FloatWritable type

      @Override
      public void reduce(Text key, Iterable<FloatWritable> values, Context context)
          throws IOException, InterruptedException{

        float maxValue = Float.MIN_VALUE;
        for(FloatWritable value : values){  // iterator all values belong to the key, choose the max value and output the key-value
          maxValue = Math.max( maxValue, value.get() );
        }
        context.write( key, new FloatWritable( maxValue ) );
      }
    }
```

***

### 3、編寫 Main 程式
Main 只是一些簡單的設定，告訴 Hadoop 每個類是在做什麼事情
###### 程式碼如下
```language-python line-numbers
 public static void main(String[] args) throws Exception{
      if (args.length!= 2)
      {
        System.err.println("Usage: MinTemperature<input path> <output path>");
        System.exit(-1);
      }
      //Job job= new Job();
      //Job job = Job.getInstance(Configuration conf);
      //Job job = Job.getInstance(getConf(), "MinTemperatureMapRedurce");
      Job job = Job.getInstance();
      job.setJarByClass(MaxTemperature.class);
      job.setJobName("Min temperature");
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass(MaxTemperatureMapper.class);
      job.setReducerClass(MaxTemperatureReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FloatWritable.class);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
```

***

### 4、編譯，打成 jar 包
hadoop 需要執行 MapReduce 程式打成的 JAR 包來進行 MapReduce 的操作

**按圖操作即可**
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170604231646.png)
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170604231753.png)
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170604231837.png)
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170604231947.png)
Build 或者 Rebuild 都可以
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170604232036.png)

Jar 包位置
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170604232339.png)

***

### 5、上傳待處理 .txt 檔至 HDFS
**以下所有操作都在 hadoop namenode 中完成**

創建一個目錄
```
$ hdfs dfs -mkdir -p /weatherData/data/input
```
將待處理的 .txt 檔案上傳到 HDFS 中
```
$ hdfs dfs -put ~/data/summer.txt  /weatherData/data/input
```

***
### 6、使用hadoop執行jar包，運行 MapReduce 程式處理.txt檔
cd 到 jar 包所在目錄
```
$ hadoop jar MaxTemperatureMapReduce_Winter.jar MaxTemperature /weatherData/input/winter.txt /weatherData/output/maxWinter
```
###### 參數解釋
MaxTemperatureMapReduce_Winter.jar  **編譯好的 JAR 包名**
MaxTemperature  **JAR 包中的項目名**
/weatherData/input/winter.txt  **待處理 .txt 檔在 hdfs 中的位置**
/weatherData/output/maxWinter  **處理結果輸出到的位置**

###### 程式運行過程截圖
![](http://codingstory.com.cn/content/images/2017/06/1.png)
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170603225255.png)

***

###  7、將結果複製到 Excel 中分析
###### hadoop 運行結果位置
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170604235626.png)
###### 從 hdfs 中獲取處理結果到本地
```
$ hadoop fs -get /weatherData/output/part-r-00000    ~/
```
###### 查看結果
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170604235955.png)
###### 將結果複製到 Excel 中並進行相關分析，可以得出如下變化情況。
![](http://codingstory.com.cn/content/images/2017/06/QQ--20170605000631.png)
**可以看出，台灣冬季夜晚平均溫差為 2.5℃，夏季平均温差為 1.96℃。氣候十分適宜人類居住。**

### 台灣如此溫柔。

***
# 以上
2017 年 6 月 5 日