# 使用介绍
主要程序有两个ceshi.scala和data_create.cpp，其中ceshi.scala是Geospark的一键测试Scala源程序，运行方法如下：
```
./bin/spark-shell --master local[*] --packages org.datasyslab:geospark:1.2.0,org.datasyslab:geospark-sql_2.3:1.2.0,org.datasyslab:geospark-viz_2.3:1.2.0 -I /a1/spark/ceshi.scala
```

运行之后在指定的目录中生成一个result.txt，其中记录各个API的平均运行时间！

其中，修改循环次数和配置生成结果result.txt方法如下：
```
val resourceFolder = "/a1/spark/";
val myInputLocation = resourceFolder + "ceshi_data.csv"
val count = 100   //循环次数
 
 
val filewriter= "/a1/spark/result.txt"
val writer = new PrintWriter(new File(filewriter ))
 
writer.write("result is :\n")
````
如上所示，myInputLocation变量代表输入csv文件的地址，count变量代表循环次数，filewriter代表输出结果文件的地址，稍作配置即可！
同理，在data_create.cpp中更改count的值可以更改数据生成数目！