import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator
import java.io._


GeoSparkSQLRegistrator.registerAll(spark)
GeoSparkVizRegistrator.registerAll(spark)

val resourceFolder = "/a1/spark/";
val myInputLocation = resourceFolder + "ceshi_data.csv"
val count = 10   //循环次数

val filewriter= "/a1/spark/result.txt"
val writer = new PrintWriter(new File(filewriter ))

writer.write("result is :\n")



// load file
{
val t0 = System.nanoTime()
var i =0
val polygonWktDf = spark.read.format("csv").option("delimiter", ";").option("header", "false").load(myInputLocation)
polygonWktDf.createOrReplaceTempView("polygontable")
spark.sql("CACHE table polygontable")
val t1 = System.nanoTime()
writer.write("load file time: " + (t1 - t0) / 1E9 + " sec \n")
}

//CONSTRUCTOR
writer.write("-----------------------CONSTRUCTOR--------------------\n")
// ST_GeomFromWKT
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_GeomFromWKT(polygontable._c4) as countyshape from polygontable")
    polygonDf.createOrReplaceTempView("polygondf")
    spark.sql("CACHE table polygondf")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table polygondf")
    }
writer.write("ST_GeomFromWKT Mean query time: " + time/ (1E9 * count) + " sec \n")
}



//ST_Point
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_Point(CAST(polygontable._c0 AS Decimal(24,20)), CAST(polygontable._c0 AS Decimal(24,20)) ) as pointshape from polygontable")
    polygonDf.createOrReplaceTempView("point")
    spark.sql("CACHE table point")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table point")
    }
writer.write("ST_Point Mean query time: " + time/ (1E9 * count) + " sec \n")
}

//make ceshi table
{
val polygonDf = spark.sql("select ST_GeomFromWKT(polygontable._c4) as polygonshape, ST_GeomFromWKT(polygontable._c5) as polygonshape2 , ST_GeomFromWKT(polygontable._c6) as point, ST_GeomFromWKT(polygontable._c7) as point2 from polygontable")
polygonDf.createOrReplaceTempView("ceshi_table")
spark.sql("CACHE table ceshi_table")
}


//ST_Circle
// {
// var i =0
// var time:Long =0
// for (i <- 1 to count){
//     val t0 = System.nanoTime()
//     val polygonDf = spark.sql("select ST_Circle(ceshi_table.polygonshape , 1.0) as pointshape from ceshi_table")
//     polygonDf.createOrReplaceTempView("point")
//     spark.sql("CACHE table point")
//     time = time + System.nanoTime() - t0
//     spark.sql("UNCACHE table point")
//     }
// writer.write("ST_Circle Mean query time: " + time/ (1E9 * count) + " sec \n")
// }

//----------------------------------functions--------------------------------
writer.write("-----------------------FUNCTIONS--------------------\n")
//ST_Distance
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_Distance(ceshi_table.polygonshape, ceshi_table.polygonshape2) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_distance")
    spark.sql("CACHE table st_distance")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_distance")
    }
writer.write("ST_Distance Mean query time: " + time/ (1E9 * count) + " sec \n")
}

//ST_ConvexHull
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_ConvexHull(ceshi_table.polygonshape) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_convexhull")
    spark.sql("CACHE table st_convexhull")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_convexhull")
    }
writer.write("ST_ConvexHull Mean query time: " + time/ (1E9 * count) + " sec \n")
}

//ST_Envelope
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_Envelope(ceshi_table.polygonshape) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_envelope")
    spark.sql("CACHE table st_envelope")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_envelope")
    }
writer.write("ST_Envelope Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_Length
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_Length(ceshi_table.polygonshape) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_length")
    spark.sql("CACHE table st_length")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_length")
    }
writer.write("ST_Length Mean query time: " + time/ (1E9 * count) + " sec \n")
}



//ST_Area
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_Area(ceshi_table.polygonshape) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_area")
    spark.sql("CACHE table st_area")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_area")
    }
writer.write("ST_Area Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_Centroid
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_Centroid(ceshi_table.polygonshape) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_centroid")
    spark.sql("CACHE table st_centroid")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_centroid")
    }
writer.write("ST_Centroid Mean query time: " + time/ (1E9 * count) + " sec \n")
}



//ST_Centroid
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_Transform(ceshi_table.polygonshape, 'epsg:4326','epsg:3857') as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_transform")
    spark.sql("CACHE table st_transform")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_transform")
    }
writer.write("ST_Transform Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_Intersection
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_Intersection(ceshi_table.polygonshape,ceshi_table.polygonshape2) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_intersection")
    spark.sql("CACHE table st_intersection")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_intersection")
    }
writer.write("ST_Intersection Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_IsValid
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_IsValid(ceshi_table.polygonshape) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_isvalid")
    spark.sql("CACHE table st_isvalid")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_isvalid")
    }
writer.write("ST_IsValid Mean query time: " + time/ (1E9 * count) + " sec \n")
}



//ST_PrecisionReduce
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_PrecisionReduce(ceshi_table.polygonshape , 9) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_precisionreduce")
    spark.sql("CACHE table st_precisionreduce")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_precisionreduce")
    }
writer.write("ST_PrecisionReduce Mean query time: " + time/ (1E9 * count) + " sec \n")
}



//ST_IsSimple
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_IsSimple(ceshi_table.polygonshape) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_issimple")
    spark.sql("CACHE table st_issimple")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_issimple")
    }
writer.write("ST_IsSimple Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_Buffer
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_Buffer(ceshi_table.polygonshape , 1) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_buffer")
    spark.sql("CACHE table st_buffer")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_buffer")
    }
writer.write("ST_Buffer Mean query time: " + time/ (1E9 * count) + " sec \n")
}

//ST_AsText
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select ST_AsText(ceshi_table.polygonshape) as pointshape from ceshi_table")
    polygonDf.createOrReplaceTempView("st_astext")
    spark.sql("CACHE table st_astext")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_astext")
    }
writer.write("ST_AsText Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_NPoints
// {
// var i =0
// var time:Long =0
// for (i <- 1 to count){
//     val t0 = System.nanoTime()
//     val polygonDf = spark.sql("select ST_NPoints(ceshi_table.polygonshape) as pointshape from ceshi_table")
//     polygonDf.createOrReplaceTempView("st_npoints")
//     spark.sql("CACHE table st_npoints")
//     time = time + System.nanoTime() - t0
//     spark.sql("UNCACHE table st_npoints")
//     }
// writer.write("ST_NPoints Mean query time: " + time/ (1E9 * count) + " sec \n")
// }


//ST_GeometryType
// {
// var i =0
// var time:Long =0
// for (i <- 1 to count){
//     val t0 = System.nanoTime()
//     val polygonDf = spark.sql("select ST_GeometryType(ceshi_table.polygonshape) as pointshape from ceshi_table")
//     polygonDf.createOrReplaceTempView("st_geometrytype")
//     spark.sql("CACHE table st_geometrytype")
//     time = time + System.nanoTime() - t0
//     spark.sql("UNCACHE table st_geometrytype")
//     }
// writer.write("ST_GeometryType Mean query time: " + time/ (1E9 * count) + " sec \n")
// }

//---------------------Predicate-------------------------------------------
writer.write("-----------------------PREDICATE--------------------\n")
//ST_Contains
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select * from ceshi_table where ST_Contains( ceshi_table.polygonshape,ceshi_table.polygonshape2 )")
    polygonDf.createOrReplaceTempView("st_contains")
    spark.sql("CACHE table st_contains")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_contains")
    }
writer.write("ST_Contains Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_Intersects
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select * from ceshi_table where ST_Intersects( ceshi_table.polygonshape,ceshi_table.polygonshape2 )")
    polygonDf.createOrReplaceTempView("st_intersects")
    spark.sql("CACHE table st_intersects")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_intersects")
    }
writer.write("ST_Intersects Mean query time: " + time/ (1E9 * count) + " sec \n")
}



//ST_Within
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select * from ceshi_table where ST_Within( ceshi_table.polygonshape,ceshi_table.polygonshape2 )")
    polygonDf.createOrReplaceTempView("st_within")
    spark.sql("CACHE table st_within")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_within")
    }
writer.write("ST_Within Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_Equals
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select * from ceshi_table where ST_Equals( ceshi_table.polygonshape,ceshi_table.polygonshape2 )")
    polygonDf.createOrReplaceTempView("st_equals")
    spark.sql("CACHE table st_equals")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_equals")
    }
writer.write("ST_Equals Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_Crosses
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select * from ceshi_table where ST_Crosses( ceshi_table.polygonshape,ceshi_table.polygonshape2 )")
    polygonDf.createOrReplaceTempView("st_crosses")
    spark.sql("CACHE table st_crosses")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_crosses")
    }
writer.write("ST_Crosses Mean query time: " + time/ (1E9 * count) + " sec \n")
}


//ST_Touches
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select * from ceshi_table where ST_Touches( ceshi_table.polygonshape,ceshi_table.polygonshape2 )")
    polygonDf.createOrReplaceTempView("st_touches")
    spark.sql("CACHE table st_touches")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_touches")
    }
writer.write("ST_Touches Mean query time: " + time/ (1E9 * count) + " sec \n")
}

//ST_Overlaps
{
var i =0
var time:Long =0
for (i <- 1 to count){
    val t0 = System.nanoTime()
    val polygonDf = spark.sql("select * from ceshi_table where ST_Overlaps( ceshi_table.polygonshape,ceshi_table.polygonshape2 )")
    polygonDf.createOrReplaceTempView("st_overlaps")
    spark.sql("CACHE table st_overlaps")
    time = time + System.nanoTime() - t0
    spark.sql("UNCACHE table st_overlaps")
    }
writer.write("ST_Overlaps Mean query time: " + time/ (1E9 * count) + " sec \n")
}



writer.close()





