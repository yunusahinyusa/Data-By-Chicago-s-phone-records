import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder().master("local").appName("Chicago-311-SparkSQL").getOrCreate();
        final Dataset<Row> rawData = sparkSession.read().option("header",true).csv("C:\\Users\\ysahi\\OneDrive\\Masaüstü");
        final Dataset<Row> secımData = rawData.select(new Column("Creation_Date"),
                new Column("Status"),
                new Column("Completion_Date"),
                new Column("Service_Request_Number"),
                new Column("Type_of_Service_Request"),
                new Column("Street_Address"));


        final Dataset<Row> dataByRequestNumber= secımData.groupBy(new Column("Service_Request_Number")).count();
        final Dataset<Row> dataByFiveRequest = dataByRequestNumber.filter(new Column("count").equalTo("5"));
        final Dataset<Row> dataBySpecialNumber = secımData.filter(new Column("Service_Request_Number").equalTo("14-01783521"));
        dataBySpecialNumber.show();



    }
}
