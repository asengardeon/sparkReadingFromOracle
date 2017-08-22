import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
	private static final Object ORACLE_USERNAME = "handitloghaus";
	private static final Object ORACLE_PWD = "demo";
	private static final String ORACLE_CONNECTION_URL = "jdbc:oracle:thin:@localhost:1521:xe";
	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Main.class);

	private static final SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("Spark2JdbcDs").config("spark.sql.warehouse.dir", "file:///C:/temp/").getOrCreate();
	
	
	public static void main(String[] args) {
		// JDBC connection properties
		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", ORACLE_USERNAME);
		connectionProperties.put("password", ORACLE_PWD);
		connectionProperties.put("driver", ORACLE_DRIVER);

		//final String dbTable = "(select emp_no, concat_ws(' ', first_name, last_name) as full_name from employees) as employees_name";
		final String dbTable = "(select CAST( PEDIDOS as NUMERIC(18,6)) as PEDIDOS, CAST( VLR_REALIZADO as NUMERIC(18,6)) as VLR_REALIZADO from CUBEIGDPP__REALIZADO_BASE)  ";//outra opção na lin ha acima
		
		// Load SQL query result as Dataset		
		Dataset<Row> jdbcDF = sparkSession.read().jdbc(ORACLE_CONNECTION_URL, dbTable, connectionProperties);

		List<Row> linhasCubo = jdbcDF.collectAsList();
		Dataset<Row> sorted = jdbcDF.orderBy("PEDIDOS");
		
		LOGGER.info("Leu DF: "+linhasCubo.size());
		for (Row row : linhasCubo) {
			LOGGER.info(row);
		}
		
		LOGGER.info("DS ORDENADO: "+sorted.count());
		
		
		System.out.println("SCHEMA =======================");
		jdbcDF.printSchema();
		
		
		
	}
}
