from pyspark.sql import SparkSession
import databricks.koalas as ks

class DBConnectionSpark():
  def __init__(self, cloud:bool, on_prem:bool, pyspark:bool, pyodbc:bool, db_type:str=None, db_ip:str=None, db_name:str=None, user_name:str=None, password:str=None):
    if pyspark and on_prem:
      assert ((db_type is not None) and (db_ip is not None) and (db_name is not None) and (user_name is not None) and (password is not None))
      self.on_prem = on_prem
      self.pyspark = pyspark
      self.jdbc_uri = "jdbc:{0}://{1};databaseName={2};user={3};password={4}".format(db_type, db_ip, db_name, user_name, password) #jdbc:sqlserver://localhost;user=MyUserName;password=*****;
      #appName = "pyspark connection"
      #builder = SparkSession.builder.appName(appName)
      #builder = builder.config("spark.sql.execution.arrow.enabled", "true") \
      #				   .config("spark.driver.extraClassPath", path_jdbc_driver)
      #builder.getOrCreate()
      #conf = SparkConf().setAppName(appName)
      #sc = SparkContext.getOrCreate(conf=conf)
      #sqlContext = SQLContext(sc)
      spark = sqlContext.sparkSession
      
  def read_sql_str(self, sql_str:str):
    return(ks.read_sql(sql_str, self.jdbc_uri))
    
  def read_sql_file(self):
    ""
    
    
      
      