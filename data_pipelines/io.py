from pyspark.sql import SparkSession
import databricks.koalas as ks
from azure.common.client_factory import get_client_from_cli_profile
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.sql import SqlManagementClient
import env

class DBConnectionServer():
  def __init__(self, connection_library:str):
    "Args:"
    "  connection_library: pyodbc or pyspark"
    "Returns:"
    "Raises:"
    if connection_library == "pyodbc":
      ""

class DBConnectionCloud():
  def __init__(self, which_cloud:str, env=None):
    "Args:"
    "  which_cloud: 'azure' or 'gcp' or 'aws'"
    if which_cloud == 'azure':    
      account_name = env.get_azure_account_name(env)



class DBConnectionSpark():
  "db_type: options are 'sqlserver' etc. to general db uri"
  def __init__(self, cloud:bool, on_prem:bool, pyspark:bool, pyodbc:bool, db_type:str=None, db_ip:str=None, db_name:str=None, user_name:str=None, password:str=None):
    if pyspark and on_prem:
      assert ((db_type is not None) and (db_ip is not None) and (db_name is not None) and (user_name is not None) and (password is not None))
      self.on_prem = on_prem
      self.pyspark = pyspark
      db_type = (''.join(db_type.split())).lower()
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
    
class DBCreation():
	def __init__(self, cloud:bool, onprem:bool,db_type:str,  resource_group:bool="YOUR_RESOURCE_GROUP_NAME", location:str="eastus", virtual_sql_server:str="yourvirtualsqlserver", db_name:str="YOUR_SQLDB_NAME", user_name:str="YOUR_USERNAME", password:str="PWD" ):
		"db_type: 'Azure SQL'"
		self.cloud=cloud
		self.onprem=onprem
		self.db_type=db_type 
		if self.cloud and (''.join(db_type.split())).lower() == 'azuresql':
			self.resource_group=resource_group
			self.location=location
			self.virtual_sql_server = virtual_sql_server
			self.resource_client = self.create_resource_client()
			self.sql_client = self.create_sql_client()
			self.server = self.create_sql_server()
			self.database = self.create_sql_db()
	
		self.db_name = db_name
		self.user_name = user_name
		self.password = password
 
	def create_resource_client(self):
		# create resource client
		return(get_client_from_cli_profile(ResourceManagementClient))

	def create_resource_group(self):
		# create resource group
		self.resource_client.resource_groups.create_or_update(self.resource_group, {'location': self.location})

	def create_sql_client(self):
		return(get_client_from_cli_profile(SqlManagementClient))

	def create_sql_server(self):

		# Create a SQL server
		version = '12.0'
		server = sql_client.servers.create_or_update(
    			self.resource_group,
    			self.virtual_sql_server,
    			{
        		'location': self.location,
        		'version': version, # Required for create
        		'administrator_login': self.user_name, # Required for create
        		'administrator_login_password': self.password # Required for create
    			}
		)
		return(server)

	def create_sql_db(self):

		# Create a SQL database in the Basic tier
		database = self.sql_client.databases.create_or_update(
    				self.resource_group,
    				self.server,
    				self.db_name,
    				{
        			'location': self.location,
        			'collation': 'SQL_Latin1_General_CP1_CI_AS',
        			'create_mode': 'default',
        			'requested_service_objective_name': 'Basic'
    				}
				)

		return(database)

# Open access to this server for IPs
firewall_rule = sql_client.firewall_rules.create_or_update(
    RESOURCE_GROUP,
    SQL_DB,
    "firewall_rule_name_123.123.123.123",
    "123.123.123.123", # Start ip range
    "167.220.0.235"  # End ip range
)
      
      
