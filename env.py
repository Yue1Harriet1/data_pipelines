"""All of the environment variables
setters
getters
"""

import os

DRIVER = "MY_DB_DRIVER"
SERVER = "MY_DB_SERVER"
PORT = "MY_DB_PORT"
DATABASE = "MY_DATABASE"
UID = "MY_UID"
PWD = "MY_PWD"
WINDOWS_AUTHENTICATION = "TRUSTED_CONNECTION" #yes or otherwise
CONN_STR = "MY_DB_CONNECTION_STRING"
CONNECTION_METHOD = "MY_DB_CONNECTION_METHOD" #pyodbc, pyspark, etc.

def set_env(KEY, value, env=None):
  if env is None:
    env = os.environ
  env[KEY] = value

def get_env_variable(KEY, default=None, env=None):
  if env is None:
    env = os.environ
  value = env.get(KEY, default)
  return(value)

def set_driver(value, env=None):
  set_env(DRIVER, value, env=env)
  
def set_server(value, env=None):
  set_env(SERVER, value, env=env)
  
def set_port(value, env=None):
  set_env(PORT, value, env=env)
  
def set_database(value, env=None):
  set_env(DATABASE, value, env=env)
  
def set_uid(value, env=None):
  set_env(UID, value, env=env)
  
def set_password(value, env=None):
  set_env(PWD, value, env=env)
  
def set_trusted_connection(value, default=None, env=None):
  try:
    v = lower(value)
  except ValueError:
    v = default
  set_env(WINDOWS_AUTHENTICATION, value=v, env=env)
  
def set_connection_method(value, env=None):
  v = lower(value)
  set_env(KEY=CONNECTION_METHOD, value=v, env=env)

def get_connection_method(default=None, env=None):
  value = get_env_variable(KEY=CONNECTION_METHOD, default=default, env=env)
  try:
    value = lower(value)
  except ValueError:
    value = default
  return(value)
  
  
def set_connection_string(env=None, default=None):
  if env is None:
    env = os.environ
  conn_method = get_connection_method(default=None, env=env)
  driver = get_env_variable(KEY=DRIVER, default=default, env=env)
  server = get_env_variable(KEY=SERVER, default=default, env=env)
  db = get_env_variable(KEY=DATABASE, default=default, env=env)
  windows_conn = get_env_variable(KEY=WINDOWS_AUTHENTICATION, default=default, env=env)
  
  if conn_method == "pyodbc":
    if windows_conn == "yes":
      v = 'Driver={0};Server={1};Database={2};Trusted_Connection={3};'.format(driver, server, db, windows_conn)
  set_env(KEY=CONNECTION_METHOD, value=v, env=env)
    
