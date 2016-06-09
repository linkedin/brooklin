
import os

import zopkio.adhoc_deployer as adhoc_deployer
import zopkio.runtime as runtime

server_deployer = None
client_deployer = None


def setup_suite():
  client_exec_location = runtime.get_active_config('client_exec_path')

  #Clients and server exec can also be in remote location.
  #if so specify the client_exec as <Remoteserver>:<path>
  #In that case you can also path a temp_scratch config for the sftp
  #else /tmp folder used as default
  if (":" in client_exec_location):
    client_exec = client_exec_location
  else:
    client_exec = os.path.join(os.path.dirname(os.path.abspath(__file__)),
      client_exec_location)

  global client_deployer
  client_deployer = adhoc_deployer.SSHDeployer("AdditionClient",
      {'pid_keyword': "AdditionClient",
       'executable': client_exec,
       'start_command': runtime.get_active_config('client_start_command')})
  runtime.set_deployer("AdditionClient", client_deployer)

  client_deployer.install("client1",
      {"hostname": "localhost",
       "install_path": runtime.get_active_config('client_install_path') + 'client1'})

  client_deployer.install("client2",
      {"hostname": "localhost",
       "install_path": runtime.get_active_config('client_install_path') + 'client2'})

  server_exec_location = runtime.get_active_config('server_exec_path')


  if (":" in server_exec_location):
    server_exec = server_exec_location
  else:
    server_exec = os.path.join(os.path.dirname(os.path.abspath(__file__)),
    runtime.get_active_config('server_exec_path'))

  global server_deployer
  server_deployer = adhoc_deployer.SSHDeployer("AdditionServer",
      {'pid_keyword': "AdditionServer",
       'executable': server_exec,
       'start_command': runtime.get_active_config('server_start_command')})
  runtime.set_deployer("AdditionServer", server_deployer)

  server_deployer.deploy("server1",
      {"hostname": "localhost",
       "install_path": runtime.get_active_config('server_install_path') + 'server1',
       "args": "localhost 8000".split()})

  server_deployer.deploy("server2",
      {"hostname": "localhost",
       "install_path": runtime.get_active_config('server_install_path') + 'server2',
       "args": "localhost 8001".split()})

  server_deployer.deploy("server3",
      {"hostname": "localhost",
       "install_path": runtime.get_active_config('server_install_path') + 'server3',
       "args": "localhost 8002".split()})

def setup():
  for process in server_deployer.get_processes():
    server_deployer.start(process.unique_id)

def teardown():
  for process in client_deployer.get_processes():
    client_deployer.stop(process.unique_id)

def teardown_suite():
  for process in server_deployer.get_processes():
    server_deployer.undeploy(process.unique_id)
