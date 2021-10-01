import requests, json
import base64
import pprint

from urllib.parse import urljoin
import configparser
import click

class dbclient(object):
  def __init__(self, workspace_url, token=None, auth_type='bearer', user=None, password=None):
    self.workspace_url = workspace_url
    self.auth_type = auth_type
    self.token = token
    self.user = user
    self.password = password
  
  def db_request(self, endpoint, method, body=None):
    url = urljoin(self.workspace_url, endpoint)

    headers=None
    auth=None
    if self.auth_type == 'bearer':
      headers={'Authorization': f'Bearer {self.token}'}
    elif self.auth_type == 'basic':
      auth = (self.user, self.password)
    else:
      raise Exception('config param error')
      
    r=requests.request(method, url, headers=headers, auth=auth, json=body)
    
    print('>>>>>', r.request.headers)

    if r.status_code != 200:
      raise Exception(f'status_code => {r.status_code}')
    return r
  
  def delete_all_clusters(self):
    r=self.db_request('api/2.0/clusters/list', 'GET')
    print(r.status_code)
    try:
      print(r.json())
    except Exception as e:
      print(e)
  
    # parse cluster_id 
    cluster_ids=[]
    for c in r.json()['clusters']:
      cluster_ids.append( c['default_tags']['ClusterId'] )
    print(cluster_ids)
    
    # remove clusters
    for c_id in cluster_ids:
      body={'cluster_id': c_id}
      print(body)
      r=self.db_request('/api/2.0/clusters/permanent-delete', 'POST', body)
  
  def _get_object_list(self, path):
    'returns notebooks, directories'
    # get file list
    body={'path': path}
    r=self.db_request('/api/2.0/workspace/list', 'GET', body)
    pprint.pprint(r.json())

    notebooks=[]
    directories=[]
    try:
      for obj in r.json()['objects']:
        if obj['object_type'] == 'NOTEBOOK':
          notebooks.append( obj['path'] )
        elif obj['object_type'] == 'DIRECTORY':
          directories.append( obj['path'] )
    except Exception as e:
      print(e)

    return notebooks, directories


  def _clean_in_directory(self, directory, notebook_only=True):
    
    # get object list
    notebooks, directories = self._get_object_list(directory)

    # remove notebooks
    for n in notebooks:
      body={'path': n, 'recursive': False}
      print(body)
      self.db_request('/api/2.0/workspace/delete', 'POST', body)
    
    # remove directories recursively
    if notebook_only == False:
      for d in directories:
        body={'path': d, 'recursive': True}
        print(body)
        self.db_request('/api/2.0/workspace/delete', 'POST', body)
        

  def clean_all_notebooks(self):
    # clean notebooks in root directory
    self._clean_in_directory('/', notebook_only=True)

    # clean all notebooks in users directory - recursive
    _, homes = self._get_object_list('/Users')
    for h in homes:
      print(f'removing home directory: {h}')
      self._clean_in_directory(h, notebook_only=False)
      

class dbapp(object):
  def __init__(self):
    pass
  def getConfig(self, config_filename='config', profile_name='default'):
    config = configparser.ConfigParser()
    config.read(config_filename)

    self.auth = config[profile_name]['auth']
    if self.auth.lower() == 'bearer':
      self.token = config[profile_name]['token']
      self.api_base_url = config[profile_name]['api_base_url']
      self.dbc = dbclient(self.api_base_url, self.token)
    elif self.auth.lower() == 'basic':
      self.user = config[profile_name]['user']
      self.password = config[profile_name]['password']
      self.api_base_url = config[profile_name]['api_base_url']
      self.dbc = dbclient(self.api_base_url, auth_type='basic', user=self.user, password=self.password)
    else:
      raise Exception('config param error')

  
  def call_api(self, call_api_dict):
    '''
    example)
    call_api_dict:
          {'method':'get', 
           'path':'/api/2.0/workspace/list', 
           'body':{'path': '/'} }
    '''
    c = call_api_dict
    body=None
    if 'body' in c:
      body=c['body']
    r=self.dbc.db_request(c['path'], c['method'], body=body)
    print(r.text)

  def run_jobs(self, jobs):
    '''
    jobs is a list of job definition:
    example: 
    [ {'name': 'job1', 'method': 'get', 'path':'/api/2.0/workspace/list'}, ... ]
    '''
    for j in jobs:
      print(f"runnig job: {j['name']}")
      self.call_api(j)



@click.command()
@click.option('-p', '--profile', type=str, default='default')
@click.option('-c', '--config', type=str, default='dbclient.conf')
@click.argument('jobs_json', type=str)
def cmd(profile, config, jobs_json):
  print(f'>>>> profile => {profile}')
  print(f'>>>> config  => {config}')
  jobs=None
  with open(jobs_json, 'r') as f:
    jobs=json.load(f)
  
  app = dbapp()
  app.getConfig(config, profile)
  app.run_jobs(jobs)

if __name__ == '__main__':
  cmd()
