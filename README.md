# dbclient

## Installation

from git repo:
```bash
$ git clone https://github.com/ktmrmshk/dbclient.git
$ cd dbclient
$ python -m pip install -r requirements.txt

$ python dbclient.py --help

Usage: dbclient.py [OPTIONS] JOBS_JSON

Options:
  -p, --profile TEXT
  -c, --config TEXT
  --help              Show this message and exit.
```

## Usage

* edit the config file `dbclient.conf` based on the template file

```bash
$ cp dbclient.conf.example dbclient.conf
$ vim dbclient.conf

[default]
auth = bearer
token = xxxxxxxxxxxxxxxxxxxxxxx <== replace with your peronal access token
api_base_url = https://xxxxxxxxxxxxxx.cloud.databricks.com/ <=== replace with your workspace url
```

* create a jobs json files: refer the template file

```bash
$ cat jobs.json.example

[
   {
      "name":"job1",
      "method":"get",
      "path":"/api/2.0/workspace/list",
      "body":{
         "path":"/"
      }
   },
   {
      "name":"job2",
      "method":"get",
      "path":"/api/2.0/workspace/list",
      "body":{
         "path":"/"
      }
   },
   {
      "name":"job3",
      "method":"get",
      "path":"/api/2.0/workspace/list",
      "body":{
         "path":"/"
      }
   }
]


```

* run the jobs!

```bash
$ python dbclient.py jobs.json.example
...
...
```
