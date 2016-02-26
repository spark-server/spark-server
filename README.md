# Spark-Server

The `spark-server` is a Node.js REST interface for an Apache Spark REPL which can execute JavaScript, via [node-java](https://github.com/joeferner/node-java)'s JNI bridge. 
It provides access to **Spark contexts**, which can be shared via **sessions**, and allows to execute **statements** within those **contexts**. It is mainly focused on Spark's `DataFrame` APIs.

**Motivation:**  
To create a proof-of-concept of a usable and multi-user Spark REPL web service. There are other projects, such as [Livy (Hue subproject)](https://github.com/cloudera/hue/tree/master/apps/spark/java) 
or [Apache Toree](http://toree.incubator.apache.org/), but each of them has it's own focus, and none of them is usable with JavaScript.

**Current status:**  
This project is to be considered as `alpha` code. Hopefully there will be some contributors which help to implement further features. Any help is welcome!

**Roadmap:**  
- [x] Optimize result parsing
- [x] Improved logging
- [ ] Better documentation at [spark-server.github.io](http://spark-server.github.io/)
- [ ] Code cleanup & refactoring
- [ ] Add tests
- [ ] Add authentication

## Contents

* [Installation](#installation)
* [Configuration](#configuration)
    - [Preconditions](#preconditions)
    - [Application environment variables](#application-environment-variables)
* [Running](#running)
    - [Running locally](#running-locally)
    - [Running via Docker](#running-via-docker)
    - [Running on Mesos via Marathon](#running-on-mesos-via-marathon)
* [API](#api)
* [Capabilities](#capabilities)
* [Examples](#examples)
    - [Creating a context](#creating-a-context)
    - [Creating a session](#creating-a-session)
    - [Execution of statements](#execution-of-statements)
* [Thanks](#thanks)

## Installation

The application can either be installed via `git clone https://github.com/spark-server/spark-server.git && cd spark-server && npm install` or via `docker pull sparkserver/spark-server`.

## Configuration

### Preconditions

When the application is installed from Git directly, the following preconditions are in effect:

* JDK >= 1.7 is installed and the `JAVA_HOME` environment variable is set correctly
* Apache Spark >= 1.5 is installed and the `SPARK_HOME` environment variable is set correctly
* Node.js >= 4 is installed

### Application environment variables

The following variables can be set:

* `ASSEMBLY_JAR`: The path to the Spark assambly jar (to be found in the `lib` folder of the Apache Spark distribution). When running via Docker, this gets automatically populated. Otherwise this need to be defined manually. **(mandatory)**.
* `PORT_RANGE_START`: The start of the application's port range (e.g. 15000). Default is 10000 if not specified.
* `PORT_RANGE_END`: The end of the application's port range (e.g. 15500). Default is 10100 if not specified.
* `SPARK_PACKAGE_PATH`: Set the path where the Spark package (*.tgz) is located locally. If set, Spark-Server will serve this file and automatically set the `spark.executor.uri` property, so that it can use a Mesos cluster to run the executors on. When running via Docker, this is automatically enabled.
* `APP_PORT`: The port on which the REST API server will listen.
* `BIND_INTERFACE`: The network interface name which Apache Spark should use to determine the IP address to bind to. Default is `en0`. 

## Running

### Running locally

Change to the folder to which you cloned `spark-server` to, and run `node spark-server.js` or `npm start`. This will start the app on port 3000 and bind on `0.0.0.0` as default. Make sure that the `ASSEMBLY_JAR` environment variable is set, and points to the Spark assembly jar.

### Running via Docker

    docker run -d -p 3000:3000 --name spark-server sparkserver/spark-server
    
### Running on Mesos via Marathon

It can be run through Mesos/Marathon like this:

```
curl -XPOST 'http://{MARATHON_URL}:8080/v2/apps' -H 'Content-Type: application/json' -d '{
  "id": "spark-server",
  "container": {
    "type": "DOCKER",
    "docker": {
      "network": "BRIDGE",
        "image": "sparkserver/spark-server",
        "forcePullImage": true,
        "portMappings": [
          { "containerPort": 3000 }
        ]
    }
  },
  "cpus": 4,
  "mem": 16384,
  "instances": 1
}'
```

If you also want to run Spark on the Mesos cluster, you have to specify the correct `masterUrl` property in the context configuration, such as `mesos://zk://192.168.0.101:2181,192.168.0.102:2181,192.168.0.103:2181/mesos`.

## API

You can find the generated RAML documentation of the REST API at [http://spark-server.github.io/api/index.html](http://spark-server.github.io/api/index.html). 

## Capabilities

The `spark-server` is able to run/execute JavaScript (including ES7-style async/await syntax, which is transpiled via babel.js) code together with Apache Spark code, which is bridged via [node-java](https://github.com/joeferner/node-java)'s JNI capabilities. 
Downside of using the JNI bridge is that the JS code gets "uglier" than the original Scala or Java code (see examples below), but this is in the nature of using a bridge.

By default, the created Spark contexts use the [FAIR scheduler](https://spark.apache.org/docs/latest/job-scheduling.html#scheduling-within-an-application) to enable the parallel processing of jobs on the executors. 
The scheduler pool is defined in the `config/spark_pools.xml` file.

## Examples

The examples will use `curl` to show how the API can be used. It is assumed that the `spark-server` is running on `http://localhost:3000`.

### Creating a context

We can create a new Spark context called `mycontext` by supplying some parameters in the request body. For an overview of possible parameters, see the [API docs](http://spark-server.github.io/api/index.html#contexts__contextName__post).

```bash
curl -X POST -H "Content-Type: application/json" -d '{
    "config": {
        "createSQLContext": true,
        "properties": {
            "spark.driver.cores": "1",
            "spark.driver.memory": "1g",
            "spark.executor.memory": "2g",
            "spark.shuffle.sort.bypassMergeThreshold": "8"
        },
        "packages": [
            "com.databricks:spark-csv_2.10:1.3.0"
        ]
    }
}' "http://localhost:3000/v1/contexts/mycontext"
```

The response will look similar to the following:

```json
{
  "context": "mycontext",
  "configuration": {
    "createSQLContext": true,
    "properties": {
      "spark.scheduler.allocation.file": "/Users/Development/Git-Projects/spark-server/config/spark_pools.xml",
      "spark.executor.memory": "2g",
      "spark.rootLogger.level": "INFO",
      "spark.app.id": "local-1456394488990",
      "spark.scheduler.mode": "FAIR",
      "spark.shuffle.sort.bypassMergeThreshold": "8",
      "spark.executor.id": "driver",
      "spark.broadcast.port": "10033",
      "spark.driver.memory": "1g",
      "spark.driver.port": "10062",
      "spark.scheduler.pool": "default",
      "spark.blockManager.port": "10003",
      "spark.driver.host": "192.168.0.183",
      "spark.ui.port": "10046",
      "spark.app.name": "mycontext",
      "spark.jars": "file:/Users/Development/.ivy2/jars/com.databricks_spark-csv_2.10-1.3.0.jar,file:/Users/Development/.ivy2/jars/org.apache.commons_commons-csv-1.1.jar,file:/Users/Development/.ivy2/jars/com.univocity_univocity-parsers-1.5.1.jar",
      "spark.fileserver.port": "10008",
      "spark.master": "local[*]",
      "spark.submit.deployMode": "client",
      "spark.driver.cores": "1",
      "spark.externalBlockStore.folderName": "spark-5b49f381-a661-4a3a-84a3-27986176d193"
    },
    "packages": [
      "com.databricks:spark-csv_2.10:1.3.0"
    ],
    "ports": [
      10003,
      10033,
      10062,
      10008,
      10046
    ],
    "applicationName": "mycontext"
  },
  "sparkWebUi": "http://192.168.0.183:10046",
  "sessions": 0
}
```

### Creating a session

A [new session](http://spark-server.github.io/api/index.html#contexts__contextName__sessions_post) for the usage of the before established context can be created like this: 

```
curl -X POST -H "Content-Type: application/json" "http://localhost:3000/v1/contexts/mycontext/sessions"
```

The response will look similar to the following:

```json
{
  "context": "mycontext",
  "session": {
    "id": "a5f2498aef12d8562a84c447105ecb4f0ab27c14",
    "startedTimestamp": 1456394976204,
    "type": "spark",
    "status": "IDLE",
    "currentlyRunning": 0,
    "statements": []
  }
}
```

The returned `session.id` is the used to issue statements (next example).

### Execution of statements

Now we'll try [execute some statements](http://spark-server.github.io/api/index.html#contexts__contextName__sessions__sessionId__statements_post). To load a provided sample csv file (under `fileCache/people.csv`) as a Spark `DataFrame` do the following:

```bash
curl -X POST -H "Content-Type: application/json" -d '{
    "code": [
      "var people = sqlContext.read().format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').option('delimiter', ',').load(getFileById('people.csv'))"
    ],
 	"return": "people.schema()"
}' "http://localhost:3000/v1/contexts/mycontext/sessions/a5f2498aef12d8562a84c447105ecb4f0ab27c14/statements"
```

This will return the schema information of the newly created `people` DataFrame:

```json
{
  "context": "mycontext",
  "sessionId": "a5f2498aef12d8562a84c447105ecb4f0ab27c14",
  "result": [
      {
        "name": "id",
        "type": "integer",
        "nullable": true,
        "metadata": {}
      },
      {
        "name": "first_name",
        "type": "string",
        "nullable": true,
        "metadata": {}
      },
      {
        "name": "last_name",
        "type": "string",
        "nullable": true,
        "metadata": {}
      },
      {
        "name": "gender",
        "type": "string",
        "nullable": true,
        "metadata": {}
      },
      {
        "name": "latitude",
        "type": "double",
        "nullable": true,
        "metadata": {}
      },
      {
        "name": "longitude",
        "type": "double",
        "nullable": true,
        "metadata": {}
      },
      {
        "name": "country",
        "type": "string",
        "nullable": true,
        "metadata": {}
      }
    ]
  }
}
```

Now, we want to retrieve the data from the `DataFrame`:

```bash
curl -X POST -H "Content-Type: application/json" -d '{
 	"return": "people"
}' "http://localhost:3000/v1/contexts/mycontext/sessions/a5f2498aef12d8562a84c447105ecb4f0ab27c14/statements"
```

which will return (shortened) 

```json
{
  "context": "mycontext",
  "sessionId": "0d3f34b951f9ef5129cb3f5870d9c70e3b9a2115",
  "result": [
    {
      "id": 1,
      "first_name": "Carol",
      "last_name": "Stanley",
      "gender": "Female",
      "latitude": 37.63667,
      "longitude": 127.21417,
      "country": "South Korea"
    },
    ...
  ]
}
```

Let's try to count the females within the `people.csv` DataFrame:

```bash
curl -X POST -H "Content-Type: application/json" -d '{
 	"return": "people.filter('gender = \"Female\"').count()"
}' "http://localhost:3000/v1/contexts/mycontext/sessions/0d3f34b951f9ef5129cb3f5870d9c70e3b9a2115/statements"
```

which returns

```json
{
  "context": "mycontext",
  "sessionId": "0d3f34b951f9ef5129cb3f5870d9c70e3b9a2115",
  "result": 482
}
``` 

Now, we want to use the promisified [actions](https://spark.apache.org/docs/latest/programming-guide.html#actions) together with the async/await syntax to enable parallel execution (i.e. when the context is used by multiple sessions in parallel, so that the execution is non-blocking to others):

```bash
curl -X POST -H "Content-Type: application/json" -d '{
    "code": [
        "var cars = await sqlContext.read().format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').option('delimiter', ',').load(getFileById('cars.csv')).toJSON().collectPromised()"
    ],
    "return": "cars"
}' "http://localhost:3000/v1/contexts/mycontext/sessions/0d3f34b951f9ef5129cb3f5870d9c70e3b9a2115/statements"
```

which returns

```json
{
  "context": "mycontext",
  "sessionId": "0d3f34b951f9ef5129cb3f5870d9c70e3b9a2115",
  "result": [
    {
      "year": 2012,
      "make": "Tesla",
      "model": "S",
      "comment": "No comment",
      "blank": ""
    },
    {
      "year": 1997,
      "make": "Ford",
      "model": "E350",
      "comment": "Go get one now they are going fast",
      "blank": ""
    },
    {
      "year": 2015,
      "make": "Chevy",
      "model": "Volt"
    }
  ]
}
```

Also, we can try to mix JS with Spark code. Let's fetch the url stats of `www.google.com` from the Facebook Graph API asynchronously and calculate the comment-to-shares ratio with Spark SQL:

```bash
{
    "code": [
        "var googleStats = sqlContext.read().json(await getRemoteJSON('http://graph.facebook.com/?id=http://www.google.com'))",
        "googleStats.registerTempTable('gs')",
        "var commentShareRatio = sqlContext.sql('select comments/shares*100 as commentShareRatio from gs')"
    ],
    "return": "commentShareRatio"
}
```

returns

```json
{
  "context": "my",
  "sessionId": "0d3f34b951f9ef5129cb3f5870d9c70e3b9a2115",
  "result": [
    {
      "commentShareRatio": 0.007931660522753256
    }
  ]
}
```

## Thanks

Thanks to [henridf](https://github.com/henridf) and the [apache-spark-node](https://github.com/henridf/apache-spark-node) project for the inspiration.