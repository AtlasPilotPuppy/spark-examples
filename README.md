spark-examples
==============

Latest version of spark can be downloaded at http://spark.apache.org/downloads.html

to run the spark shell use:

```./bin/spark-shell```


The pyspark shell can be started using:

```./bin/pyspark```

To run spark with IPython notebook you need to have IPython notebook installed.
It can be installed using :

```
pip install ipython
pip install 'ipython[notebook]'
```

to run pyspark with ipython notebook:

```
IPYTHON_OPTS="notebook --pylab inline --notebook-dir=<directory sto store notebooks>" MASTER=local[6] ./bin/pyspark --executor-memory=6G
```
