
==============

Latest version of spark can be downloaded at http://spark.apache.org/downloads.html

to run the spark shell use:

```
./bin/spark-shell
```


The pyspark shell can be started using:

```
./bin/pyspark
```

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

## Running examples with provided docker container

```
#Pull image from docker hub
sudo docker pull anantasty/ubuntu_spark_ipython:latest

#or load from disk
sudo docker load < ubuntu_spark_ipython.tar

# Find image id using 

sudo docker images

# Run Image using
# -v arg takes local path and mounts it to path on container
# eg. -v ~/spark-examples:ipython will mount ~/spark-examples
# to /ipython on container

sudo docker run -i -t -h sandbox -v $(pwd):/ipython -d <IMAGE_ID> -d

# if you want to have ipython run on localhost use

sudo docker run -i -t -h sandbox -p 8888:8888 -v $(pwd):/ipython -d <IMAGE_ID> -d


# Upload files from repo to HDFS
# Step 1 get container id
sudo docker ps

#Step 2 log into container

sudo docker exec -it <container_id> /bin/bash

#Step 3 Run upload to HDFS

cd /ipython
hadoop fs -put data /user/

```
