Welcome to the RedShift Workspace of Darius.

# Installation
I assume you run this on ubuntu, locally with Python 3.10 (ubuntu default)
```shell
python3 -m venv ./venv
. venv/bin/activate
pip3 install -r requirements.txt
```


# Combine data
In case you want to work with the entire dataset, I wrote a script that speeds up the data loading,
after a one time converting.

Download first the data from udacity (this can take some time, depending on your geolocation...).

```shell
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
aws s3 sync s3://udacity-dend .
```

Now you can combine the required data, to only have two files that you upload.
* This speedup the redshift import a lot
* Also increase the speed of iterating on the solution, as this was before by far the slowest part.

```shell
python3 combine.py
```

This will create two files that you can use now to upload into a s3 bucket.
Ensure that you select the same region as your redshift cluster, by default in `us-west-2`.
Create a `dwh.cfg` file where you provide the aws keys and the location of your just created small files:

```shell
echo "[AWS]" > dwh.cfg
echo "KEY=<KEY>" >> dwh.cfg
echo "SECRET=<SECRET>" >> dwh.cfg
echo "LOG_DATA=s3://<LOG_DATA_PATH>/events.json" >> dwh.cfg
echo "SONG_DATA=s3://<SONG_DATA_PATH>/songs.json" >> dwh.cfg
```


# Setup Redshift
Now we will create a redshift instance with the required roles and s3 permissions:

```shell
python3 create_redshift.py
```

This script will wait (via polling) until the redshift is up and running.

# Setup Tables
After we have a running redshift, we can now create the required tables.
```shell
python3 create_tables.py
```

# Setup ELT
And after we have the tables, we can fill the tables.
```shell
python3 etl.py
```

# Quality Checks