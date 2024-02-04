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
Also copy the `log_json_path.json` to your bucket.
Ensure that you select the same region as your redshift cluster, by default in `us-west-2`.
Create a `dwh.cfg` file where you provide the aws keys and the location of your just created small files:

```shell
echo "[AWS]" > dwh.cfg
echo "KEY=<KEY>" >> dwh.cfg
echo "SECRET=<SECRET>" >> dwh.cfg
echo "LOG_DATA=s3://<LOG_DATA_PATH>/events.json" >> dwh.cfg
echo "SONG_DATA=s3://<SONG_DATA_PATH>/songs.json" >> dwh.cfg
echo "LOG_SCHEMA_PATH=s3://<LOG_JSONPATH>/log_json_path.json" >> dwh.cfg
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
Here we will take a look at several aspects of the data quality.
I don't add this script to the pipeline directly,
as this are run one time but can be easy adopted given the other existing code.
```shell
python3 data_quality.py
```

This will generate a report under `data-quality-report.md` that you can see [here](data-quality-report.md)

# Business Questions
Finally, lets take a look at some questions that can come up from the business.
To run them, directly execute them in redshift editor.

## What is the most played song?
```sql
SELECT dim_songs.title, dim_artists.name, count(fact_songplays.song_id) as cnt
FROM fact_songplays, dim_songs, dim_artists
WHERE (fact_songplays.song_id = dim_songs.song_id AND fact_songplays.artist_id = dim_artists.artist_id)
GROUP BY dim_songs.title, dim_artists.name
ORDER BY count(fact_songplays.song_id) DESC limit 10
```
result:

| song                                                 | artist         | played |
|------------------------------------------------------|----------------|--------|
| You're The One                                       | Dwight Yoakam  | 37     |
| Supermassive Black Hole (Album Version)              | Muse           | 28     |
| Catch You Baby (Steve Pitron & Max Sanna Radio Edit) | Lonnie Gordon  | 18     |
| Hey Daddy (Daddy's Home)                             | Usher          | 18     |
| The Boy With The Thorn In His Side                   | The Smiths     | 12     |
| Girlfriend In A Coma                                 | The Smiths     | 12     |
| If I Ain't Got You                                   | Alicia Keys    | 9      |
| Fade To Black                                        | Metallica      | 9      |
| From The Ritz To The Rubble                          | Arctic Monkeys | 9      |
| I CAN'T GET STARTED                                  | Ron Carter     | 9      |


