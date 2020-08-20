## Setup
Note: This process assumes you have python3 installed as well as PostgreSQL. If you do not have these installed, run `chmod +x ./install.sh && . ./install.sh`

1. Create the virtual environment by running:
```
pip3 install --upgrade pip
python3 -m venv .venv
source ./.venv/bin/activate
pip install -r requirements.txt
```
## Running the job
1. Open `./download-files.sh` and update the Kaggle environment variables. If you don't have an API key go to your account page to generate one.
2. Mark it as executable and run it: `chmod +x ./download-files.sh && . ./download-files.sh`
3. Run `python3 main.py` to populate the PostgreSQL table `movies`.
4. Once the python program is complete (~12 minutes), you can check PostgreSQL via terminal:
```
 psql -p5432 "postgres"
 postgres=# \dt
           List of relations
 Schema |    Name    | Type  |  Owner   
--------+------------+-------+----------
 public | movies     | table | admin

postgres=# select count(1) from movies;
 count 
-------
  1000
(1 row)
```

### Tools chosen & testing
* I chose to implement this solution using Python/Spark as it's a good fit for general ETL tasks as well as data exploration.
* `Koalas` is a module created by Databricks which enables the use of Pandas APIs on Apache Spark which is good for big datasets such as the Wikipedia one.
* Since both dataframes are static we can test they've been loaded correctly by asserting their shape & length using `pytest`.
* Breaking this process into methods means each element can be unit-tested.
* To run the tests: `pytest tests.py`
### Future improvements
* Dockerise solution to ensure solution can run on any machine.
* Further cleansing/transformation of the Wikipedia dataset: I noticed when loading this data that many wikipedia URLs are not a 1-1 match of the films which makes joining on the movie title problematic. In addition, some films titles do match a Wikipedia page URL exactly but it's the wrong Wikiepedia entry. For example, there is a film titled "Abraham Lincoln" but the matched URL will take you to the late President's Wikipedia [page](https://en.wikipedia.org/wiki/Abraham_Lincoln), not that of the [film](https://en.wikipedia.org/wiki/Abraham_Lincoln_(1930_film)) from 1930. I would solve this by writing a regular expression to remove the additional information from the URL and store that information in an additional column. That column could then be used to determine which URL we should be joining on to get the correct URL.    
* Deserialize the `production_companies` column as it's not ideal to store raw JSON objects in a relational database!