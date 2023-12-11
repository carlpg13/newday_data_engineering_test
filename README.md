# NewDay Data Engineering Test
## Project Structure
```bash
spark_app/
 |-- data/
 |-- logs/
 |-- src/
 |   |-- configs/
 |   |-- jobs/
 |   |   |-- etl/
 |   |   |-- utils/
 |   |   |-- main.py
 |-- tests/
```
## Run Guide:
This code can be run with spark-submit from the root directory. It requires to have installed Python, Java, Hadoop and Spark.
spark-submit --master local[*] --py-files src/jobs/main.py src/jobs/main.py

## What else:
This codebase would benefits from more tests. With more time I would test all the functions from src/jobs/etl/transform and from main.py.

In Addition, I could have be added the movie title and genre to the df_top3_movies DataFrame. And also I could format the timestamp to a better format.
