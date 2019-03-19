# Scraping Poems with Spark and EMR

With this upgraded scraper, the poem classifier dataset can be scraped using AWS EMR in under half an hour for less than $1.

### Files:
1. `pyspark-scraper.ipynb`
2. `bootstrap-py-dependencies.sh`
3. `scrape-poems-pyspark.py`

### Output:
1. `poet_info.csv` - Contains poet names, number of poems, and years lived.
2. `clean_poems.csv` - Contains poems and poet names.

### Steps to Reproduce:
+ Upload `bootstrap-py-dependencies.sh` and `scrape-poems-pyspark.py` to an S3 bucket.
+ Configure your EMR cluster - in the EMR console, select `Create cluster`, then `Go to advanced options`:
    1. Step 1:
        + Check Hadoop, Spark, and Livy.
        + Add two custom steps:
            1.     + JAR Location: command-runner.jar
                + Arguments: `aws s3 cp s3://<your-bucket-here>/scrape-poems-pyspark.py /home/hadoop`
            2.     + JAR Location: command-runner.jar  
                + Arguments: `spark-submit --deploy-mode client --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' /home/hadoop/scrape-poems-pyspark.py <your-bucket-here>`
    2. Step 2:
        + Choose type and number of instances. I used m4.large instances - 1 Master, and 6 Core.
    3. Step 3:
        + Add a custom bootstrap action - Select `bootstrap-py-dependencies.sh` from the bucket you uploaded it to.
    4. Step 4:
        + Leave default settings.
+ Create your cluster!
+ You can monitor the job's progress from the EMR console, or just check your S3 bucket when it's finished. The output files will be there.
