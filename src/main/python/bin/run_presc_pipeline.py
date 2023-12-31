### Import all the necessary Modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
import sys
from loguru import logger
import os
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_Prescribers
from subprocess import Popen, PIPE

### Load the Logging Configuration File
#logging.config.fileConfig(fname='../util/logging_to_file.conf')

logger.add(
    "presc_run_pipeline_{time}.log",
    format="{time} - {level} - {message}",
    rotation="10 MB",
    lelvel="INFO"
)


def main():
    try:
        logger.info("main() is started ...")
        ### Get Spark Object
        spark = get_spark_object(gav.envn,gav.appName)
        # Validate Spark Object
        get_curr_date(spark)

        ### Initiate presc_run_data_ingest Script
        # Load the City File
        file_dir="Dimension_File"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir.encode('utf-8')], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        out = out.decode('utf-8')
        if 'parquet' in out:
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out:
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema

        df_city = load_files(spark = spark, file_dir = file_dir, file_format =file_format , header =header, inferSchema = inferSchema)

        # Load the Prescriber Fact File
        file_dir="Fact_File"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir.encode('utf-8')], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        out = out.decode('utf-8')
        if 'parquet' in out:
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out:
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema

        df_fact = load_files(spark = spark, file_dir = file_dir, file_format =file_format , header =header, inferSchema = inferSchema)

        ### Validate run_data_ingest script for city Dimension & Prescriber Fact dataframe
        df_count(df_city,'df_city')
        df_top10_rec(df_city,'df_city')

        df_count(df_fact,'df_fact')
        df_top10_rec(df_fact,'df_fact')

        ### Initiate presc_run_data_preprocessing Script
        ## Perform data Cleaning Operations for df_city and df_fact
        df_city_sel,df_fact_sel = perform_data_clean(df_city,df_fact)

        #Validation for df_city and df_fact
        df_top10_rec(df_city_sel,'df_city_sel')
        df_top10_rec(df_fact_sel,'df_fact_sel')
        df_print_schema(df_fact_sel,'df_fact_sel')

        ### Initiate presc_run_data_transform Script
        df_city_final = city_report(df_city_sel,df_fact_sel)
        df_presc_final = top_5_Prescribers(df_fact_sel)

        #Validation for df_city_final
        df_top10_rec(df_city_final,'df_city_final')
        df_print_schema(df_city_final,'df_city_final')
        df_top10_rec(df_presc_final,'df_presc_final')
        df_print_schema(df_presc_final,'df_presc_final')

        # Set up logger Configuration Mechanism

        ### Initiate run_data_extraction Script
        # Validate
        # Set up Error Handling
        # Set up logger Configuration Mechanism

        ### End of Application Part 1
        logger.info("presc_run_pipeline.py is Completed.")

    except Exception as exp:
        logger.exception("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
              "and fix it: {error} " , error=exp)
        sys.exit(1)

if __name__ == "__main__" :
    logger.info("run_presc_pipeline is Started ...")
    main()
