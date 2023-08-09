from pyspark.sql import SparkSession
from loguru import logger

# Load the Logging Configuration File
logger.add(
    "presc_run_pipeline_{time}.log",
    format="{time} - {level} - {message}",
    rotation="10 MB"
)

logger.info(__name__)

def get_spark_object(envn,appName ):
    try:
        logger.info("get_spark_object() is started. The '{envn}' envn is used.")
        if envn == 'TEST' :
            master='local'
        else:
            master='yarn'
        spark = SparkSession \
                  .builder \
                  .master(master) \
                  .appName(appName) \
                  .getOrCreate()
    except Exception as exp:
        logger.exception("Error in the method - get_spark_object(). Please check the Stack Trace : {error} ", error=exp)
    else:
        logger.info("Spark Object is created ...")
    return spark



