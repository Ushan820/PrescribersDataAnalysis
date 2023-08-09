from loguru import logger

# Load the Logging Configuration File
logger.add(
    "presc_run_pipeline_{time}.log",
    format="{time} - {level} - {message}",
    rotation="10 MB"
)

# Get the custom Logger from Configuration File
logger.info(__name__)

def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.info("load_files() is Started ...")
        if file_format == 'parquet' :
            df = spark. \
                read. \
                format(file_format). \
                load(file_dir)
        elif file_format == 'csv' :
            df = spark. \
                read. \
                format(file_format). \
                options(header=header). \
                options(inferSchema=inferSchema). \
                load(file_dir)
    except Exception as exp:
        logger.exception("Error in the method - load_files(). Please check the Stack Trace : {error} ", str(exp))
        raise
    else:
        logger.info("The input File {file_dir} is loaded to the data frame. The load_files() Function is completed.")
    return df