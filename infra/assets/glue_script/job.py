import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *


args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATA_BUCKET", "DB_TARGET", "TB_TARGET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

raw_data_bucket = args['RAW_DATA_BUCKET']
stage_data_bucket = args['STAGE_DATA_BUCKET']
db_target = args['DB_TARGET']
tb_target = args['TB_TARGET']

crimes_frame = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            f"s3://{raw_data_bucket}/crimes/"
        ],
        "recurse": True,
    },
    transformation_ctx="read_crime_trans",
)

change_schema = ApplyMapping.apply(
    frame=crimes_frame,
    mappings=[
        ("ID", "string", "id", "string"),
        ("Case Number", "string", "case_number", "string"),
        ("Date", "string", "case_date", "string"),
        ("Block", "string", "block", "string"),
        ("IUCR", "string", "iucr", "string"),
        ("Primary Type", "string", "primary_type", "string"),
        ("Description", "string", "description", "string"),
        ("Location Description", "string", "loc_desc", "string"),
        ("Arrest", "string", "arrest", "string"),
        ("Domestic", "string", "docmestic", "string"),
        ("Beat", "string", "beat", "string"),
        ("District", "string", "distruct", "string"),
        ("Ward", "string", "ward", "string"),
        ("Community Area", "string", "c_area", "string"),
        ("FBI Code", "string", "fbi_code", "string"),
        ("X Coordinate", "string", "x_coord", "string"),
        ("Y Coordinate", "string", "y_coord", "string"),
        ("Year", "string", "year", "int"),
        ("Updated On", "string", "update_on", "date"),
        ("Latitude", "string", "latitude", "float"),
        ("Longitude", "string", "longitude", "float"),
        ("Location", "string", "location", "string")
    ],
    transformation_ctx="changing_schema_trans",
)

df = change_schema.toDF()
df = df.withColumn("case_date", to_timestamp("case_date", "MM/dd/yyyy KK:mm:ss a")) 
df = df.withColumn("update_on", to_timestamp("update_on", "MM/dd/yyyy KK:mm:ss a")) 
df_tgt = DynamicFrame.fromDF(df, glueContext , "df_tgt")

destination_sink = glueContext.getSink(
    path=f"s3://{data_bucket}/transformed/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="destination_trans",
)
destination_sink.setCatalogInfo(catalogDatabase=db_target, catalogTableName=tb_target)
destination_sink.setFormat("glueparquet")
destination_sink.writeFrame(df_tgt)
job.commit()
