from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_s3_deployment as s3deploy,
    aws_glue as glue
)
from constructs import Construct

class GlueAthenaExampleStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        raw_data_bucket = s3.Bucket(self, 
                                "raw-data-bucket",
                                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                removal_policy=RemovalPolicy.RETAIN)
        
        stage_data_bucket = s3.Bucket(self, 
                        "stage-data-bucket",
                        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                        removal_policy=RemovalPolicy.RETAIN)

        #data_example_deployment = s3deploy.BucketDeployment(self, "deploy-data",
        #    sources=[s3deploy.Source.asset(r'assets\data')],
        #    destination_bucket=data_bucket,
        #    destination_key_prefix='raw'
        #)

        data_script_deployment = s3deploy.BucketDeployment(self, "deploy-data",
            sources=[s3deploy.Source.asset(r'assets\glue_script')],
            destination_bucket=data_bucket,
            destination_key_prefix='glue-assets/scripts'
        )

        glue_role = iam.Role(self, "glue-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Glue etl job role"
        )

        self.glue_policy = iam.Policy(
                self, 
                'glue-policy',
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:ListBucket"
                        ],
                        resources=[
                            f"arn:aws:s3:::{data_bucket.bucket_name}"
                        ],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:PutObject",
                            "s3:GetObject"
                        ],
                        resources=[
                            f"arn:aws:s3:::{data_bucket.bucket_name}/*",
                        ],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "glue:CreateTable",
                            "glue:GetTables",
                            "glue:GetDatabases",
                            "glue:GetTable"
                        ],
                        resources=[
                            f"arn:aws:glue:*:{self.account}:table/*/*",
                            f"arn:aws:glue:*:{self.account}:catalog",
                            f"arn:aws:glue:*:{self.account}:database/*"
                        ],
                    )
                ],
                roles=[glue_role],
            )
        
        glue_etl_job = glue.CfnJob(self, "dae-glue-job",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{data_bucket.bucket_name}/glue-assets/scripts/job.py"
            ),
            name="dae-glue-job",
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=5,
            role=glue_role.role_arn,
            default_arguments={
                "--enable-metrics":	"true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{data_bucket.bucket_name}/glue-assets/sparkHistoryLogs/",
                "--enable-job-insights": "false",
                "--enable-glue-datacatalog": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-bookmark-option": "job-bookmark-disable",
                "--job-language": "python",
                "--TempDir": f"s3://{data_bucket.bucket_name}/glue-assets/temporary/",
                "--DB_TARGET": "default",
                "--TB_TARGET": "chicago_crimes",
                "--DATA_BUCKET": data_bucket.bucket_name,
            })
