class Config:
    JOB_FLOW_OVERRIDES = {
        "Name": "spark-jose",
        "ReleaseLabel": "emr-5.28.0",
        "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
        "Configurations": [
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                    }
                ],
            }
        ],
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Master node",
                    "Market": "SPOT",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core - 2",
                    "Market": "SPOT", # Spot instances are a "use as available" instances
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False, # this lets us programmatically terminate the cluster
        },
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole"
    }

    SPARK_STEPS = [
        {
            "Name": "Transform data",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    "--packages",
                    "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                    "s3a://{{ params.BUCKET_NAME }}/{{ params.transform_script }}"
                ],
            },
        },
        {
            "Name": "Data quality",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    "s3a://{{ params.BUCKET_NAME }}/{{ params.quality_script }}"
                ],
            },
        }
    ]