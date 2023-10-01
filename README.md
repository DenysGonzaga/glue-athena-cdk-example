# Simple AWS Glue, Athena and CDK Example

A walkthrough to use AWS Glue/Spark Job, s3 and Athena.

Throughout the  `infra` folder has a Python/CDK project to create everything that you need to run this example.

## How To

### Requisites

* AWS Account
* AWS CLI v2
* Python 3.8+
* CDK version 2.89.0

## Architecture Design

![design](assets/design/glue-athena-example-design.jpg?raw=true)

## Example Setup

1. Configure your AWS Credentials. 
2. Browse to `infra` folder:
    ```bash
    $ cd glue-athena-cdk-example/glue 
    ```
3. Create and activate a virtual env:
    ```bash
    $ python3 -m venv .venv
    $ source .venv/bin/activate
    $ pip3 install -r requirements.txt
    ```
4. Bootstrap CDK (Optional if you already did):
    ```bash
    $ cdk bootstrap
    ```
5. Synth and deploy stack
    ```bash
    $ cdk synth
    $ cdk deploy
    ```

## Resources

* More information about [CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) setup.
* Whether you don't know how to configure AWS CLI, check [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
* 2023 Chicago's Crime dataset was downloaded from [here](https://data.cityofchicago.org/Public-Safety/Crimes-2023/xguy-4ndq).