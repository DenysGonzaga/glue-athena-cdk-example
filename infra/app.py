#!/usr/bin/env python3
import os

import aws_cdk as cdk

from glue_athena_infra.components import GlueAthenaExampleStack

app = cdk.App()
GlueAthenaExampleStack(app, "glue-athena-example")

app.synth()
