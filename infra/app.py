#!/usr/bin/env python3
import os

import aws_cdk as cdk

from dae.components import DaeStack

app = cdk.App()
DaeStack(app, "D3jsAthenaExampleStack")

app.synth()
