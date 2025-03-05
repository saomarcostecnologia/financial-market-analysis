#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { FinancialMarketStack } from '../lib/cdk-new-stack';

const app = new cdk.App();
new FinancialMarketStack(app, 'FinancialMarketStack');