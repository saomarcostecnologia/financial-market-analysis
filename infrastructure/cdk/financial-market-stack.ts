// infrastructure/cdk/lib/financial-market-stack.ts
import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as kinesisfirehose from 'aws-cdk-lib/aws-kinesisfirehose';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cloudtrail from 'aws-cdk-lib/aws-cloudtrail';
import { Construct } from 'constructs';

export class FinancialMarketStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 Bucket for Raw Data
    const rawDataBucket = new s3.Bucket(this, 'RawDataBucket', {
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          id: 'TransitionToIntelligentTiering',
          transitions: [
            {
              storageClass: s3.StorageClass.INTELLIGENT_TIERING,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
        },
      ],
    });

    // S3 Bucket for Processed Data
    const processedDataBucket = new s3.Bucket(this, 'ProcessedDataBucket', {
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          id: 'TransitionToIntelligentTiering',
          transitions: [
            {
              storageClass: s3.StorageClass.INTELLIGENT_TIERING,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
        },
      ],
    });

    // DynamoDB Table for Stocks
    const stocksTable = new dynamodb.Table(this, 'StocksTable', {
      partitionKey: { name: 'ticker', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      pointInTimeRecovery: true,
    });

    // DynamoDB Table for Stock Prices
    const pricesTable = new dynamodb.Table(this, 'PricesTable', {
      partitionKey: { name: 'ticker', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'timestamp', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      pointInTimeRecovery: true,
    });

    // Kinesis Data Stream for Real-time Data
    const stockDataStream = new kinesis.Stream(this, 'StockDataStream', {
      streamName: 'financial-market-stock-data-stream',
      encryption: kinesis.StreamEncryption.MANAGED,
      retentionPeriod: cdk.Duration.hours(24),
      shardCount: 1,
    });

    // Glue Database
    const glueDatabase = new glue.CfnDatabase(this, 'FinancialMarketDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: 'financial_market_db',
        description: 'Database for Financial Market Analysis',
      },
    });

    // Glue IAM Role
    const glueRole = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });

    // Grant permissions to Glue Role
    rawDataBucket.grantReadWrite(glueRole);
    processedDataBucket.grantReadWrite(glueRole);

    // Glue Job for Stock Data ETL
    const stockDataEtlJob = new glue.CfnJob(this, 'StockDataEtlJob', {
      name: 'financial-market-stock-data-etl',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `s3://${rawDataBucket.bucketName}/scripts/stock_data_ingestor.py`,
      },
      defaultArguments: {
        '--job-language': 'python',
        '--enable-continuous-cloudwatch-log': 'true',
        '--enable-metrics': 'true',
        '--target_bucket': processedDataBucket.bucketName,
        '--target_prefix': 'processed/stocks',
      },
      executionProperty: {
        maxConcurrentRuns: 2,
      },
      maxRetries: 2,
      timeout: 60,
      numberOfWorkers: 2,
      workerType: 'G.1X',
      glueVersion: '3.0',
    });

    // Lambda Function for Real-time Processing
    const realTimeProcessorLambda = new lambda.Function(this, 'RealTimeProcessor', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'lambda_function.lambda_handler',
      code: lambda.Code.fromAsset('../src/lambda/real_time_processor'),
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      environment: {
        OUTPUT_BUCKET: processedDataBucket.bucketName,
        SENSITIVE_FIELDS: 'name,sector',
      },
    });

    // Grant necessary permissions to Lambda
    processedDataBucket.grantReadWrite(realTimeProcessorLambda);
    stockDataStream.grantRead(realTimeProcessorLambda);
    stockDataStream.grantWrite(realTimeProcessorLambda);

    // Lambda can also publish metrics
    realTimeProcessorLambda.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['cloudwatch:PutMetricData'],
        resources: ['*'],
      })
    );

    // Event Rule to Trigger Glue Job Daily
    const dailySchedule = new events.Rule(this, 'DailyStockDataRule', {
      schedule: events.Schedule.cron({ minute: '0', hour: '0' }),
    });

    // Step Function for Orchestrating the Data Pipeline
    const submitGlueJob = new tasks.GlueStartJobRun(this, 'Submit Glue Job', {
      glueJobName: stockDataEtlJob.name as string,
      arguments: stepfunctions.TaskInput.fromObject({
        '--tickers': '${$.tickers}',
        '--start_date': '${$.start_date}',
        '--end_date': '${$.end_date}',
      }),
      resultPath: '$.glueJobResult',
    });

    const processRealTimeData = new tasks.LambdaInvoke(this, 'Process Real-time Data', {
      lambdaFunction: realTimeProcessorLambda,
      payloadResponseOnly: true,
      payload: stepfunctions.TaskInput.fromObject({
        tickers: '${$.tickers}',
      }),
      resultPath: '$.realTimeResult',
    });

    const waitForGlueJob = new stepfunctions.Wait(this, 'Wait for Glue Job', {
      time: stepfunctions.WaitTime.duration(cdk.Duration.minutes(5)),
    });

    const checkGlueJobStatus = new tasks.GlueGetJobRun(this, 'Check Glue Job Status', {
      jobName: stockDataEtlJob.name as string,
      runId: stepfunctions.JsonPath.stringAt('$.glueJobResult.JobRunId'),
      resultPath: '$.glueJobStatus',
    });

    const jobComplete = new stepfunctions.Choice(this, 'Is Job Complete?');
    const jobSucceeded = new stepfunctions.Condition(this, 'Job Succeeded', {
      variable: '$.glueJobStatus.JobRun.JobRunState',
      stringEquals: 'SUCCEEDED',
    });
    const jobFailed = new stepfunctions.Condition(this, 'Job Failed', {
      variable: '$.glueJobStatus.JobRun.JobRunState',
      stringEquals: 'FAILED',
    });

    const dataPipelineDefinition = submitGlueJob
      .next(waitForGlueJob)
      .next(checkGlueJobStatus)
      .next(
        jobComplete
          .when(jobSucceeded, processRealTimeData)
          .when(jobFailed, new stepfunctions.Fail(this, 'Data Pipeline Failed', {
            cause: 'Glue Job Failed',
            error: 'GlueJobFailed',
          }))
          .otherwise(waitForGlueJob)
      );

    const dataPipeline = new stepfunctions.StateMachine(this, 'StockDataPipeline', {
      definition: dataPipelineDefinition,
      timeout: cdk.Duration.hours(2),
    });

    dailySchedule.addTarget(new targets.SfnStateMachine(dataPipeline, {
      input: events.RuleTargetInput.fromObject({
        tickers: 'AAPL,MSFT,GOOGL,AMZN,FB',
        start_date: events.EventField.fromPath('$.time'),
        end_date: events.EventField.fromPath('$.time'),
      }),
    }));

    // CloudWatch Dashboard for Monitoring
    const dashboard = new cloudwatch.Dashboard(this, 'FinancialMarketDashboard', {
      dashboardName: 'FinancialMarketAnalytics',
    });

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Kinesis Data Stream Metrics',
        left: [
          new cloudwatch.Metric({
            namespace: 'AWS/Kinesis',
            metricName: 'IncomingRecords',
            dimensionsMap: {
              StreamName: stockDataStream.streamName,
            },
            statistic: 'Sum',
            period: cdk.Duration.minutes(1),
          }),
          new cloudwatch.Metric({
            namespace: 'AWS/Kinesis',
            metricName: 'ReadProvisionedThroughputExceeded',
            dimensionsMap: {
              StreamName: stockDataStream.streamName,
            },
            statistic: 'Sum',
            period: cdk.Duration.minutes(1),
          }),
        ],
      }),
      new cloudwatch.GraphWidget({
        title: 'Lambda Function Metrics',
        left: [
          new cloudwatch.Metric({
            namespace: 'AWS/Lambda',
            metricName: 'Invocations',
            dimensionsMap: {
              FunctionName: realTimeProcessorLambda.functionName,
            },
            statistic: 'Sum',
            period: cdk.Duration.minutes(1),
          }),
          new cloudwatch.Metric({
            namespace: 'AWS/Lambda',
            metricName: 'Errors',
            dimensionsMap: {
              FunctionName: realTimeProcessorLambda.functionName,
            },
            statistic: 'Sum',
            period: cdk.Duration.minutes(1),
          }),
        ],
      }),
      new cloudwatch.GraphWidget({
        title: 'Custom Application Metrics',
        left: [
          new cloudwatch.Metric({
            namespace: 'FinancialDataPipeline',
            metricName: 'RecordsProcessed',
            dimensionsMap: {
              Function: 'RealTimeProcessor',
            },
            statistic: 'Sum',
            period: cdk.Duration.minutes(5),
          }),
          new cloudwatch.Metric({
            namespace: 'FinancialDataPipeline',
            metricName: 'ProcessingErrors',
            dimensionsMap: {
              Function: 'RealTimeProcessor',
            },
            statistic: 'Sum',
            period: cdk.Duration.minutes(5),
          }),
        ],
      })
    );

    // CloudTrail for Audit Logging
    const trail = new cloudtrail.Trail(this, 'FinancialMarketTrail', {
      sendToCloudWatchLogs: true,
      managementEvents: cloudtrail.ReadWriteType.ALL,
      bucket: new s3.Bucket(this, 'CloudTrailBucket', {
        versioned: true,
        encryption: s3.BucketEncryption.S3_MANAGED,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      }),
    });

    // Output the resource names
    new cdk.CfnOutput(this, 'RawDataBucketName', {
      value: rawDataBucket.bucketName,
      description: 'Name of the S3 bucket for raw data',
    });

    new cdk.CfnOutput(this, 'ProcessedDataBucketName', {
      value: processedDataBucket.bucketName,
      description: 'Name of the S3 bucket for processed data',
    });

    new cdk.CfnOutput(this, 'StocksTableName', {
      value: stocksTable.tableName,
      description: 'Name of the DynamoDB table for stocks',
    });

    new cdk.CfnOutput(this, 'PricesTableName', {
      value: pricesTable.tableName,
      description: 'Name of the DynamoDB table for stock prices',
    });

    new cdk.CfnOutput(this, 'KinesisStreamName', {
      value: stockDataStream.streamName,
      description: 'Name of the Kinesis data stream',
    });

    new cdk.CfnOutput(this, 'GlueDatabaseName', {
      value: 'financial_market_db',
      description: 'Name of the Glue database',
    });

    new cdk.CfnOutput(this, 'RealTimeProcessorLambdaName', {
      value: realTimeProcessorLambda.functionName,
      description: 'Name of the Lambda function for real-time processing',
    });

    new cdk.CfnOutput(this, 'DataPipelineStepFunctionName', {
      value: dataPipeline.stateMachineName,
      description: 'Name of the Step Functions state machine for the data pipeline',
    });
  }
}