// test/financial-market.test.ts (atualizado para a infraestrutura atual)
import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { FinancialMarketStack } from '../lib/financial-market-stack';

describe('FinancialMarketStack', () => {
  
  test('S3 Bucket Created with Lifecycle Rules', () => {
    const app = new cdk.App({
      context: {
        environment: 'dev'
      }
    });
    
    // WHEN
    const stack = new FinancialMarketStack(app, 'MyTestStack', {
      environment: 'dev'
    });
    
    // THEN
    const template = Template.fromStack(stack);

    // Verifica se o bucket S3 foi criado
    template.hasResourceProperties('AWS::S3::Bucket', {
      BucketName: {
        'Fn::Join': [
          '',
          [
            'financial-market-data-dev-',
            {
              Ref: 'AWS::AccountId'
            }
          ]
        ]
      },
      LifecycleConfiguration: {
        Rules: [
          {
            // Camada bronze
            Status: 'Enabled',
            Transitions: [
              {
                StorageClass: 'STANDARD_IA',
                TransitionInDays: 30
              },
              {
                StorageClass: 'GLACIER',
                TransitionInDays: 90
              }
            ],
            ExpirationInDays: 365,
            Prefix: 'bronze/'
          },
          {
            // Camada prata
            Status: 'Enabled',
            Transitions: [
              {
                StorageClass: 'STANDARD_IA',
                TransitionInDays: 60
              }
            ],
            ExpirationInDays: 730,
            Prefix: 'silver/'
          },
          {
            // Camada ouro
            Status: 'Enabled',
            Transitions: [
              {
                StorageClass: 'STANDARD_IA',
                TransitionInDays: 365
              }
            ],
            ExpirationInDays: 1095,
            Prefix: 'gold/'
          }
        ]
      }
    });
  });

  test('DynamoDB Tables Created with Correct Configuration', () => {
    const app = new cdk.App({
      context: {
        environment: 'dev'
      }
    });
    
    // WHEN
    const stack = new FinancialMarketStack(app, 'MyTestStack', {
      environment: 'dev'
    });
    
    // THEN
    const template = Template.fromStack(stack);

    // Verifica se as tabelas DynamoDB foram criadas
    template.resourceCountIs('AWS::DynamoDB::Table', 2);
    
    // Verifica a tabela de Stocks
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      TableName: 'financial-stocks-dev',
      KeySchema: [
        {
          AttributeName: 'ticker',
          KeyType: 'HASH'
        }
      ],
      BillingMode: 'PAY_PER_REQUEST'
    });
    
    // Verifica a tabela de Preços com índice secundário
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      TableName: 'financial-prices-dev',
      KeySchema: [
        {
          AttributeName: 'ticker',
          KeyType: 'HASH'
        },
        {
          AttributeName: 'timestamp',
          KeyType: 'RANGE'
        }
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: 'DateIndex',
          KeySchema: [
            {
              AttributeName: 'date',
              KeyType: 'HASH'
            }
          ],
          Projection: {
            ProjectionType: 'ALL'
          }
        }
      ]
    });
  });

  test('Lambda Functions Created for All Layers', () => {
    const app = new cdk.App({
      context: {
        environment: 'dev'
      }
    });
    
    // WHEN
    const stack = new FinancialMarketStack(app, 'MyTestStack', {
      environment: 'dev'
    });
    
    // THEN
    const template = Template.fromStack(stack);

    // Verifica se as funções Lambda foram criadas
    template.resourceCountIs('AWS::Lambda::Function', 3);
    
    // Verifica a função Bronze (Extração)
    template.hasResourceProperties('AWS::Lambda::Function', {
      FunctionName: 'financial-market-extractor-dev',
      Runtime: 'python3.11',
      Environment: {
        Variables: {
          ENVIRONMENT: 'dev',
          LAYER: 'bronze'
        }
      }
    });
    
    // Verifica a função Silver (Processamento)
    template.hasResourceProperties('AWS::Lambda::Function', {
      FunctionName: 'financial-market-silver-processor-dev',
      Runtime: 'python3.11',
      Environment: {
        Variables: {
          ENVIRONMENT: 'dev',
          LAYER: 'silver'
        }
      }
    });
    
    // Verifica a função Gold (Análise)
    template.hasResourceProperties('AWS::Lambda::Function', {
      FunctionName: 'financial-market-gold-processor-dev',
      Runtime: 'python3.11',
      Environment: {
        Variables: {
          ENVIRONMENT: 'dev',
          LAYER: 'gold'
        }
      }
    });
  });
  
  test('Step Function Created for Workflow Orchestration', () => {
    const app = new cdk.App({
      context: {
        environment: 'dev'
      }
    });
    
    // WHEN
    const stack = new FinancialMarketStack(app, 'MyTestStack', {
      environment: 'dev'
    });
    
    // THEN
    const template = Template.fromStack(stack);

    // Verifica se o Step Function foi criado
    template.resourceCountIs('AWS::StepFunctions::StateMachine', 1);
    
    template.hasResourceProperties('AWS::StepFunctions::StateMachine', {
      StateMachineName: 'financial-market-workflow-dev'
    });
  });
  
  test('EventBridge Rules Created for Scheduling', () => {
    const app = new cdk.App({
      context: {
        environment: 'dev'
      }
    });
    
    // WHEN
    const stack = new FinancialMarketStack(app, 'MyTestStack', {
      environment: 'dev'
    });
    
    // THEN
    const template = Template.fromStack(stack);

    // Verifica se as regras EventBridge foram criadas
    template.resourceCountIs('AWS::Events::Rule', 3);
  });
  
  test('CloudWatch Alarms and Dashboard Created', () => {
    const app = new cdk.App({
      context: {
        environment: 'dev'
      }
    });
    
    // WHEN
    const stack = new FinancialMarketStack(app, 'MyTestStack', {
      environment: 'dev'
    });
    
    // THEN
    const template = Template.fromStack(stack);

    // Verifica se o Dashboard foi criado
    template.resourceCountIs('AWS::CloudWatch::Dashboard', 1);
    
    // Verifica se os alarmes foram criados
    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      AlarmDescription: {
        'Fn::Join': [
          '',
          [
            'Erros na função Lambda de extração de dados (bronze) - ',
            'dev'
          ]
        ]
      }
    });
  });
});