import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { FinancialMarketStack } from '../lib/financial-market-stack';

describe('FinancialMarketStack', () => {
  
  test('S3 Bucket Created', () => {
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
      }
    });
  });

  test('DynamoDB Tables Created', () => {
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
      ]
    });
    
    // Verifica a tabela de Preços
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
      ]
    });
  });

  test('Lambda Function Created', () => {
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

    // Verifica se a função Lambda foi criada
    template.hasResourceProperties('AWS::Lambda::Function', {
      FunctionName: 'financial-market-extractor-dev',
      Runtime: 'python3.9'
    });
  });
});