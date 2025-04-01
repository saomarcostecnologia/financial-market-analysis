#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { FinancialMarketStack } from '../lib/financial-market-stack';

const app = new cdk.App();

// Obter o ambiente a partir de um parâmetro da linha de comando ou contexto
const environment = app.node.tryGetContext('environment') || 'dev';

// Configurações específicas para cada ambiente
const envConfigs = {
  dev: {
    env: { 
      account: process.env.CDK_DEFAULT_ACCOUNT, 
      region: process.env.CDK_DEFAULT_REGION || 'us-east-1' 
    },
    tags: {
      Environment: 'dev',
      Project: 'FinancialMarket',
      CostCenter: 'Research',
      Owner: 'DataTeam'
    }
  },
  staging: {
    env: { 
      account: process.env.CDK_DEFAULT_ACCOUNT, 
      region: process.env.CDK_DEFAULT_REGION || 'us-east-1' 
    },
    tags: {
      Environment: 'staging',
      Project: 'FinancialMarket',
      CostCenter: 'Research',
      Owner: 'DataTeam'
    }
  },
  prod: {
    env: { 
      account: process.env.CDK_DEFAULT_ACCOUNT, 
      region: process.env.CDK_DEFAULT_REGION || 'us-east-1' 
    },
    tags: {
      Environment: 'prod',
      Project: 'FinancialMarket',
      CostCenter: 'Production',
      Owner: 'DataTeam'
    }
  }
};

// Verificar se o ambiente é válido
if (!Object.keys(envConfigs).includes(environment)) {
  throw new Error(
    `Invalid environment: ${environment}. Valid values are: ${Object.keys(envConfigs).join(', ')}`
  );
}

// Criar a stack do Financial Market
const stack = new FinancialMarketStack(app, `FinancialMarketStack-${environment}`, {
  environment,
  ...envConfigs[environment]
});

// Aplicar tags gerais à stack
Object.entries(envConfigs[environment].tags).forEach(([key, value]) => {
  cdk.Tags.of(stack).add(key, value as string);
});

app.synth();