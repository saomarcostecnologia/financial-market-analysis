# src/infrastructure/services/aws_observability_service.py
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Optional

import boto3

from src.domain.interfaces.repositories import ObservabilityService
from src.infrastructure.config.settings import Settings


class AWSObservabilityService(ObservabilityService):
    """Implementation of ObservabilityService using AWS CloudWatch."""
    
    def __init__(self, settings: Settings, cloudwatch_client=None, logs_client=None):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self.cloudwatch_client = cloudwatch_client or boto3.client('cloudwatch', region_name=settings.AWS_REGION)
        self.logs_client = logs_client or boto3.client('logs', region_name=settings.AWS_REGION)
        self.service_name = settings.PROJECT_NAME
        self.environment = settings.ENVIRONMENT
        
        # Ensure log group exists
        self._ensure_log_group_exists()
    
    def _ensure_log_group_exists(self):
        """Ensure CloudWatch Logs log group exists."""
        log_group_name = f"/aws/lambda/{self.service_name}-{self.environment}"
        try:
            self.logs_client.create_log_group(logGroupName=log_group_name)
            self.logger.info(f"Created log group: {log_group_name}")
        except Exception as e:
            # Ignore ResourceAlreadyExistsException
            self.logger.debug(f"Log group may already exist: {log_group_name} - {str(e)}")
        
    def log_event(self, event_type: str, event_data: Dict[str, Any]) -> None:
        """Log an event for observability."""
        try:
            # Generate a unique trace ID if not already present
            if 'trace_id' not in event_data:
                event_data['trace_id'] = str(uuid.uuid4())
            
            # Add timestamp and event type
            log_data = {
                'timestamp': datetime.now().isoformat(),
                'event_type': event_type,
                'environment': self.environment,
                'service': self.service_name,
                'data': event_data
            }
            
            # Log locally as well
            self.logger.info(f"Event: {event_type} - {json.dumps(log_data)}")
            
            # In development mode, we might not want to send to CloudWatch
            if self.environment == "development":
                return
                
            # Send to CloudWatch Logs
            log_group_name = f"/aws/lambda/{self.service_name}-{self.environment}"
            log_stream_name = f"{datetime.now().strftime('%Y/%m/%d')}/{event_type}"
            
            try:
                # Try to create the log stream if it doesn't exist
                self.logs_client.create_log_stream(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name
                )
            except Exception:
                # Ignore ResourceAlreadyExistsException
                pass
            
            # Put log event
            try:
                self.logs_client.put_log_events(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name,
                    logEvents=[
                        {
                            'timestamp': int(time.time() * 1000),
                            'message': json.dumps(log_data)
                        }
                    ]
                )
            except Exception as e:
                self.logger.warning(f"Could not send log to CloudWatch: {str(e)}")
                
        except Exception as e:
            self.logger.error(f"Error logging event: {str(e)}")
    
    def track_metric(self, metric_name: str, value: float, dimensions: Dict[str, str] = None) -> None:
        """Track a metric for monitoring."""
        try:
            # Prepare CloudWatch metric data
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'None',  # Default unit, can be customized if needed
                'Dimensions': [
                    {
                        'Name': 'Environment',
                        'Value': self.environment
                    },
                    {
                        'Name': 'Service',
                        'Value': self.service_name
                    }
                ]
            }
            
            # Add custom dimensions if provided
            if dimensions:
                for name, value in dimensions.items():
                    metric_data['Dimensions'].append({
                        'Name': name,
                        'Value': value
                    })
            
            # Log locally as well
            self.logger.info(f"Metric: {metric_name} = {value} - Dimensions: {dimensions}")
            
            # In development mode, we might not want to send to CloudWatch
            if self.environment == "development":
                return
                
            # Push metric to CloudWatch
            try:
                self.cloudwatch_client.put_metric_data(
                    Namespace=f"{self.service_name}/{self.environment}",
                    MetricData=[metric_data]
                )
            except Exception as e:
                self.logger.warning(f"Could not send metric to CloudWatch: {str(e)}")
                
        except Exception as e:
            self.logger.error(f"Error tracking metric: {str(e)}")
    
    def start_trace(self, trace_name: str, trace_data: Dict[str, Any] = None) -> str:
        """Start a new trace and return the trace ID."""
        trace_id = str(uuid.uuid4())
        trace_info = {
            'trace_id': trace_id,
            'trace_name': trace_name,
            'start_time': datetime.now().isoformat(),
            'status': 'started'
        }
        
        if trace_data:
            trace_info.update(trace_data)
        
        self.log_event(f"trace_start_{trace_name}", trace_info)
        return trace_id
    
    def end_trace(self, trace_id: str, success: bool = True, result_data: Dict[str, Any] = None) -> None:
        """End a trace with the given trace ID."""
        trace_info = {
            'trace_id': trace_id,
            'end_time': datetime.now().isoformat(),
            'status': 'success' if success else 'failed'
        }
        
        if result_data:
            trace_info.update(result_data)
        
        self.log_event(f"trace_end_{trace_id}", trace_info)