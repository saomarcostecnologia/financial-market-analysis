from src.infrastructure.services.kinesis_service import KinesisService

def start_streaming_job(stream_name):
    # Implementa��o do job de streaming
    kinesis_service = KinesisService(stream_name)
    # L�gica de streaming
