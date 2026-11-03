import os
import logging
from postoffice.data_plane import DataPlane
from postoffice.logging_config import configure_structured_logging

if os.environ.get("POSTOFFICE_LOG_FORMAT", "").lower() == "json":
    configure_structured_logging(
        level=logging.INFO,
        engine=os.environ.get("POSTOFFICE_WORKER_ENGINE", "python"),
        worker_id=os.environ.get("POSTOFFICE_WORKER_ID"),
    )
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting PostOffice Data Plane Worker...")
    worker = DataPlane(
        redis_host=os.environ.get("REDIS_HOST", "localhost"),
        redis_port=int(os.environ.get("REDIS_PORT", "6379")),
        redis_password=os.environ.get("REDIS_PASSWORD") or None,
        redis_tls=os.environ.get("REDIS_TLS") == "1",
    )
    worker.run()

if __name__ == "__main__":
    main()
