import os
import logging
from postoffice.data_plane import DataPlane

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting PostOffice Data Plane Worker...")
    redis_host = os.environ.get("REDIS_HOST", "localhost")

    worker = DataPlane(redis_host=redis_host)
    worker.run()

if __name__ == "__main__":
    main()
