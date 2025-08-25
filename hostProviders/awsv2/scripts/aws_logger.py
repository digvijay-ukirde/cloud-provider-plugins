# Copyright International Business Machines Corp, 2025
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import socket
import sys
from logging.handlers import RotatingFileHandler
from aws_rc_config import get_aws_configs

def set_rc_logger() -> None:
    """Configure the root logger with settings from config and environment."""
    config = get_aws_configs()
    
    # Convert log level string to logging level
    log_level = getattr(logging, config["LogLevel"].upper(), logging.INFO)
    
    # Validate and get log directory
    try:
        log_dirname = os.environ["PRO_LSF_LOGDIR"]
    except KeyError:
        sys.exit("Error: The PRO_LSF_LOGDIR environment variable is not set")
    
    # Get provider name with default fallback
    provider_name = os.getenv("PROVIDER_NAME", "aws")
    
    # Clear existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Configure logging
    log_format = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
    date_format = '%m-%d %H:%M:%S'
    
    if log_dirname:
        host_name = socket.gethostname()
        log_file = f"{log_dirname}/{provider_name}-provider.log.{host_name}"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handler = RotatingFileHandler(
            log_file,
            maxBytes=2_097_152,
            backupCount=5
        )
        logging.basicConfig(
            handlers=[handler],
            level=log_level,
            format=log_format,
            datefmt=date_format
        )
    else:
        logging.basicConfig(
            level=log_level,
            format=log_format,
            datefmt=date_format
        )
        

