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

import json
import logging
from logging.handlers import RotatingFileHandler
import os, socket
import sys
from typing import Optional, Dict, Any
from os import path

global log_file

def load_json_file(file_path: str) -> Optional[dict]:
    """Load and parse a JSON file safely.
    
    Args:
        file_path: Path to the JSON file.
    
    Returns:
        Parsed JSON data as a dict, or None if file is invalid/missing.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except (FileNotFoundError, PermissionError) as e:
        logging.error(f"File access error: {str(e)}")
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    return None
  

def set_rc_logger() -> None:
    """Configure the root logger with settings from config and environment."""
    config, _ = get_aws_configs()
    
    # Convert log level string to logging level
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    
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

class AWSConfig:
    """Handles configuration for the aws configs defined in awsprov_config.json.

    Attributes:
        log_level (str): The logging level (e.g., "INFO", "DEBUG", "WARNING", "ERROR"). Defaults to "INFO".
        TBD

    """
    def __init__(self,  config_data: Dict[str, Any]) -> None:
        """Initialize configuration from JSON content.

            Args:
            content (dict): A dictionary containing configuration values.
        """
      
        self.log_level: str = config_data.get("LogLevel", "INFO") # Validated via setter
        self.region: str = config_data.get("AWS_REGION", "")
        self.api_endpoint_url: str = config_data.get("AWS_ENDPOINT_URL", "")
        self.credential_file: str = config_data.get("AWS_CREDENTIAL_FILE", "")
        self.credential_script: str = config_data.get("AWS_CREDENTIAL_SCRIPT", "")
        self.credential_renew_margin: str = config_data.get("AWS_CREDENTIAL_RENEW_MARGIN", "")
        self.key_file: str = config_data.get("AWS_KEY_FILE", "")
        self.spot_terminate_on_reclaim: bool = config_data.get("AWS_SPOT_TERMINATE_ON_RECLAIM", False)
        self.instance_creation_timeout: int = config_data.get("INSTANCE_CREATION_TIMEOUT", 10)
        self.tag_instance_id: bool = config_data.get("AWS_TAG_InstanceID", False)


    @property
    def log_level(self) -> str:
        return self._log_level

    @log_level.setter
    def log_level(self, value: str) -> None:
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR"}
        if value not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of: {valid_levels}")
        self._log_level = value


    def __str__(self) -> str:
        return (
            f"log_level: {self.log_level}\n"
            f"region: {self.region}\n"
            f"api_endpoint_url: {self.api_endpoint_url}\n"
            f"credential_file: {self.credential_file}\n"
            f"credential_script: {self.credential_script}\n"
            f"credential_renew_margin: {self.credential_renew_margin}\n"
            f"key_file: {self.key_file}\n"
            f"spot_terminate_on_reclaim: {self.spot_terminate_on_reclaim}\n"
            f"instance_creation_timeout: {self.instance_creation_timeout}\n"
            f"tag_instance_id: {self.tag_instance_id}\n"
        )



class AWSTemplate:
    """Handles configuration for the aws template defined in awsprov_templates.json.

    Attributes:
      templateId (str): The template ID (e.g., "INFO", "DEBUG", "WARNING", "ERROR"). Defaults to "INFO".
      TBD

    """
    def __init__(self, template_data: Dict[str, Any]) -> None:
        """Initialize configuration from JSON content.

          Args:
            content (dict): A dictionary containing configuration values.
        """

        self.templateId = template_data.get("templateId", "")
        self.maxNumber = template_data.get("maxNumber", 0)
        self.imageId = template_data.get("imageId", "")
        self.subnetId = template_data.get("subnetId", "")
        self.vmType = template_data.get("vmType", "")
        self.vmNumber = template_data.get("vmNumber", 0)
        self.ttl = template_data.get("ttl", 0)
        self.keyName = template_data.get("keyName", "")
        self.sgIds = template_data.get("sgIds", "")
        self.userData = template_data.get("userData", "")
        self.userDataObj = template_data.get("userDataObj", "")
        self.pGrpName = template_data.get("pGrpName", "")
        self.instanceProfile = template_data.get("instanceProfile", "")
        self.ebsOptimized = template_data.get("ebsOptimized", false)
        self.priority = template_data.get("priority", 0)
        self.tenancy = template_data.get("tenancy", "")
        self.interfaceType = template_data.get("interfaceType", "")
        self.efaCount = template_data.get("efaCount", "")
        self.launchTemplateId = template_data.get("launchTemplateId", "")
        self.launchTemplateVersion = template_data.get("launchTemplateVersion", "")    
        self.marketSpotPrice = template_data.get("marketSpotPrice", "")
        self.ec2FleetConfig = template_data.get("ec2FleetConfig", "")
        self.onDemandTargetCapacityRatio = template_data.get("onDemandTargetCapacityRatio", "")


    def __str__(self) -> str:
        return (
            f"templateId: {self.templateId}\n"
            f"maxNumber: {self.maxNumber}\n"
            f"imageId: {self.imageId}\n"
            f"subnetId: {self.subnetId}\n"
            f"vmType: {self.vmType}\n"
            f"vmNumber: {self.vmNumber}\n"
            f"ttl: {self.ttl}\n"
            f"keyName: {self.keyName}\n"
            f"sgIds: {self.sgIds}\n"
            f"userData: {self.userData}\n"
            f"userDataObj: {self.userDataObj}\n"
            f"pGrpName: {self.pGrpName}\n"
            f"instanceProfile: {self.instanceProfile}\n"
            f"ebsOptimized: {self.ebsOptimized}\n"
            f"priority: {self.priority}\n"
            f"tenancy: {self.tenancy}\n"
            f"interfaceType: {self.interfaceType}\n"
            f"efaCount: {self.efaCount}\n"
            f"launchTemplateId: {self.launchTemplateId}\n"
            f"launchTemplateVersion: {self.launchTemplateVersion}\n"
            f"marketSpotPrice: {self.marketSpotPrice}\n"
            f"ec2FleetConfig: {self.ec2FleetConfig}\n"
            f"onDemandTargetCapacityRatio: {self.onDemandTargetCapacityRatio}\n"
        )

def get_aws_configs(template_id: str = "") -> tuple[AWSConfig, Optional[AWSTemplate]]:
    """Load and return AWS configuration and template.
    
    Args:
        template_id: ID of the template to load. If empty, no template is loaded.
    
    Returns:
        Tuple of (AWSConfig, AWSTemplate). Template may be None.
    
    Raises:
        EnvironmentError: If required env vars or files are missing.
        JSONDecodeError: If config files contain invalid JSON.
    """
    try:
        conf_dir = os.environ["PRO_CONF_DIR"]
    except KeyError:
        raise EnvironmentError("PRO_CONF_DIR environment variable is not set")

    # Validate files exist first
    config_path = os.path.join(conf_dir, "conf", "awsprov_config.json")
    template_path = os.path.join(conf_dir, "conf", "awsprov_templates.json")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    if template_id and not os.path.exists(template_path):
        raise FileNotFoundError(f"Template file not found: {template_path}")

    # Load config
    with open(config_path, "r", encoding="utf-8") as config_file:
        config = AWSConfig(json.load(config_file))

    # Load template if ID provided
    template = None
    if template_id:
        with open(template_path, "r", encoding="utf-8") as template_file:
            template_data = json.load(template_file)
            template = AWSTemplate(template_data, template_id)
        logging.debug("Loaded template in get_aws_configs: %s", template_id)

    return config, template


def main() -> None:
    """Main function to load and validate configuration and templates."""
    # Load config
    config_data = load_json_file("conf/awsprov_config.json")
    if not config_data:
        logging.critical("Failed to load config file. Exiting.")
        return

    config_obj = AWSConfig(config_data)
    print(config_obj)

    # Load template
    template_data = load_json_file("conf/awsprov_templates.json")
    if not template_data:
        logging.critical("Failed to load template file. Exiting.")
        return

    template_obj = AWSTemplate(template_data, "CENTOS-Template-NGVM-1")
    print(template_obj)

if __name__ == "__main__":
  logging.basicConfig(level=logging.DEBUG)
  main()
