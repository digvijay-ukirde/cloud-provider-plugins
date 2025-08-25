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
import os
from typing import Optional, Dict, Any
import logging


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
    def __init__(self, template_data: Dict[str, Any], template_id: str) -> None:
        """Initialize configuration from JSON content.

        Args:
            template_data (dict): A dictionary containing template configuration values.
            template_id (str): The ID of the template to use from the data.
        """
        # Find the specific template by ID
        templates = template_data.get("templates", [])
        template = None
        for t in templates:
            if t.get("templateId") == template_id:
                template = t
                break
        
        if not template:
            raise ValueError(f"Template with ID '{template_id}' not found")

        self.templateId = template.get("templateId", "")
        self.maxNumber = template.get("maxNumber", 0)
        self.imageId = template.get("imageId", "")
        self.subnetId = template.get("subnetId", "")
        self.vmType = template.get("vmType", "")
        self.vmNumber = template.get("vmNumber", 0)
        self.ttl = template.get("ttl", 0)
        self.keyName = template.get("keyName", "")
        self.SecurityGroupIds = template.get("SecurityGroupIds", "")
        self.userData = template.get("userData", "")
        self.userDataObj = template.get("userDataObj", "")
        self.pGrpName = template.get("pGrpName", "")
        self.instanceProfile = template.get("instanceProfile", "")
        self.ebsOptimized = template.get("ebsOptimized", False)
        self.priority = template.get("priority", 0)
        self.tenancy = template.get("tenancy", "")
        self.interfaceType = template.get("interfaceType", "")
        self.efaCount = template.get("efaCount", "")
        self.launchTemplateId = template.get("launchTemplateId", "")
        self.launchTemplateVersion = template.get("launchTemplateVersion", "")    
        self.marketSpotPrice = template.get("marketSpotPrice", "")
        self.ec2FleetConfig = template.get("ec2FleetConfig", "")
        self.onDemandTargetCapacityRatio = template.get("onDemandTargetCapacityRatio", "")


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
            f"SecurityGroupIds: {self.SecurityGroupIds}\n"
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


def get_aws_configs():
    """Load and return AWS configuration and template.
    
    Args:
        template_id: ID of the template to load. If empty, no template is loaded.
    
    Returns:
        Dict of AWSConfig
    
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
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # Load config
    with open(config_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
    return config

    
def get_aws_template(template_id: str = ""):
    """Load and return AWS configuration and template.
    
    Args:
        template_id: ID of the template to load. If empty, no template is loaded.
    
    Returns:
        Dict of AWSTemplate. Template may be None.
    
    Raises:
        EnvironmentError: If required env vars or files are missing.
        JSONDecodeError: If config files contain invalid JSON.
    """
    try:
        conf_dir = os.environ["PRO_CONF_DIR"]
    except KeyError:
        raise EnvironmentError("PRO_CONF_DIR environment variable is not set")

    # Validate files exist first
    template_path = os.path.join(conf_dir, "conf", "awsprov_templates.json")
    
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template file not found: {template_path}")

    # Load template if ID provided
    template = None
    if template_id:
        with open(template_path, "r", encoding="utf-8") as template_file:
            templates = json.load(template_file)
            
        for template in templates.get("templates", []):
            if template.get("templateId") == template_id:
                return template
    return template


def main() -> None:
    """Main function to load and validate configuration and templates."""
    # Load config
    config = get_aws_configs()
    config_obj = AWSConfig(config)
    if not config_obj:
        logging.critical("Failed to load config file. Exiting.")
        return
    print(config_obj)

    # Load template
    template = get_aws_template("template-01")
    template_obj = AWSTemplate(template, "template-01")
    if not template_obj:
        logging.critical("Failed to load template file. Exiting.")
        return

    print(template_obj)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
