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

import os
import json
import logging
import sys
import traceback
from typing import Dict, Any
from aws_logger import set_rc_logger
from aws_utils import load_json_file

def main() -> None:
    """Main function to load and display AWS templates configuration."""
    try:
        # Get configuration directory from environment
        conf_dir = os.environ.get("PRO_CONF_DIR")
        if not conf_dir:
            logging.critical("The PRO_CONF_DIR env. variable is not set")
            sys.exit("Error: PRO_CONF_DIR environment variable is not set")

        # Build template file path safely
        template_file = os.path.join(conf_dir, "conf", "awsprov_templates.json")
        
        # Verify template file exists
        if not os.path.exists(template_file):
            logging.critical(f"Template file does not exist: {template_file}")
            sys.exit(f"Error: Template file {template_file} does not exist")

        # Read and parse template file
        out_json = load_json_file(template_file)
            
        logging.info("Loaded template configuration: %s", out_json)
        print(json.dumps(out_json, indent=2))

    except json.JSONDecodeError as e:
        logging.critical(f"Invalid JSON in template file: {str(e)}")
        sys.exit(f"Error: Invalid JSON in template file")
    except Exception as e:
        logging.critical(f"Unexpected error: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        set_rc_logger()  # Initialize logging first
        logging.critical("----- Entering getAvailableTemplates -----")
        main()
        logging.critical("----- Exiting getAvailableTemplates -----")
    except Exception as e:
        logging.error("Error in main execution: %s", traceback.format_exc())
        sys.exit(1)
