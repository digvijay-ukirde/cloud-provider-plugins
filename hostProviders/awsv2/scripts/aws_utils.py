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

import boto3
import configparser
import json
import logging
import multiprocessing
import os
import sys
import time
import random
import threading
import traceback
from datetime import datetime, timedelta
from contextlib import contextmanager
from functools import partial
from typing import List, Dict, Tuple, Optional, Any
from botocore.exceptions import ClientError

import logging

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


class AWSVPCManager:
    def __init__(self, config: dict, template: dict):
        self.config = config
        self.template = template
        self.vm_pool = None
        self.min_vm_workers = 4
        self.timeout = 300
        self.aws_credentials = self._get_aws_credentials()
        self._initialize_aws_resources()
        self._init_vm_pool()

    def _get_aws_credentials(self) -> dict:
        """Extract AWS credentials from config"""
        credentials = {}
        aws_credential_file = self.config['AWS_CREDENTIAL_FILE']
        if aws_credential_file:
            credentials = self.get_aws_credentials_from_credential_file(aws_credential_file, 'default')
        
        return credentials

    def get_aws_credentials_from_credential_file(self, credentials_file, profile_name="default"):
        """
        Parse AWS credentials file and return access key & secret key.

        :param credentials_file: Path to credentials file (no defualt)
        :param profile_name: AWS profile name (default is "default")
        :return: dict with aws_access_key_id and aws_secret_access_key
        """
        config = configparser.ConfigParser()
        config.read(credentials_file)

        if profile_name not in config:
            raise ValueError(f"Profile '{profile_name}' not found in {credentials_file}")

        return {
            "aws_access_key_id": config[profile_name].get("aws_access_key_id"),
            "aws_secret_access_key": config[profile_name].get("aws_secret_access_key")
        }

    def _initialize_aws_resources(self):
        """Initialize AWS resources with instance profile"""
        try:
            # Get region from config or template
            region = self.config['AWS_REGION']
            
            logging.info(f"Initializing AWS resources in region: {region}")
            
            # Initialize with credentials if available
            if self.aws_credentials:
                aws_config = {
                    'region_name': region,
                    'aws_access_key_id': self.aws_credentials['aws_access_key_id'],
                    'aws_secret_access_key': self.aws_credentials['aws_secret_access_key']
                }
                if 'session_token' in self.aws_credentials:
                    aws_config['aws_session_token'] = self.aws_credentials['session_token']
                
                self.ec2_client = boto3.client('ec2', **aws_config)
                self.ec2_resource = boto3.resource('ec2', **aws_config)
            else:
                # Use default credential provider chain
                self.ec2_client = boto3.client('ec2', region_name=region)
                self.ec2_resource = boto3.resource('ec2', region_name=region)
            
            # Test the connection
            self.ec2_client.describe_regions()
            logging.info("Successfully initialized AWS resources")
                
        except Exception as e:
            logging.error(f"AWS initialization failed: {str(e)}")
            raise RuntimeError(f"AWS initialization failed: {str(e)}")

    def _init_vm_pool(self):
        """Initialize multiprocessing pool"""
        if self.vm_pool is None:
            try:
                cpu_count = multiprocessing.cpu_count()
                workers = max(self.min_vm_workers, min(cpu_count, 8))
                
                self.vm_pool = multiprocessing.Pool(
                    processes=workers,
                    maxtasksperchild=10
                )
                logging.info(f"Initialized VM pool with {workers} workers")
                
            except Exception as e:
                logging.error(f"Failed to initialize VM pool: {str(e)}")
                # Continue without pool, will use sequential fallback

    @staticmethod
    def _create_instance_worker(args: tuple) -> dict:
        """Worker function that creates its own AWS session"""
        instance_name, template, tag_value, credentials, region = args
        try:
            aws_config = {
                'region_name': region,
                'aws_access_key_id': credentials.get('aws_access_key_id'),
                'aws_secret_access_key': credentials.get('aws_secret_access_key')
            }
            if 'session_token' in credentials:
                aws_config['aws_session_token'] = credentials['session_token']

            session = boto3.Session(**aws_config)
            ec2_client = session.client('ec2')
            print("dv, ec2_client", ec2_client)
            
            # Prepare instance parameters
            instance_params = {
                'ImageId': template['imageId'],
                'InstanceType': template['vmType'],
                'KeyName': template['keyName'],
                'SubnetId': template['subnetId'],
                'MinCount': 1,
                'MaxCount': 1,
                'TagSpecifications': [{
                    'ResourceType': 'instance',
                    'Tags': [
                        {'Key': 'Name', 'Value': instance_name},
                        {'Key': 'ManagedBy', 'Value': 'AWSVPCManager'},
                        {'Key': 'Tag', 'Value': tag_value}
                    ]
                }]
            }
            
            # Handle security groups - FIXED: Check if SecurityGroupIds exists and is not empty
            security_group_ids = template.get('SecurityGroupIds')
            if security_group_ids and security_group_ids.strip():
                instance_params['SecurityGroupIds'] = [sg.strip() for sg in security_group_ids.split(',') if sg.strip()]
            
            # Add optional parameters
            if template.get('userData'):
                instance_params['UserData'] = template['userData']
            
            if template.get('ebsOptimized') is not None:
                instance_params['EbsOptimized'] = template['ebsOptimized']
            
            # Create instance
            response = ec2_client.run_instances(**instance_params)
            instance_id = response['Instances'][0]['InstanceId']
            
            logging.info(f"Created instance {instance_id} ({instance_name})")
            return {
                'success': True,
                'id': instance_id, 
                'name': instance_name,
                'status': 'pending'
            }
            
        except Exception as e:
            error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', 'N/A')
            error_msg = getattr(e, 'response', {}).get('Error', {}).get('Message', str(e))
            error_trace = traceback.format_exc()
            
            logging.error(f"Instance creation failed for {instance_name} ({error_code}): {error_msg}")
            logging.debug(f"Traceback: {error_trace}")
            
            return {
                'success': False,
                'name': instance_name,
                'error': error_msg,
                'error_code': error_code,
                'traceback': error_trace
            }


    @contextmanager
    def resource_context(self):
        """Context manager for resource cleanup"""
        try:
            yield self
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        if self.vm_pool:
            try:
                logging.info("Shutting down VM pool...")
                self.vm_pool.close()
                self.vm_pool.terminate()
                self.vm_pool.join()
                self.vm_pool = None
                logging.info("VM pool shutdown complete")
            except Exception as e:
                logging.error(f"Error shutting down VM pool: {str(e)}")

    def request_instances(self, count: int, tag_value: str) -> Tuple[List[dict], str]:
        """Create multiple instances in parallel"""
        
        base_name = f"dv-{os.getpid()}-{int(time.time())}"
        vm_names = [f"{base_name}-{i:03d}" for i in range(count)]
        
        try:
            # Prepare template config for workers
            logging.debug(f"Template : {self.template}")
            
            # Prepare arguments for parallel processing with credentials
            args = [(name, self.template, tag_value, self.aws_credentials, self.config['AWS_REGION']) for name in vm_names]
            print("dv - request_instances, args", args)
            # Execute in parallel with timeout
            logging.info(f"Creating {count} instances in parallel...")
            results = self.vm_pool.map_async(
                self._create_instance_worker, 
                args
            ).get()
            print("dv - request_instances-results", results)
            # Process results
            instances = []
            errors = []
            
            for result in results:
                if result.get('success'):
                    instances.append({
                        'id': result['id'],
                        'name': result['name'],
                        'status': result['status'],
                        'launch_time': time.time(),
                        'tag': tag_value
                    })
                else:
                    errors.append(f"{result['name']}: {result.get('error', 'Unknown error')} (Code: {result.get('error_code', 'N/A')})")
            
            error_msg = "; ".join(errors) if errors else ""
            
            if errors:
                logging.warning(f"Completed with {len(errors)} errors: {error_msg}")
            else:
                logging.info(f"Successfully created {len(instances)} instances")
                
            return instances, error_msg
            
        except multiprocessing.TimeoutError:
            logging.error("Instance creation timed out")
            return [], "Creation timeout"
        except Exception as e:
            logging.error(f"Instance creation failed: {str(e)}")
            return [], str(e)


    # ... (wait_for_instances, terminate_instances, check_instance_status methods remain the same)
    def wait_for_instances(self, instance_ids: List[str]) -> Tuple[List[dict], List[dict]]:
        """Wait for instances to reach running state"""
        try:
            waiter = self.ec2_client.get_waiter('instance_running')
            success = []
            failed = []
            
            logging.info(f"Waiting for {len(instance_ids)} instances to become running...")
            waiter.wait(
                InstanceIds=instance_ids,
                WaiterConfig={'Delay': 10, 'MaxAttempts': 40}
            )
            
            # Get instance details
            responses = self.ec2_client.describe_instances(InstanceIds=instance_ids)
            for reservation in responses['Reservations']:
                for instance in reservation['Instances']:
                    instance_info = {
                        'id': instance['InstanceId'],
                        'private_ip': instance.get('PrivateIpAddress', ''),
                        'public_ip': instance.get('PublicIpAddress', ''),
                        'status': instance['State']['Name'],
                        'instance_type': instance['InstanceType']
                    }
                    
                    if instance['State']['Name'] == 'running':
                        success.append(instance_info)
                    else:
                        instance_info['reason'] = instance.get('StateTransitionReason', 'Unknown')
                        failed.append(instance_info)
            
            logging.info(f"Instances ready: {len(success)} successful, {len(failed)} failed")
            return success, failed
            
        except Exception as e:
            logging.error(f"Error waiting for instances: {str(e)}")
            # Return all instances as failed with error reason
            failed = [{'id': id, 'status': 'error', 'reason': str(e)} for id in instance_ids]
            return [], failed

    def terminate_instances(self, instance_ids: List[str]) -> Tuple[bool, List[dict]]:
        """Terminate instances with retry logic"""
        max_retries = 3
        results = []
        
        for attempt in range(max_retries):
            try:
                response = self.ec2_client.terminate_instances(InstanceIds=instance_ids)
                
                for termination in response['TerminatingInstances']:
                    results.append({
                        'id': termination['InstanceId'],
                        'previous_state': termination['PreviousState']['Name'],
                        'current_state': termination['CurrentState']['Name']
                    })
                
                logging.info(f"Termination request sent for {len(instance_ids)} instances")
                return True, results
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'InvalidInstanceID.NotFound':
                    logging.info("Instances already terminated")
                    for instance_id in instance_ids:
                        results.append({
                            'id': instance_id,
                            'previous_state': 'terminated',
                            'current_state': 'terminated'
                        })
                    return True, results
                    
                logging.warning(f"Termination attempt {attempt + 1} failed: {error_code}")
                time.sleep(random.randint(2, 5))
                
            except Exception as e:
                logging.warning(f"Termination attempt {attempt + 1} failed: {str(e)}")
                time.sleep(random.randint(2, 5))
                
        logging.error(f"Failed to terminate instances after {max_retries} attempts")
        return False, results

    def check_instance_status(self, instance_ids: List[str]) -> List[dict]:
        """Check current status of instances"""
        try:
            responses = self.ec2_client.describe_instances(InstanceIds=instance_ids)
            statuses = []
            
            for reservation in responses['Reservations']:
                for instance in reservation['Instances']:
                    statuses.append({
                        'id': instance['InstanceId'],
                        'status': instance['State']['Name'],
                        'private_ip': instance.get('PrivateIpAddress', ''),
                        'public_ip': instance.get('PublicIpAddress', ''),
                        'instance_type': instance['InstanceType'],
                        'launch_time': instance['LaunchTime'].isoformat() if 'LaunchTime' in instance else 'unknown',
                        'reason': instance.get('StateTransitionReason', '')
                    })
                    
            return statuses
            
        except Exception as e:
            logging.error(f"Failed to check instance status: {str(e)}")
            return []