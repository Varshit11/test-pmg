"""
AWS Database Discovery Module
Discovers and provides connection details for MySQL RDS instances attached to EC2
"""

import boto3
import json
import logging
import os
from typing import Dict, Any, List, Optional
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class AWSDiscoveryError(Exception):
    """Custom exception for AWS discovery errors"""
    pass


class AWSRDSDiscovery:
    """
    Discovers MySQL RDS instances attached to the current EC2 instance
    """
    
    def __init__(self):
        """Initialize AWS RDS discovery"""
        self.session = None
        self.rds_client = None
        self.ec2_client = None
        self.instance_id = None
        self.region = None
        
        self._initialize_aws_session()
        
    def _initialize_aws_session(self):
        """Initialize AWS session and clients"""
        try:
            # Try to get region from instance metadata
            self.region = self._get_instance_region()
            
            # Create session
            self.session = boto3.Session(region_name=self.region)
            self.rds_client = self.session.client('rds')
            self.ec2_client = self.session.client('ec2')
            
            # Get current instance ID
            self.instance_id = self._get_instance_id()
            
            logger.info(f"AWS session initialized - Region: {self.region}, Instance: {self.instance_id}")
            
        except Exception as e:
            logger.warning(f"Could not initialize AWS session: {e}")
            # Continue without AWS discovery - will use manual config
    
    def _get_instance_region(self) -> str:
        """Get current EC2 instance region"""
        try:
            # Try instance metadata service
            import requests
            response = requests.get(
                'http://169.254.169.254/latest/meta-data/placement/availability-zone',
                timeout=2
            )
            if response.status_code == 200:
                return response.text[:-1]  # Remove last character (availability zone letter)
        except:
            pass
        
        # Fallback to environment variable or default
        return os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    
    def _get_instance_id(self) -> Optional[str]:
        """Get current EC2 instance ID"""
        try:
            import requests
            response = requests.get(
                'http://169.254.169.254/latest/meta-data/instance-id',
                timeout=2
            )
            if response.status_code == 200:
                return response.text
        except:
            pass
        
        return None
    
    def discover_mysql_instances(self) -> List[Dict[str, Any]]:
        """
        Discover MySQL RDS instances that could be used by this EC2 instance
        
        Returns:
            List of MySQL RDS instances with connection details
        """
        if not self.rds_client:
            logger.warning("AWS RDS client not available, skipping discovery")
            return []
        
        try:
            mysql_instances = []
            
            # Get all RDS instances
            response = self.rds_client.describe_db_instances()
            
            for db_instance in response['DBInstances']:
                # Filter for MySQL instances
                if db_instance['Engine'].lower() in ['mysql', 'aurora-mysql']:
                    instance_info = {
                        'identifier': db_instance['DBInstanceIdentifier'],
                        'engine': db_instance['Engine'],
                        'host': db_instance['Endpoint']['Address'],
                        'port': db_instance['Endpoint']['Port'],
                        'status': db_instance['DBInstanceStatus'],
                        'availability_zone': db_instance['AvailabilityZone'],
                        'vpc_security_groups': [sg['VpcSecurityGroupId'] for sg in db_instance['VpcSecurityGroups']],
                        'db_subnet_group': db_instance.get('DBSubnetGroup', {}).get('DBSubnetGroupName'),
                        'publicly_accessible': db_instance.get('PubliclyAccessible', False),
                        'storage_encrypted': db_instance.get('StorageEncrypted', False),
                        'master_username': db_instance.get('MasterUsername'),
                        'allocated_storage': db_instance.get('AllocatedStorage'),
                        'db_instance_class': db_instance.get('DBInstanceClass'),
                        'creation_time': db_instance.get('InstanceCreateTime', '').isoformat() if db_instance.get('InstanceCreateTime') else ''
                    }
                    
                    # Check if this instance is accessible from current EC2
                    if self._is_instance_accessible(instance_info):
                        mysql_instances.append(instance_info)
            
            logger.info(f"Found {len(mysql_instances)} accessible MySQL RDS instances")
            return mysql_instances
            
        except Exception as e:
            logger.error(f"Error discovering MySQL instances: {e}")
            return []
    
    def _is_instance_accessible(self, db_instance: Dict[str, Any]) -> bool:
        """
        Check if RDS instance is accessible from current EC2 instance
        
        Args:
            db_instance: RDS instance information
            
        Returns:
            True if accessible, False otherwise
        """
        try:
            # If publicly accessible, consider it available
            if db_instance.get('publicly_accessible'):
                return True
            
            # If we don't have instance ID, assume accessible
            if not self.instance_id:
                return True
            
            # Check if EC2 and RDS are in compatible security groups/subnets
            # This is a simplified check - in production you might want more sophisticated logic
            return db_instance.get('status', '').lower() == 'available'
            
        except Exception as e:
            logger.warning(f"Error checking instance accessibility: {e}")
            return True  # Default to accessible if check fails
    
    def get_recommended_instance(self) -> Optional[Dict[str, Any]]:
        """
        Get the recommended MySQL RDS instance for this EC2
        
        Returns:
            Dictionary with recommended instance details or None
        """
        instances = self.discover_mysql_instances()
        
        if not instances:
            return None
        
        # Sort by preference: available status, same AZ, largest storage
        def sort_key(instance):
            status_score = 1 if instance['status'] == 'available' else 0
            same_az_score = 1 if self._is_same_availability_zone(instance) else 0
            storage_score = instance.get('allocated_storage', 0)
            return (status_score, same_az_score, storage_score)
        
        instances.sort(key=sort_key, reverse=True)
        
        recommended = instances[0]
        logger.info(f"Recommended MySQL instance: {recommended['identifier']}")
        
        return recommended
    
    def _is_same_availability_zone(self, db_instance: Dict[str, Any]) -> bool:
        """Check if RDS instance is in same AZ as EC2"""
        try:
            if not self.instance_id or not self.ec2_client:
                return False
            
            # Get EC2 instance AZ
            response = self.ec2_client.describe_instances(InstanceIds=[self.instance_id])
            if response['Reservations']:
                ec2_az = response['Reservations'][0]['Instances'][0]['Placement']['AvailabilityZone']
                return db_instance.get('availability_zone') == ec2_az
        except:
            pass
        
        return False
    
    def get_connection_config(self, instance_identifier: str = None) -> Dict[str, Any]:
        """
        Get database connection configuration
        
        Args:
            instance_identifier: Specific RDS instance identifier (optional)
            
        Returns:
            Database connection configuration
        """
        if instance_identifier:
            # Get specific instance
            instances = self.discover_mysql_instances()
            instance = next((i for i in instances if i['identifier'] == instance_identifier), None)
        else:
            # Get recommended instance
            instance = self.get_recommended_instance()
        
        if not instance:
            raise AWSDiscoveryError("No accessible MySQL RDS instance found")
        
        return {
            'type': 'mysql',
            'host': instance['host'],
            'port': instance['port'],
            'database': '',  # Will need to be specified
            'username': instance.get('master_username', ''),
            'password': '',  # Will need to be provided
            'connection_params': {
                'charset': 'utf8mb4',
                'connect_timeout': 30,
                'autocommit': True,
                'use_unicode': True
            },
            'aws_info': {
                'identifier': instance['identifier'],
                'engine': instance['engine'],
                'status': instance['status'],
                'availability_zone': instance['availability_zone'],
                'storage_encrypted': instance['storage_encrypted']
            }
        }
    
    def test_connection(self, connection_config: Dict[str, Any], database_name: str = None) -> bool:
        """
        Test connection to MySQL RDS instance
        
        Args:
            connection_config: Database connection configuration
            database_name: Database name to test (optional)
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            import pymysql
            
            config = connection_config.copy()
            if database_name:
                config['database'] = database_name
            
            connection = pymysql.connect(
                host=config['host'],
                port=config['port'],
                user=config['username'],
                password=config['password'],
                database=config.get('database', ''),
                **config.get('connection_params', {})
            )
            
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            
            connection.close()
            logger.info("Database connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False


def discover_aws_mysql_database() -> Dict[str, Any]:
    """
    Convenience function to discover and return MySQL database configuration
    
    Returns:
        Database configuration dictionary
    """
    try:
        discovery = AWSRDSDiscovery()
        return discovery.get_connection_config()
    except Exception as e:
        logger.warning(f"AWS discovery failed: {e}")
        return {}


def list_available_mysql_instances() -> List[Dict[str, Any]]:
    """
    List all available MySQL RDS instances
    
    Returns:
        List of MySQL RDS instances
    """
    try:
        discovery = AWSRDSDiscovery()
        return discovery.discover_mysql_instances()
    except Exception as e:
        logger.error(f"Failed to list MySQL instances: {e}")
        return []