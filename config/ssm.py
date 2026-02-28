import boto3
from functools import lru_cache
from botocore.config import Config
import logging

logger = logging.getLogger(__name__)

# No profile for EC2 - uses IAM role automatically
my_config = Config(
    region_name='ap-south-1',
    signature_version='s3v4',
)

ssm = boto3.client('ssm', config=my_config)

@lru_cache(maxsize=10)
def get_parameter(param_name: str, decrypt: bool = True) -> str:
    """
    Retrieves a parameter from SSM Parameter Store.

    Args:
        param_name (str): The name of the parameter to retrieve
        decrypt (bool, optional): Whether to decrypt the parameter value. Defaults to True.

    Returns:
        str: The parameter value

    Raises:
        Exception: If there is an error retrieving the parameter from SSM
    """
    try:
        response = ssm.get_parameter(
            Name=param_name,
            WithDecryption=decrypt
        )
        return response['Parameter']['Value']
    except Exception as e:
        logger.error("SSM error for %s: %s", param_name, e)
        return None

class SSMConfig:
    @property
    def NEON_CONNECTION_STRING_CALL(self):
        """
        Property that returns the Neon connection string for CALL options from SSM Parameter Store.

        Returns:
            str: The Neon connection string for CALL options
        """
        return get_parameter('/neon_connection_string/call')

    @property
    def NEON_CONNECTION_STRING_PUT(self): 
        """
        Property that returns the Neon connection string for PUT options from SSM Parameter Store.

        Returns:
            str: The Neon connection string for PUT options
        """
        return get_parameter('/neon_connection_string/put')

    @property
    def NIFTY_SPOT(self):
        """
        Property that returns the current Nifty spot price from SSM Parameter Store.

        Returns:
            float: The current Nifty spot price
        """
        return get_parameter('nifty_spot', decrypt=True)

config = SSMConfig()