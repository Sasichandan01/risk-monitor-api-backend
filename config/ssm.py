import boto3
from functools import lru_cache
from botocore.config import Config

session = boto3.Session(profile_name='Absc')

my_config = Config(
    region_name='ap-south-1',
    signature_version='s3v4',
)

ssm = session.client('ssm', config=my_config)

@lru_cache(maxsize=10)
def get_parameter(param_name: str, decrypt: bool = True) -> str:
    try:
        response = ssm.get_parameter(
            Name=param_name,
            WithDecryption=decrypt
        )
        return response['Parameter']['Value']
    except Exception as e:
        print(f"❌ SSM error for {param_name}: {e}")
        return None

class SSMConfig:
    @property
    def NEON_CONNECTION_STRING_CALL(self):
        return get_parameter('/neon_connection_string/call')

    @property
    def NEON_CONNECTION_STRING_PUT(self):
        return get_parameter('/neon_connection_string/put')

    @property
    def NIFTY_SPOT(self):
        return get_parameter('nifty_spot', decrypt=True)



config = SSMConfig()