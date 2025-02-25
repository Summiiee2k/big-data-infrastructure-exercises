import os
import json
import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_s3
from io import BytesIO

from bdi_api.settings import Settings

settings = Settings()

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Create a mocked S3 client."""
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        # Create a bucket
        s3.create_bucket(Bucket=settings.s3_bucket)
        
        # Create some test data
        test_data = {
            'now': 1234567890,
            'aircraft': [{
                'hex': 'abc123',
                'r': 'TEST123',
                't': 'B738',
                'alt_baro': 30000,
                'gs': 450,
                'emergency': 'none',
                'lat': 41.123,
                'lon': 2.123
            }]
        }
        
        # Upload data to S3
        s3.put_object(
            Bucket=settings.s3_bucket,
            Key="raw/day=20231101/000000Z.json.gz",
            Body=json.dumps(test_data).encode('utf-8')
        )
        
        yield s3

class TestS4Student:
    """Tests for the S4 implementation that uses AWS S3"""

    def test_download_time_format(self, client: TestClient, s3_client) -> None:
        """Tests whether files are downloaded with the correct time format."""
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=10")
            assert response.status_code == 200

            # List files in S3
            response = s3_client.list_objects_v2(
                Bucket=settings.s3_bucket,
                Prefix="raw/day=20231101/"
            )
            
            assert 'Contents' in response, "No file was created in S3"
            files = response['Contents']
            
            for obj in files:
                # Extract timestamp from the file name
                file_name = os.path.basename(obj['Key'])
                if not file_name.endswith('.json.gz'):
                    continue
                    
                time_str = file_name.replace('Z.json.gz', '')
                
                # Check format HHMMSS
                hours = int(time_str[:2])
                minutes = int(time_str[2:4])
                seconds = int(time_str[4:6])
                
                assert 0 <= hours <= 23, f"Invalid hours: {hours}"
                assert 0 <= minutes <= 59, f"Invalid minutes: {minutes}"
                assert seconds % 5 == 0 and seconds < 60, f"Invalid seconds: {seconds}"

    def test_prepare_data_structure(self, client: TestClient, s3_client) -> None:
        """Tests whether the prepared files have the correct structure."""
        with client as client:
            # Download and prepare data
            response = client.post("/api/s4/aircraft/prepare")
            assert response.status_code == 200

            # Verify if the files were created in S3
            expected_files = ['aircraft_info.csv', 'positions.csv', 'statistics.csv']
            for filename in expected_files:
                response = s3_client.list_objects_v2(
                    Bucket=settings.s3_bucket,
                    Prefix=f"prepared/day=20231101/{filename}"
                )
                assert 'Contents' in response, f"File {filename} was not created in S3"

    def test_data_format(self, client: TestClient, s3_client) -> None:
        """Tests the format of data in the prepared files."""
        with client as client:
            # Prepare data
            response = client.post("/api/s4/aircraft/prepare")
            assert response.status_code == 200

            # Verify aircraft_info.csv
            response = s3_client.get_object(
                Bucket=settings.s3_bucket,
                Key="prepared/day=20231101/aircraft_info.csv"
            )
            content = response['Body'].read().decode('utf-8').strip()
            assert content, "aircraft_info.csv is empty"
            header = content.split('\n')[0]
            expected_columns = ['icao', 'registration', 'type']
            assert all(col in header for col in expected_columns), "Incorrect columns in aircraft_info.csv"

            # Verify positions.csv
            response = s3_client.get_object(
                Bucket=settings.s3_bucket,
                Key="prepared/day=20231101/positions.csv"
            )
            content = response['Body'].read().decode('utf-8').strip()
            assert content, "positions.csv is empty"
            header = content.split('\n')[0]
            expected_columns = ['icao', 'timestamp', 'lat', 'lon']
            assert all(col in header for col in expected_columns), "Incorrect columns in positions.csv"

            # Verify statistics.csv
            response = s3_client.get_object(
                Bucket=settings.s3_bucket,
                Key="prepared/day=20231101/statistics.csv"
            )
            content = response['Body'].read().decode('utf-8').strip()
            assert content, "statistics.csv is empty"
            header = content.split('\n')[0]
            expected_columns = ['icao', 'max_altitude_baro', 'max_ground_speed', 'had_emergency']
            assert all(col in header for col in expected_columns), "Incorrect columns in statistics.csv"

    def test_s3_cleanup(self, client: TestClient, s3_client) -> None:
        """Tests whether S3 cleanup is working correctly."""
        # First upload
        response = client.post("/api/s4/aircraft/download?file_limit=5")
        assert response.status_code == 200

        # List files after first upload
        response = s3_client.list_objects_v2(
            Bucket=settings.s3_bucket,
            Prefix="raw/day=20231101/"
        )
        assert 'Contents' in response
        first_count = len(response['Contents'])

        # Second upload
        response = client.post("/api/s4/aircraft/download?file_limit=3")
        assert response.status_code == 200

        # List files after second upload
        response = s3_client.list_objects_v2(
            Bucket=settings.s3_bucket,
            Prefix="raw/day=20231101/"
        )
        assert 'Contents' in response
        second_count = len(response['Contents'])

        # Verify that the number of files is less than or equal to the first upload
        assert second_count <= first_count, "S3 cleanup is not working correctly"
