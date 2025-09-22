# Agent UI TSV - Expected Results Backend

A serverless backend service that processes TSV (Tab-Separated Values) data files to generate expected results for UI validation. This service loads clinical study data from S3, applies filters, and returns statistical summaries and participant information.

## What It Does

This project provides an API that:

- **Loads clinical study data** from TSV files stored in S3 (case, demographic, diagnosis, sample, and file tables)
- **Applies participant-level filters** based on breed, sex, and disease terms
- **Generates statistical summaries** including participant counts, sample counts, and file counts
- **Returns structured JSON responses** with filtered participant IDs and metadata
- **Supports both AWS Lambda deployment** and local development

The service is designed to work with clinical research data, particularly for canine studies, and provides expected results that can be used to validate UI components in data exploration applications.

## Project Structure

```
src/
├── app.py                    # Main Lambda handler
├── local_server.py          # Flask server for local development
├── template.yaml            # AWS SAM template for deployment
├── samconfig.toml           # SAM configuration
├── requirements.txt         # Python dependencies
├── Dockerfile              # Container configuration
└── expected_backend/       # Core processing modules
    ├── __init__.py
    ├── loader.py           # TSV loading and data normalization
    ├── filter.py           # Participant filtering logic
    ├── stats_bar.py        # Statistical summary generation
    └── cli.py              # Command-line interface
```

## Setup

### Prerequisites

- Python 3.12+
- AWS CLI configured (for deployment)
- AWS SAM CLI (for deployment)
- Docker (for containerized deployment)

### Local Development

1. **Clone and navigate to the project:**
   ```bash
   cd /Users/leungvw/agent-ui-tsv/src
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables:**
   ```bash
   export DATA_BUCKET="your-s3-bucket-name"
   export DATA_PREFIX="tsv/"
   ```

4. **Run the local development server:**
   ```bash
   python local_server.py
   ```

   The API will be available at `http://localhost:3000/mock-api`

### Using the CLI

You can also use the command-line interface for testing:

```bash
# Create a filters.json file
echo '{"filters": {"Breed": ["Golden Retriever"], "Sex": ["Male"]}}' > filters.json

# Run the CLI
python -m expected_backend.cli --study "your-study-name" --filters filters.json
```

## Deployment

### AWS Lambda Deployment (Recommended)

1. **Install AWS SAM CLI:**
   ```bash
   # macOS
   brew install aws-sam-cli
   
   # Or download from: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html
   ```

2. **Build the application:**
   ```bash
   sam build
   ```

3. **Deploy to AWS:**
   ```bash
   sam deploy --guided
   ```

   You'll be prompted to configure:
   - Stack name: `expected-backend`
   - AWS Region: `us-east-1` (or your preferred region)
   - Data bucket name: Your S3 bucket containing TSV files
   - Data prefix: `tsv/` (or your preferred prefix)

4. **Get the API endpoint:**
   After deployment, SAM will output the Function URL. This is your API endpoint.

### Manual AWS Lambda Deployment

1. **Build the Docker container:**
   ```bash
   docker build -t expected-backend .
   ```

2. **Tag and push to ECR:**
   ```bash
   aws ecr create-repository --repository-name expected-backend
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 905418397328.dkr.ecr.us-east-1.amazonaws.com
   docker tag expected-backend:latest 905418397328.dkr.ecr.us-east-1.amazonaws.com/expected-backend:latest
   docker push 905418397328.dkr.ecr.us-east-1.amazonaws.com/expected-backend:latest
   ```

3. **Create Lambda function** using the ECR image URI

## Usage

### API Endpoints

#### POST `/mock-api` (Local) or Function URL (AWS)

**Request Body:**
```json
{
  "study": "study-name",
  "filters": {
    "Breed": ["Golden Retriever", "Labrador"],
    "Sex": ["Male", "Female"],
    "Diagnosis.disease_term": ["Lymphoma", "Osteosarcoma"]
  }
}
```

**Response:**
```json
{
  "study": "study-name",
  "filters": {
    "Breed": ["Golden Retriever", "Labrador"],
    "Sex": ["Male", "Female"],
    "Diagnosis.disease_term": ["Lymphoma", "Osteosarcoma"]
  },
  "expected": {
    "count": 150,
    "ids": ["CASE-001", "CASE-002", "..."],
    "stats": {
      "participants": 150,
      "samples": 300,
      "files": 450,
      "caseFiles": 450,
      "studyFiles": 25
    },
    "statBar": {
      "participants": 150,
      "samples": 300,
      "studies": 1
    }
  },
  "meta": {
    "source": "tsv",
    "ts": "2024-01-15T10:30:00.000Z"
  }
}
```

### Example Usage

**Using curl:**
```bash
curl -X POST http://localhost:3000/mock-api \
  -H "Content-Type: application/json" \
  -d '{
    "study": "canine-commons",
    "filters": {
      "Breed": ["Golden Retriever"],
      "Sex": ["Male"]
    }
  }'
```

**Using Python:**
```python
import requests
import json

url = "http://localhost:3000/mock-api"  # or your Lambda URL
data = {
    "study": "canine-commons",
    "filters": {
        "Breed": ["Golden Retriever"],
        "Sex": ["Male"]
    }
}

response = requests.post(url, json=data)
result = response.json()
print(json.dumps(result, indent=2))
```

## Data Format

### Expected TSV Files in S3

The service expects the following TSV files in your S3 bucket:

- `{study}-case.tsv` - Case/participant information
- `{study}-demographic.tsv` - Demographic data (breed, sex, etc.)
- `{study}-diagnosis.tsv` - Diagnosis information
- `{study}-sample.tsv` - Sample data
- `{study}-file.tsv` - File metadata

### Column Mapping

The service automatically maps various column name patterns:

**Participant IDs:** `case_record_id`, `case.case_record_id`, `participant_id`, `case_id`, `submitter_id`, `id`

**Sample IDs:** `sample_id`, `aliquot_id`, `sample_submitter_id`, `id`

**File IDs:** `uuid`, `file_id`, `object_id`, `file_submitter_id`, `file_name`, `id`

**Disease Terms:** `disease_term`, `primary_diagnosis`, `diagnosis`

## Configuration

### Environment Variables

- `DATA_BUCKET` - S3 bucket containing TSV files
- `DATA_PREFIX` - S3 prefix for TSV files (default: `tsv/`)

### SAM Configuration

The `samconfig.toml` file contains deployment parameters:

```toml
[default.deploy.parameters]
stack_name = "expected-backend"
resolve_s3 = true
s3_prefix = "expected-backend"
region = "us-east-1"
confirm_changeset = true
capabilities = "CAPABILITY_IAM"
parameter_overrides = "DataBucketName=\"your-bucket\" DataPrefix=\"tsv/\""
```

## Error Handling

The service returns appropriate HTTP status codes:

- `200` - Success
- `500` - Server error (with error details in JSON response)

All responses are in JSON format, even errors, to ensure consistent API behavior.

## Development

### Adding New Filters

To add new filter types, modify `expected_backend/filter.py`:

1. Add the filter logic in `apply_filters()`
2. Update the column candidates in `expected_backend/loader.py` if needed
3. Test with the local server

### Adding New Statistics

To add new statistical calculations, modify `expected_backend/stats_bar.py`:

1. Add calculation functions
2. Update `build_expected_payload()` to include new stats
3. Update the response schema documentation

## Troubleshooting

### Common Issues

1. **S3 Access Denied**: Ensure your Lambda execution role has S3 read permissions
2. **Missing TSV Files**: Check that files exist in S3 with the correct naming pattern
3. **Column Mapping Issues**: Verify that your TSV files have recognizable column names
4. **Memory Issues**: Increase Lambda memory allocation in `template.yaml`

### Debugging

Enable debug logging by setting the `DEBUG` environment variable:

```bash
export DEBUG=1
python local_server.py
```

## License

This project is part of the autonomous UI validator system for clinical data exploration.
