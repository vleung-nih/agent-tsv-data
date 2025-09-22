# Dockerfile
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

# Work in Lambda's task root so files land where the runtime expects
WORKDIR ${LAMBDA_TASK_ROOT}

# Install dependencies into the task root
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt -t .

# Copy your code
COPY app.py .
COPY expected_backend ${LAMBDA_TASK_ROOT}/expected_backend

# Lambda entrypoint (module.function)
CMD ["app.handler"]
