FROM public.ecr.aws/lambda/python:3.11

# Install valkey-glide and dependencies
RUN pip install --no-cache-dir valkey-glide boto3 && \
    python3 -c "from glide import Glide; print('✅ glide module imported successfully')"

# Copy your Lambda function code into the Lambda task root directory
COPY app.py ${LAMBDA_TASK_ROOT}/

# Set the Lambda handler (module.function)
CMD ["app.lambda_handler"]
