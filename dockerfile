FROM public.ecr.aws/lambda/python:3.11

# Install valkey-glide
RUN pip install --no-cache-dir valkey-glide && \
    python3 -c "import valkey_glide; print('✅ valkey-glide is installed')"

# Copy your code
COPY app.py ${LAMBDA_TASK_ROOT}

# Set the Lambda handler
CMD ["app.lambda_handler"]
