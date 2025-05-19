FROM public.ecr.aws/lambda/python:3.11

# Install OS packages required to compile Python packages
RUN yum install -y \
    gcc \
    gcc-c++ \
    make \
    python3-devel \
    openssl-devel \
    libffi-devel \
    && yum clean all

# Install valkey-glide and boto3
RUN pip install --no-cache-dir valkey-glide boto3 && \
    python3 -c "from glide import GlideClient; print('✅ glide module imported successfully')"

# Copy the Lambda function code
COPY app.py ${LAMBDA_TASK_ROOT}

# Set the Lambda handler
CMD ["app.lambda_handler"]
