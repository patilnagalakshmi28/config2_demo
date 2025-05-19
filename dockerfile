FROM public.ecr.aws/lambda/python:3.11

# Install valkey-glide
RUN pip install --no-cache-dir valkey-glide && \
    python3 -c "import pkgutil; print([m.name for m in pkgutil.iter_modules()])"

# Copy your code
COPY app.py ${LAMBDA_TASK_ROOT}

# Set the Lambda handler
CMD ["app.lambda_handler"]
