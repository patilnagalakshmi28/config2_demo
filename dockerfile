FROM public.ecr.aws/lambda/python:3.11

# Install valkey client
RUN pip install valkey-glide

# Copy code
COPY app.py ${LAMBDA_TASK_ROOT}

# Set the handler
CMD ["app.lambda_handler"]
