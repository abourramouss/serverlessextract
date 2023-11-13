# Stage 1: Build Environment
FROM python:3.8 AS builder

RUN apt-get update && apt-get install -y \
	g++ cmake libboost-all-dev libhdf5-serial-dev \
	libfftw3-dev libcfitsio-dev libarmadillo-dev \
	liblog4cplus-dev libopenblas-dev liblapack-dev \
	git wcslib-dev casacore-dev casacore-tools \
	libpython3-dev libgsl-dev liblua5.3-dev \
	libpng-dev libgtkmm-3.0-dev ninja-build \
	pkg-config swig wget unzip flex bison ninja-build \
	&& rm -rf /var/lib/apt/lists/*

# Install AOFlagger
RUN git clone https://gitlab.com/aroffringa/aoflagger.git /AOFlagger \
	&& cd /AOFlagger \
	&& mkdir build && cd build \
	&& cmake .. \
	&& make -j8 \
	&& make install \
	&& cd / && rm -rf /AOFlagger

# Install EveryBeam
RUN git clone https://git.astron.nl/RD/EveryBeam.git /EveryBeam \
	&& cd /EveryBeam \
	&& mkdir build && cd build \
	&& cmake .. \
	&& make -j8 \
	&& make install \
	&& cd / && rm -rf /EveryBeam

# Install WSClean
RUN git clone --recursive -j8 https://gitlab.com/aroffringa/wsclean.git /wsclean \
	&& cd /wsclean \
	&& mkdir build && cd build \
	&& cmake -G "Unix Makefiles" -DCMAKE_INSTALL_PREFIX=/usr \
	-DUSE_OPENMP=ON -DBUILD_GUI=OFF .. \
	&& make -j8 \
	&& make install \
	&& cd / && rm -rf /wsclean

# Install DP3
RUN git clone https://github.com/lofar-astron/DP3.git /DP3 \
	&& cd /DP3 \
	&& mkdir build && cd build \
	&& cmake .. \
	&& make -j8 \
	&& make install \
	&& cd / && rm -rf /DP3

# Stage 2: Runtime Environment
FROM python:3.8-slim

# Copy necessary binaries and libraries from builder stage
COPY --from=builder /usr/local /usr/local
ENV OPENBLAS_NUM_THREADS=1

# Install Python dependencies
RUN pip install --upgrade pip wheel setuptools \
	&& pip install --no-cache-dir \
	lithops awslambdaric boto3 redis httplib2 \
	requests numpy scipy pandas pika kafka-python \
	cloudpickle ps-mem tblib psutil s3path

# Set up the working directory
ARG FUNCTION_DIR="/function"
WORKDIR ${FUNCTION_DIR}
COPY lithops_lambda.zip ${FUNCTION_DIR}
RUN unzip lithops_lambda.zip \
	&& rm lithops_lambda.zip \
	&& mkdir handler \
	&& touch handler/__init__.py \
	&& mv entry_point.py handler/

# Set the entrypoint and command
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "handler.entry_point.lambda_handler" ]
