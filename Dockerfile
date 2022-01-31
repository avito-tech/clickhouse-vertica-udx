FROM vertica/vertica-ce:11.0.2-0

SHELL ["/bin/bash", "-c"]

RUN sudo yum install -y centos-release-scl \
 && sudo yum install -y devtoolset-8-gcc devtoolset-8-gcc-c++ \
 && scl enable devtoolset-8 -- bash \
 && source scl_source enable devtoolset-8 \
 && pushd . \
 && wget https://cmake.org/files/v3.12/cmake-3.12.3.tar.gz \
 && tar zxvf cmake-3.* \
 && cd cmake-3.* \
 && ./bootstrap --prefix=/usr \
 && make -j$(nproc) && sudo make install \
 && popd \
 && sudo yum install -y python3

COPY . /opt/vertica-udf
RUN cd /opt/vertica-udf  \
 && source scl_source enable devtoolset-8 \
 && sudo make all \
 && cp ClickhouseUdf.so /tmp \
 && sudo mkdir -p /docker-entrypoint-initdb.d \
 && sudo cp install.sql /docker-entrypoint-initdb.d/ \
 && sudo make install-lib \
 && printf '\! make -f /opt/vertica-udf/Makefile run-service VSQL_USER=dbadmin' \
    | sudo tee /docker-entrypoint-initdb.d/service.sql

ENTRYPOINT ["/bin/bash", "-c", "/home/dbadmin/docker-entrypoint.sh"]

