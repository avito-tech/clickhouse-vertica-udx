
NAME     := ClickhouseUdf.so
CXXFLAGS := $(CXXFLAGS) -fabi-version=2 -D_GLIBCXX_USE_CXX11_ABI=0

INC      := -I /opt/vertica/sdk/include -I./src -I/usr/local/include -I/usr/local/curl/include
SRC      := src/source.cpp src/parser.cpp src/export.cpp src/utils.cpp src/curl_fopen.cpp /opt/vertica/sdk/include/Vertica.cpp

LIB      := -Wl,--whole-archive /usr/local/lib/libclickhouse-cpp-lib-static.a -Wl,--no-whole-archive
LIB      := $(LIB) -Wl,--whole-archive /usr/local/lib/libcityhash-lib.a -Wl,--no-whole-archive
LIB      := $(LIB) -Wl,--whole-archive /usr/local/lib/liblz4-lib.a -Wl,--no-whole-archive
LIB      := $(LIB) -Wl,-Bstatic -lstdc++ -lgcc -Wl,-Bdynamic
LIB      := $(LIB) -Wl,-Bstatic -L/usr/local/curl/lib -lcurl -Wl,-Bdynamic
LIB      := $(LIB) -lpthread

MACRO    := -DDSN_PATH_DEFAULT=/etc/secrets/dwh/clickhouse.toml
MACRO    := $(MACRO) -DDSN_SELECT_DEFAULT=dwh_clickstream 
MACRO    := $(MACRO) -DDSN_INSERT_DEFAULT=dwh_clickstream_loader 


build:
	g++ $(CXXFLAGS) -fPIC -static-libstdc++ -std=gnu++1z -O2 -shared -o $(NAME) $(SRC) $(INC) $(LIB) $(MACRO)


clickhouse-cpp:
	tar -xzf 3rd_part/clickhouse-cpp.tar.gz \
	&& cd clickhouse-cpp \
	&& mkdir ./build && cd ./build \
	&& cmake -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_CXX_FLAGS="$(CXXFLAGS) -static-libstdc++ -std=c++17 -fPIC" .. \
	&& make \
	&& make install \
	&& find contrib/ -name '*.a' -exec cp {} /usr/local/lib \; \
	&& cd ../.. && rm -rf clickhouse-cpp


libcurl:
	tar -xzf 3rd_part/curl-7.76.1.tar.gz \
	&& cd curl-7.76.1 \
	&& export CFLAGS="$(CFLAGS) -fPIC" \
	&& export CPPFLAGS="$(CXXFLAGS) -static-libstdc++ -fPIC" \
	&& ./configure --disable-shared --enable-static --prefix=/usr/local/curl --disable-ldap --disable-sspi --without-ssl --without-zlib \
	&& make \
	&& make install \
	&& cd .. && rm -rf curl-7.76.1


all: clean clickhouse-cpp libcurl build

clean:
	rm -rf clickhouse-cpp curl-7.76.1 || :

check-vsql:
ifndef VSQL_USER
	$(error VSQL_USER is undefined)
endif

install-udf: check-vsql
	sed -i "s#/tmp/ClickhouseUdf.so#$(PWD)/ClickhouseUdf.so#g" install.sql \
	&& /opt/vertica/bin/vsql -U $(VSQL_USER) -f install.sql

install-lib:
	python3 -m pip install 3rd_part/vertica_parser-9.2.36-py3-none-any.whl \
	&& python3 -m pip install -r service/requirements.txt \
	&& cp -rf service/bin/query_service.py /usr/bin/

run-service: check-vsql
	CREDITS=vertica://$(VSQL_USER)@127.0.0.1:5433 \
	nohup python3 /usr/bin/query_service.py &

