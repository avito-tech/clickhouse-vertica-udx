
\set ON_ERROR_STOP on

-- check if new library installed without errors
CREATE OR REPLACE LIBRARY ClickhouseLibTest AS '/tmp/ClickhouseUdf.so';
CREATE or replace PARSER ClickhouseParserTest AS LANGUAGE 'C++' NAME 'ClickhouseParserFactory' LIBRARY ClickhouseLibTest;
CREATE or replace SOURCE VoidSourceTest AS LANGUAGE 'C++' NAME 'VoidSourceFactory' LIBRARY ClickhouseLibTest;
CREATE OR REPLACE TRANSFORM FUNCTION ClickhouseExportTest AS LANGUAGE 'C++' NAME 'ClickhouseExportFactory' LIBRARY ClickhouseLibTest;

-- replace old library
CREATE OR REPLACE LIBRARY ClickhouseLib AS '/tmp/ClickhouseUdf.so';
CREATE or replace PARSER ClickhouseParser AS LANGUAGE 'C++' NAME 'ClickhouseParserFactory' LIBRARY ClickhouseLib;
CREATE or replace SOURCE VoidSource AS LANGUAGE 'C++' NAME 'VoidSourceFactory' LIBRARY ClickhouseLib;
CREATE OR REPLACE TRANSFORM FUNCTION ClickhouseExport AS LANGUAGE 'C++' NAME 'ClickhouseExportFactory' LIBRARY ClickhouseLib;
CREATE OR REPLACE TRANSFORM FUNCTION ClickhousePipe AS LANGUAGE 'C++' NAME 'ClickhousePipeFactory' LIBRARY ClickhouseLib;
CREATE OR REPLACE TRANSFORM FUNCTION ThroughClickhouse AS LANGUAGE 'C++' NAME 'ClickhousePipeFactory' LIBRARY ClickhouseLib;

grant all on library ClickhouseLib to public;
GRANT all ON ALL FUNCTIONS IN SCHEMA public to public;

