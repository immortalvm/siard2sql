# SIARD to sqlite SQL

A library to convert SIARD format to sqlite3-compliant SQL.

## Build
Type ```build.sh``` for help:

* ```./build.sh -c linux```: compile for linux

* ```./build.sh -c ivm```: (cross)compile for ivm64

If used flag ```-c```, a cleaning is made prior to building.

This script creates a building directory called  ```run-linux``` or ```run-ivm```
respectively, where executable, libraries and other files for the selected
architecture are placed.

## Cross compiling for ivm64

When cross-compiling for the ivm64 architecture you need to
prepare the environment:

* Add to the environment variable PATH, these 3 things:
  1. the path to the ivm64-gcc compiler and assembler (```ivm64-gcc```, ```ivm64-g++```, ```ivm64-as```) (generally ```<gcc-12.2.0 install dir>/bin```)
  1. the path to your favourite ivm emulator (e.g. ```ivm64-emu```)
  1. the path to the ivm in-ram filesystem generator (```ivm64-fsgen```, distributed together with the compiler as of version 3.2)
  <br/><br/>
* Optionally, you can define the environment IVM_EMU variable with your favourite
ivm emulator (e.g. export IVM_EMU="ivm64-emu" for the fast emulator, or export IVM_EMU="ivm run" to use the ivm implementation), and the IVM_AS variable with your favourite assembler (e.g. export IVM_AS=ivm64-as for the assembler integrated in the compiler or IVM_AS="ivm as" to use the ivm implementation.)

## Using this project stadalone

In the command line, you can invoke directly the resulting executable to convert a SIARD file to a sqlite-compliant SQL file.

The first argument is the siard input filename, the second one is the SQL output filename and a third optional argument allows filtering by schema name using a regexp; if no output file is specified, a list of all schemas in the SIARD file is printed:

  ```sh 
     siard2sql             # print a short help
     siard2sql file.siard  # list schemas in SIARD file  
     siard2sql file.siard out.sql                      # convert SIARD to sqlite3 SQL
     siard2sql file.siard out.sql schema_filter_regex  # convert filtering by schema name
  ```


For example, if you compiled for linux:

  ```run-linux/siard2sql  data/simpledb.siard out.sql```

In case a siard file contains more than one schema, a regex to filter the schema name can be used:

  ```run-linux/siard2sql  data/sakila.siard out.sql sakila$```

Several siard examples for testing are included in  ```run-<arch>/data```.

## Using the library

Library ```libsiard2sql.a``` is created in ```run-<arch>/lib``` which
provided the following C public API:

  ```c
    int IDA_unzip(const char* siardfile, char *filename);
    int IDA_unzip_siard_full(const char *siardfile);
    int IDA_unzip_siard_metadata(const char* siardfile);
    int IDA_siard2sql(const char *siardfilein, const char* sqlfileout, const char *schema_filter);
  ```

The main routine  ```IDA_siard2sql()``` is in charge of doing the conversion in two
steps: (1) the siard file is unzipped in a temporary directory, (2) the xml representation
is converted to SQL.

This describes the function ```IDA_siard2sql(siardfilein, sqlfileout, schema_filter)```:

```
Argument siardfilein can be:
  - A regular SIARD (.zip) file
  - A directory with the unzipped SIARD file (containing
    subdirectories 'header' and 'content')
    
Argument sqlfileout can be:
  - A regular file where to write SQL to
  - NULL: If sqlfileout is NULL, file /header/metadata.xml is only
    unzipped or searched in directory, and a summary of the schemas
    in metadata is printed instead of doing the conversion
    
The schema_filter is a regular expression to filter schemas by name;
only those schema names matching it will be converted. Use "" to not filter.
```

## Standards
SIARD2SQL has been tested successfully with SIARD 2.1 archives. It has been also tested with SIARD version 2.2.

Nevertheless, the support of some advanced features is experimental. Such advanced features include external archives, and complex data types not directly supported by sqlite3 (like arrays and user-defined data types):

- Non-empty complex type data are coded as _JSON_ strings.
- Empty complex types are coded as empty strings (for example, an empty array is stored as "").
- External files are limited to local filesystems, using POSIX path format (full URI not supported yet).

UTF-8 encoding is assumed for all SIARD XML files.

## Dependences

This project is self-contained, including the following third-party libraries
(under directory thirdparty); the corresponding libraries are created when building the
project and placed in ```run-<arch>/lib```:

* tinyxml2
* zlib

## References

* IVM C/C++ compiler and assembler (```ivm64-gcc, ivm64-g++, ivm64-as```): https://github.com/immortalvm/ivm-compiler
* Yet another (FAST) IVM emulator (```ivm64-emu```): https://github.com/immortalvm/yet-another-fast-ivm-emulator
* IVM in-ram filesystem (```ivm64-fsgen```): https://github.com/immortalvm/ivm-fs
* IVM F# implementation (ivm: assembler+emulator): https://github.com/immortalvm/ivm-implementations


