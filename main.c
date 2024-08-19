/*
    siard2sqlite - A library to translate SIARD format 
    to sqlite-compliant SQL

    Immortal Database Access (iDA) EUROSTARS project

    Eladio Gutierrez, Sergio Romero, Oscar Plata
    University of Malaga, Spain

    May 2023 - Jan 2024
*/
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <libgen.h>
#include <unistd.h>
#include <ctype.h>
#include <unzip.h>

#include "siard2sql.h"

//------------------------------------------------------------------

int main(int argc, char *argv[]) {
    char *siardfile=NULL, *sqlfile=NULL, *schema_filter = "";

    if (argc < 2) {
        fprintf(stderr, "Usage: %s siardfile.siard [sqlitefile.sql [schema filter regex]]\n", argv[0]);
        fprintf(stderr, "       If SQL output file is omitted, only print schemas found in siard file\n");
        return EXIT_FAILURE;
    } else {
        siardfile = argv[1];
        if (argv[2]){ 
            sqlfile = argv[2];
            if (argv[3]) schema_filter = argv[3];
        }
    }

    // SIARD -> SQL
    IDA_siard2sql(siardfile, sqlfile, schema_filter);

    // Dump full sqlfile (or not)
    const int dump_full_sqlite = 0;
    FILE *f = fopen(sqlfile, "r");
    if (f){
        char buff[512];
        int n;
        while ((n = fread(buff, 1, 512, f))) {
            if (dump_full_sqlite)
                fwrite(buff, 1, n, stdout);
        }
    }

    return EXIT_SUCCESS;
}
