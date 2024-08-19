/*
    siard2sqlite - A library to translate SIARD format 
    to sqlite-compliant SQL

    Immortal Database Access (iDA) EUROSTARS project

    Eladio Gutierrez, Sergio Romero, Oscar Plata
    University of Malaga, Spain

    May 2023
*/

#ifndef _SIARD2SQL_H_
#define _SIARD2SQL_H_

#ifdef __cplusplus
extern "C" {
#endif

    #define HUMANSIZE(x) (round(100*((double)(((x)>1e12)?((x)/1.0e12):((x)>1e9)?((x)/1.0e9):((x)>1.0e6)?((x)/1.0e6):((x)>1e3)?((x)/1.0e3):(x))))/100)
    #define HUMANPREFIX(x)  (((x)>1e12)?"T":((x)>1e9)?"G":((x)>1e6)?"M":((x)>1e3)?"K":"")

    // libsiardunzip
    int IDA_unzip(const char* siardfile, char *filename);
    int IDA_unzip_siard_full(const char *siardfile);
    int IDA_unzip_siard_metadata(const char* siardfile);
    void IDA_unzip_close_all();

    // libsiardxml
    char *IDA_get_siard_version_from_dir(const char *path_to_siard, char *buff, long size);
    int IDA_siard2sql(const char *siardfilein, const char* sqlfileout, const char *schema_filter);

#ifdef __cplusplus
}
#endif

#endif
