/*
    siard2sql - A library to translate SIARD format
    to sqlite-compliant SQL

    Immortal Database Access (iDA) EUROSTARS project

    Eladio Gutierrez, Sergio Romero, Oscar Plata
    University of Malaga, Spain

    Aug 2023
*/

#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <dirent.h>
#include <libgen.h>

// Functions to extract zip files defined in thirdparty/zlib/contrib/minizip/miniunz.c
extern int IDA_miniunz_do_unzip(const char *zipfilename, char *filename);
extern void IDA_minunz_close_all_open_zip();

// Unzip a (SIARD) zip file (see miniunz.c)
// If filename != NULL, only this particular file is extracted,
// otherwise the file is fully unzipped
int IDA_unzip(const char* siardfile, char *filename)
{
    return IDA_miniunz_do_unzip(siardfile, filename);
}

// Unzip a SIARD file (see miniunz.c)
int IDA_unzip_siard_full(const char* siardfile)
{
    return IDA_unzip(siardfile, NULL);
}

// Extract ONLY header/metadata.xml from a SIARD file (see miniunz.c)
int IDA_unzip_siard_metadata(const char* siardfile)
{
    return IDA_unzip(siardfile, "header/metadata.xml");
}

// The string path_to_siard must be the directory contaning the unzipped
// siard, that is, where folders "./header" and "./medatada" are placed
char* IDA_get_siard_version_from_dir(const char* path_to_siard, char* buff, long size)
{
    const char *siard_header_version_rel = "./header/siardversion";

    char path_to_siard_version[PATH_MAX];
    strncpy(path_to_siard_version, path_to_siard, PATH_MAX - 1);
    strcat(path_to_siard_version, "/");
    strncat(path_to_siard_version, siard_header_version_rel, PATH_MAX - 1);
    path_to_siard_version[PATH_MAX - 1] = '\0';

    struct dirent **files;
    int nfiles = scandir(path_to_siard_version, &files, NULL, alphasort);
    if (nfiles == -1) {
        //perror("scandir");
        return NULL;
    }

    int found_version = 0;
    for (long k=0; k<nfiles; k++){
        struct dirent *d = files[k];
        // Remember: ivmfs's d->d_name may have the full path name
        // Get its name with basename()
        char bn[PATH_MAX];
        strncpy(bn, d->d_name, PATH_MAX); bn[PATH_MAX-1]='\0';
        strncpy(buff, basename(bn), size); buff[size-1]='\0';
        if (buff && *buff && buff[0] != '.'){
            found_version = 1;
            break;
        }
        free(d);
    }
    free(files);

    if (found_version)
        return buff;
    else
        return NULL;
}

// Public function to close all indexed (cached) open zip
void IDA_unzip_close_all(){
    IDA_minunz_close_all_open_zip();
}