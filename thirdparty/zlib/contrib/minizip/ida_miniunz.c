/*
    siard2sql - A library to translate SIARD format
    to sqlite-compliant SQL

    Immortal Database Access (iDA) EUROSTARS project

    Eladio Gutierrez, Sergio Romero, Oscar Plata
    University of Malaga, Spain

    Oct 2023
*/

#ifndef main
#define main _IDA_minunz_main_
#endif
#include "miniunz.c"

// IDA/IVM: This is the public interface

unzFile IDA_miniunz_open(const char *zipfilename){
    char filename_try[MAXFILENAME+1] = "";
    unzFile uf=NULL;

    if (zipfilename) {
        strncpy(filename_try, zipfilename, MAXFILENAME-1);
        /* strncpy doesnt append the trailing NULL, if the string is too long. */
        filename_try[MAXFILENAME] = '\0';
        uf = unzOpen64(zipfilename);
        if (uf==NULL) {
            strcat(filename_try,".zip");
            uf = unzOpen64(filename_try);
        }
    }

    if (uf==NULL) {
        fprintf(stderr, "Cannot unzip '%s' or '%s.zip'\n",zipfilename,zipfilename);
        return NULL;
    }

    fprintf(stderr, "%s opened\n",filename_try);
    return uf;
}

int IDA_miniunz_do_extract(unzFile uf, int opt_extract_without_path, int opt_overwrite, const char *password)
{
    return do_extract(uf, opt_extract_without_path, opt_overwrite, password);
}

int IDA_miniunz_do_extract_onefile(unzFile uf, const char *filename, int opt_extract_without_path,
                                   int opt_overwrite, const char *password)
{
    return do_extract_onefile(uf, filename, opt_extract_without_path, opt_overwrite, password);
}

// Unzip a zip file (see miniunz.c)
// If argument filename is NULL extract the full zip content
int IDA_miniunz_do_unzip(const char *zipfilename, char* filename)
{
    const char *password=NULL;
    int ret_value;
    int opt_do_extract_withoutpath=0;
    int opt_overwrite=1;  /* OVERWRITE */
    unzFile uf=NULL;

    uf = IDA_miniunz_open(zipfilename);

    if (uf) {
        if (filename) {
            // Unzip only one file
            ret_value = IDA_miniunz_do_extract_onefile(uf, filename, opt_do_extract_withoutpath, opt_overwrite,
                                                       password);
        } else {
            // Unzip everything
            ret_value = IDA_miniunz_do_extract(uf, opt_do_extract_withoutpath, opt_overwrite, password);
        }
        unzClose(uf);
        return ret_value;
    } else {
        return 1;
    }
}

// This implementation run the miniunz program calling the main() function
// If argument filename is NULL extract the full zip content
int IDA_miniunz_do_unzip_main(const char *zipfilename, char* filename)
{
    char* argv[16];
    int argc = 0;
    argv[argc++] = "miniunz";
    argv[argc++] = "-x";
    argv[argc++] = "-o";
    argv[argc++] = (char*) zipfilename;
    if (filename) {
        argv[argc++] = filename;
    }
    argv[argc] = 0;
    return main(argc, argv);
}