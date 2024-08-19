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

#ifndef UNZ_MAXFILENAMEINZIP
#define UNZ_MAXFILENAMEINZIP (256)
#endif

// From ida_miniunz_utils.cpp
unzFile get_open_zip_by_name(const char* zipname, int *open);
void IDA_ZIP_add_open_zip(unzFile uf, const char* zipname);
extern void IDA_ZIP_add_file_to_index(unzFile uf, const char* filename, unz_file_pos pos);
extern int IDA_ZIP_get_file_pos(unzFile uf, const char *filename, unz_file_pos *pos);
void IDA_ZIP_remove_open_zip(unzFile uf);
void IDA_ZIP_add_zip_pending_to_close(unzFile uf);
unzFile IDA_ZIP_get_zip_pending_to_close();
long IDA_ZIP_get_zip_number_of_entries(unzFile uf);
void IDA_ZIP_print_index(unzFile uf);

// Prototypes for this file
static int IDA_miniunz_create_index(unzFile uf);
static void IDA_miniunz_close(unzFile uf);
static unzFile IDA_miniunz_open_indexed(const char *zipfilename);
static int IDA_miniunz_do_extract_onefile_indexed(unzFile uf, const char *filename, int opt_extract_without_path, int opt_overwrite, const char *password);
static unzFile IDA_miniunz_open(const char *zipfilename);
static void IDA_miniunz_close_indexed(unzFile uf);
static int IDA_miniunz_do_extract(unzFile uf, int opt_extract_without_path, int opt_overwrite, const char *password);

// Public functions (to be used in libsiardunzip.c)

// Close all pending open zips and remove from the cache
void IDA_minunz_close_all_open_zip(){
    unzFile uf;
    while (uf = IDA_ZIP_get_zip_pending_to_close()){
        IDA_ZIP_remove_open_zip(uf);
        IDA_miniunz_close(uf);
    }
}

// Unzip a zip file (see miniunz.c)
// If argument filename is NULL extract the full zip content
int IDA_miniunz_do_unzip(const char *zipfilename, char* filename)
{
    const char *password=NULL;
    int opt_extract_without_path=0;
    int opt_overwrite=1;
    unzFile uf=NULL;
    int err = 1;
    if (filename) {
        // Unzip only one file
#if 1
        uf = IDA_miniunz_open_indexed(zipfilename);
        if (uf) {
            err = IDA_miniunz_do_extract_onefile_indexed(uf, filename, opt_extract_without_path, opt_overwrite,
                                                         password);
            IDA_miniunz_close_indexed(uf);
        }
#else
        // Without indexing (slower)
        uf = IDA_miniunz_open(zipfilename);
        err = IDA_miniunz_do_extract_onefile(uf, filename, opt_extract_without_path, opt_overwrite, password);
        IDA_miniunz_close(uf);
#endif
    } else {
        // Unzip everything
        uf = IDA_miniunz_open(zipfilename);
        err = IDA_miniunz_do_extract(uf, opt_extract_without_path, opt_overwrite, password);
        IDA_miniunz_close(uf);
    }
    return err;
}



// Static private functions (not to be used outside this file)

static unzFile IDA_miniunz_open(const char *zipfilename)
{
    char filename_try[MAXFILENAME+1] = "";
    unzFile uf;

    uf = NULL;
    if (zipfilename) {
        strncpy(filename_try, zipfilename, MAXFILENAME-1);
        /* strncpy doesnt append the trailing NULL, if the string is too long. */
        filename_try[MAXFILENAME] = '\0';
        uf = unzOpen64(zipfilename);
    }

    if (uf==NULL) {
        fprintf(stderr, "Path '%s' cannot be unzipped\n",zipfilename);
        return NULL;
    }

    fprintf(stderr, "%s opened\n",filename_try);
    return uf;
}

static unzFile IDA_miniunz_open_indexed(const char *zipfilename){
    unzFile uf;

    // Use the cached index if already open and indexed
    int isopen = 0;
    uf = get_open_zip_by_name(zipfilename, &isopen);
    if (isopen) {
        unzGoToFirstFile(uf); // Set the initial position as if you just open
        return uf; // The file is open
    }

    uf = IDA_miniunz_open(zipfilename);

    if (uf) {
        // TODO: check errors
        IDA_ZIP_add_open_zip(uf, zipfilename); // Add to open zipfile cache
        IDA_miniunz_create_index(uf); // Create its index

        printf("File '%s' open and indexed: found %ld entries\n", zipfilename, IDA_ZIP_get_zip_number_of_entries(uf));
        // Debugging
        //printf("----------------\n");
        //IDA_ZIP_print_index(uf);
        //printf("----------------\n");
    }

    return uf;
}

static void IDA_miniunz_close(unzFile uf)
{
    unzClose(uf);
}

static void IDA_miniunz_close_indexed(unzFile uf)
{
    IDA_ZIP_add_zip_pending_to_close(uf);
}

static int IDA_miniunz_do_extract(unzFile uf, int opt_extract_without_path, int opt_overwrite, const char *password)
{
    return do_extract(uf, opt_extract_without_path, opt_overwrite, password);
}

static int IDA_miniunz_do_extract_onefile(unzFile uf, const char *filename, int opt_extract_without_path,
                                   int opt_overwrite, const char *password)
{
    // This way is very slow as need to traverse all the entries of ZIP index
    return do_extract_onefile(uf, filename, opt_extract_without_path, opt_overwrite, password);
}

static int IDA_miniunz_do_extract_onefile_indexed(unzFile uf, const char *filename, int opt_extract_without_path,
                                   int opt_overwrite, const char *password)
{

    // Debugging
    // printf("Unzipping the file '%s'\n", filename);
    // printf("----------------\n");
    // IDA_ZIP_print_index(uf);
    // printf("----------------\n");

    unz_file_pos pos;
    int err = IDA_ZIP_get_file_pos(uf, filename, &pos);
    if (!err) {
        err = unzGoToFilePos(uf, &pos);
        if (!err) {
              return do_extract_currentfile(uf,&opt_extract_without_path, &opt_overwrite, password);
        } else {
            printf("Error going to position\n");
        }
    } else {
        printf("Error find position for file '%s'\n", filename);
    }

    printf("Error extracting file '%s'\n", filename);
    return UNZ_INTERNALERROR;
}
// Traverse all the files in one open zip, and add a pair (filename,position)
// to the index of such an open zip
static int IDA_miniunz_create_index(unzFile uf)
{
    long c=0;
    int err = unzGoToFirstFile(uf);
    while (err == UNZ_OK)
    {
        char currentFileName[UNZ_MAXFILENAMEINZIP + 1];
        err = unzGetCurrentFileInfo64(uf, NULL,
                                      currentFileName, sizeof(currentFileName) - 1,
                                      NULL, 0, NULL, 0);
        if (err == UNZ_OK)
        {
            unz_file_pos file_pos;
            err =  unzGetFilePos(uf, &file_pos);

            //fprintf(stdout, "%s \tnof=%ld \tposindir=%ld\n", currentFileName, file_pos.num_of_file, file_pos.pos_in_zip_directory); // Debug
            if (!(c++%1000)) {printf("."); fflush(stdout);} // Debug
            IDA_ZIP_add_file_to_index(uf, currentFileName, file_pos);

            if (err == UNZ_OK) {
                err = unzGoToNextFile(uf);
            }
        }
    }
    unzGoToFirstFile(uf); // Let the same position after open
    fprintf(stdout,"\n"); // Debug
    return err;
}



// These functions are not used yet (or any more)

// This implementation run the miniunz program calling the main() function
// If argument filename is NULL extract the full zip content
static int IDA_miniunz_do_unzip_main(const char *zipfilename, char* filename)
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

// Extract the current file of a zip into a memory buffer, and return a pointer to this buffer
// Memory is allocated for this buffer, consequently needs to be deallocated after its use
//
// This function is based on  do_extract_currentfile() from miniuz.c
static char *ida_do_extract_currentfile_to_buffer(unzFile  uf, const int* popt_extract_without_path, int* popt_overwrite, const char* password, long *size_buf)
{
    char filename_inzip[256];
    int err=UNZ_OK;
    void* buf;
    *size_buf = 0;

    unz_file_info64 file_info;
    err = unzGetCurrentFileInfo64(uf,&file_info,filename_inzip,sizeof(filename_inzip),NULL,0,NULL,0);

    if (err!=UNZ_OK){
        printf("error %d with zipfile in unzGetCurrentFileInfo\n",err);
        return NULL;
    }

    err = unzOpenCurrentFilePassword(uf,password);
    if (err!=UNZ_OK){
        printf("error %d with zipfile in unzOpenCurrentFilePassword\n",err);
    }

    *size_buf = file_info.uncompressed_size;
    buf = (void*)malloc(*size_buf * sizeof(char));
    if (buf==NULL){
        printf("Error allocating memory\n");
        return NULL;
    }

    /*
      Read bytes from the current file (opened by unzOpenCurrentFile)
      buf contain buffer where data must be copied
      len the size of buf.

      return the number of byte copied if somes bytes are copied
      return 0 if the end of file was reached
      return <0 with error code if there is an error
        (UNZ_ERRNO for IO error, or zLib error for uncompress error)
    */
    err = unzReadCurrentFile(uf,buf,*size_buf);

    if (err < *size_buf) {
        printf("error %d (must be %ld) with zipfile in unzReadCurrentFile\n",err, *size_buf);
        if (buf) free(buf);
        return NULL;
    }

    return buf;
}

static char* IDA_miniunz_do_extract_onefile_indexed_to_buffer(unzFile uf, const char *filename, int opt_extract_without_path,
                                           int opt_overwrite, const char *password, long *size_buf)
{
    unz_file_pos pos;
    int err = IDA_ZIP_get_file_pos(uf, filename, &pos);
    if (!err) {
        err = unzGoToFilePos(uf, &pos);
        if (!err) {
            return ida_do_extract_currentfile_to_buffer(uf,&opt_extract_without_path, &opt_overwrite, password, size_buf);
        } else {
            printf("Error going to position\n");
        }
    } else {
        printf("Error find position for file '%s'\n", filename);
    }

    printf("Error extracting file '%s'\n", filename);
    return NULL;
}

// Unzip one zip file into an allocated buffer
// Return a pointer to the allocated buffer (it needs to be release), and its size in size_buf
// Return NULL if error
static char* IDA_miniunz_do_unzip_to_buffer(const char *zipfilename, char* filename, long *size_buf)
{
    const char *password=NULL;
    int opt_extract_without_path=0;
    int opt_overwrite=1;
    unzFile uf=NULL;
    char* buff = NULL;
    if (filename) {
        uf = IDA_miniunz_open_indexed(zipfilename);
        if (uf) {
            buff = IDA_miniunz_do_extract_onefile_indexed_to_buffer(uf, filename, opt_extract_without_path, opt_overwrite,
                                                         password, size_buf);
            IDA_miniunz_close_indexed(uf);
        }
    }
    return buff;
}
