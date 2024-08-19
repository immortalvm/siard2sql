/*
    siard2sql - A library to translate SIARD format
    to sqlite-compliant SQL

    Immortal Database Access (iDA) EUROSTARS project

    Eladio Gutierrez, Sergio Romero, Oscar Plata
    University of Malaga, Spain

    Aug 2023 - Jul 2024
*/

// Compile for ivm:
//   ivm64-g++ lib.cpp -c ; ivm64-gcc libroae.o main.c -lstd++
// or:
//   ivm64-gcc main.c -c ; ivm64-g++ main.o libroae.cpp

#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <vector>
#include <queue>
#include <set>
#include <unordered_map>
#include <regex>
#include <iterator>
#include <algorithm>

#include <cstdio>
#include <cstdarg>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <cstdarg>
#include <cmath>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <libgen.h>
#include <cassert>

#include "tinyxml2.h"
#include "siard2sql.h"

#if !defined(_GNU_SOURCE)
extern "C" char *strcasestr(const char*, const char *);
#endif

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"

// Define IDA_FULL_UNZIP to unzip the full siard zip, instead of extracting file by file
// In general unziping file by files is faster, so it is not recommended unzip the whole siard
#define IDA_FULL_UNZIP___

using namespace tinyxml2;
using namespace std;

namespace IDA {

    static long dbgc1=0; // Debug, a counter used to print some messages

    // Some useful methods to use when parsing siard format
    class IDA_siard_utils{
    public:

        enum SQLITE_COLTYPES {COLTYPE_BLOB, COLTYPE_NUMERIC, COLTYPE_INTEGER, COLTYPE_REAL, COLTYPE_TEXT};

        static string coltype_to_str(enum SQLITE_COLTYPES t)
        {
            switch(t) {
                case COLTYPE_BLOB:
                    return " BLOB";
                    break;
                case COLTYPE_NUMERIC:
                    return " NUMERIC";
                    break;
                case COLTYPE_INTEGER:
                    return " INTEGER";
                    break;
                case COLTYPE_REAL:
                    return " REAL";
                    break;
                case COLTYPE_TEXT:
                default:
                    return " TEXT";
                    break;
            }
        }

        // Convert SIARD column type to sqlite3 types
        // See:
        //   https://siard.dilcis.eu/SIARD%202.2/SIARD%202.2.pdf (page 18)
        //   https://www.sqlite.org/draft/datatype3.html
        static enum SQLITE_COLTYPES siard_type_to_sqlite3(string s)
        {
            /* These are the SIARD types to be mapped to the 5 sqlite3 types:
                BIGINT
                BOOLEAN
                INTEGER, INT
                SMALLINT

                DECIMAL(...), DEC(...)
                NUMERIC(...)

                DOUBLE PRECISION
                FLOAT(p)
                REAL

                CHARACTER VARYING(...), CHAR VARYING(...), VARCHAR(...)
                CHARACTER(...), CHAR(...)
                XML
                CHARACTER LARGE OBJECT(...), CLOB(...)

                BINARY LARGE OBJECT(...), BLOB(...)
                BINARY VARYING(...), VARBINARY(...)
                BINARY(…)

                NATIONAL CHARACTER LARGE OBJECT(...), NCHAR LARGE OBJECT(...), NCLOB(...)
                NATIONAL CHARACTER VARYING(...), NATIONAL CHAR VARYING(...), NCHAR VARYING(...)
                NATIONAL CHARACTER(...), NCHAR(...), NATIONAL CHAR(...),

                TIME(...)
                TIME WITH TIME ZONE(...)
                TIMESTAMP(...)
                TIMESTAMP WITH TIME ZONE(...)

                INTERVAL <start> [TO <end>]
                DATE
                DATALINK
            */

            // There are 5 sqlite3 main types (column affinity): TEXT, NUMERIC, INTEGER, REAL, BLOB
            // Let us map them
            //static regex re_int("NUMERIC|INT|BOOL|DECIMAL|DEC\\s*\\(");
            static regex re_int(R"((BIG|SMALL)INT|INTEGER|\bINT\b|BOOL)");
            static regex re_numeric(R"(NUMERIC|DECIMAL|DEC\s*\()");
            static regex re_real("DOUBLE|FLOAT|REAL");
            static regex re_text( "CHARACTER|CHAR|CLOB|VARCHAR" );
            static regex re_blob( "BINARY|BLOB|VARBINARY");

            // Cache SIARD simple types to sqlite3 because regex operations are not so fast
            static unordered_map<string, enum SQLITE_COLTYPES> typecache;

            enum SQLITE_COLTYPES ret;

            try {
                // First check the type cache
                ret = typecache.at(s);
                return ret;
            } catch (...) {
            }

            if (regex_search(s, re_int)) {
                ret =  COLTYPE_INTEGER;
            }
            else if (regex_search(s, re_numeric)) {
                ret =  COLTYPE_NUMERIC;
            }
            else if (regex_search(s, re_real)) {
                ret =  COLTYPE_REAL;
            }
            else if (regex_search(s, re_blob)) {
                ret =  COLTYPE_BLOB;
            }
            else {
                // text by default
                ret =  COLTYPE_TEXT;
            }
            typecache[s] = ret;
            return ret;
        }

        // Convert a string to a sqlite3 BLOB hex literal
        // "SOS" -> "X'534f53'"
        static string string_to_blob_literal(const string &s)
        {
            std::ostringstream ss;
            ss << "X'";
            for (unsigned long k=0; k<s.size(); k++){
               ss << setfill('0') << setw(2) << hex << (unsigned long)(unsigned char)s[k];
            }
            ss << "'";
            return ss.str();
        }

        // Convert a uint_8 array (may include NULLs) to a sqlite3 BLOB hex literal
        // "SOS\0" -> "X'534f5300'"
        static string char_array_to_blob_literal(const uint8_t *s, unsigned long size)
        {
            std::ostringstream ss;
            ss << "X'";
            if (s) {
                for (unsigned long k = 0; k < size; k++) {
                    ss << setfill('0') << setw(2) << hex << (unsigned long) (uint8_t) s[k];
                }
            }
            ss << "'";
            return ss.str();
        }

        // Convert a uint_8 array (may include NULLs) to a sqlite3 BLOB hex literal
        // appending it to the string a
        // "SOS\0" -> "X'534f5300'"
        static void char_array_to_blob_literal_append(const uint8_t *a, unsigned long size, string &s)
        {
            s.append("X'");
            if (a) {
                for (unsigned long k = 0; k < size; k++) {
                    char uu[3];
                    sprintf(uu, "%02x", (uint8_t)a[k]);
                    s.append(uu);
                }
            }
            s.append("'");
            return;
        }

        #define FILE_BLOB_BUFF_SIZE (1024*10)

        // Convert the content of a file to a sqlite3 BLOB hex literal
        // "SOS" -> "X'534f53'"
        static string file_to_blob_literal(const string &file)
        {
            unsigned char buf[FILE_BLOB_BUFF_SIZE]; // This MUST be unsigned
            FILE *f = fopen(file.c_str(), "r");
            if (!f) {
                cerr << "Error: opening '" << file << "' (notice: perhaps external file)" << endl;
                return "X''";
            }
            std::ostringstream ss;
            ss << "X'";
            long n;
            while ((n = fread(buf, 1, FILE_BLOB_BUFF_SIZE, f )) > 0) {
                for (long k=0; k<n; k++){
                    char uu[3];
                    sprintf(uu, "%02x", (unsigned char)buf[k]);
                    ss << uu;
                    //ss << setfill('0') << setw(2) << hex << (unsigned long)(unsigned char)buf[k];
                }
            }
            fclose(f);
            ss << "'";
            return ss.str();
        }

        // Convert the content of a file to a sqlite3 BLOB hex literal
        // appending it to the string s
        // "SOS" -> "X'534f53'"
        static void file_to_blob_literal_append(const string &file, string &s)
        {
            unsigned char buf[FILE_BLOB_BUFF_SIZE]; // This MUST be unsigned
            FILE *f = fopen(file.c_str(), "r");
            if (!f) {
                cerr << "Error: opening '" << file << "' (notice: perhaps external file)" << endl;
                s.append("X''");
                return;
            }
            s.append("X'");
            long n, n4;
            std::ostringstream ss;
            while ((n = fread(buf, 1, FILE_BLOB_BUFF_SIZE, f )) > 0) {
                n4 = n/4;
                for (long k=0; k<n4; k++){
                    char uu[2*4+1];
                    sprintf(uu, "%02x%02x%02x%02x",
                                           (unsigned char)buf[k*4], (unsigned char)buf[k*4+1],
                                           (unsigned char)buf[k*4+2], (unsigned char)buf[k*4+3]);
                    s.append(uu);
                    //ss << setfill('0') << setw(2) << hex << (unsigned long)(unsigned char)buf[k];
                }
                for (long k=n4*4; k<n; k++){
                    char uu[3];
                    sprintf(uu, "%02x", (unsigned char)buf[k]);
                    s.append(uu);
                    //ss << setfill('0') << setw(2) << hex << (unsigned long)(unsigned char)buf[k];
                }

                //-- ss.str(string()); // Clear stringstream
                //-- for (long k=0; k<n; k++){
                //--     ss << setfill('0') << setw(2) << hex << (unsigned long)(unsigned char)buf[k];
                //-- }
                //-- s.append(ss.str());
                //-- FREE_ALLOCA();
            }
            fclose(f);
            s.append("'");
        }

        // Enclose string in single quotes by escaping
        // the existing single quotes, in order to use the
        // input string in sqlite
        // Escaping ' in sqlite is doubling it: ''
        static string enclose_sqlite_single_quote(string s){
            static regex e("'");
            return "'" + regex_replace(s, e, "''") + "'";
        }

        #define siard_special(s) (!strncmp(s, "\\u00", 4))

        //  Return true if a SIARD coded string has "special" chars, of the form \u005c,
        //  and consequently it needs to be decoded
        static bool has_siard_special_chars(const string &siard_str){
            bool has_specials = false;
            const char *s_encod = siard_str.c_str();
            for (unsigned long i = 0; i < strlen(s_encod); i++) {
                if (siard_special(&s_encod[i])) {
                    has_specials = true;
                    break;
                }
            }
            return has_specials;
        }

        //  Decode a SIARD coded string allocating the decoded form in an
        //  uint8_t array (remember that the char 0 may appear decoded, so
        //  we cannot use a string as 0 is the mark for its end). The returned
        //  array must be deallocated after its use.
        //
        //  SIARD encoding G_3.3-4:
        //  Characters that cannot be represented in UNICODE (codes 0-8,
        //  14-31, 127-159), as well as the escape character '\' and
        //  multiple space characters are escaped as u00<xx> in XML. Quote,
        //  less than, greater than, and ampersand are represented in XML
        //  as entity references.
        //
        //  ==========      ===================
        //  Original        Characters in
        //  characters      in the SIARD format
        //  ==========      ===================
        //  0 to 8          \u0000 to \u0008
        //  14 to 31        \u000E to \u001F
        //  32              \u0020, for multiple spaces
        //  "               &quot;
        //  &               &amp;
        //  '               &apos;
        //  <               &lt;
        //  >               &gt;
        //  \               \u005c
        //  127 to 159      \u007F to \u009F
        //
        // Notice that XML entities are already decoded by the XML parser (tinyxml2)
        static uint8_t* siard_decode(const string &siard_str, long &size, bool &has_specials){
            has_specials = false;
            size = 0;
            if (siard_str.empty()){
                return NULL;
            }
            const char *s_encod = siard_str.c_str();
            uint8_t *s_decod = (uint8_t*) malloc(strlen(s_encod) * sizeof(char));
            if (s_decod) {
                for (unsigned long i = 0; i < strlen(s_encod); i++) {
                    if (siard_special(&s_encod[i])) {
                        has_specials = true;
                        char hex[5] = {s_encod[i + 2], s_encod[i + 3], s_encod[i + 4], s_encod[i + 5], '\0'};
                        unsigned char val = strtol(hex, NULL, 16);
                        s_decod[size++] = val;
                        i += 5;
                    } else {
                        s_decod[size++] = s_encod[i];
                    }
                }
            }
            return s_decod;
        }

    }; /* class IDA_siard_utils */

    // Some useful methods to use when parsing
    class IDA_parsing_utils{
    public:
        static string remove_comments(const string &s){
            static regex re_comment("#.*$");
            return regex_replace(s, re_comment, "");
        }

        static const string WHITESPACE;

        static string ltrim(const string &s){
            size_t start = s.find_first_not_of(WHITESPACE);
            return (start == string::npos) ? "" : s.substr(start);
        }

        static string rtrim(const string &s){
            size_t end = s.find_last_not_of(WHITESPACE);
            return (end == string::npos) ? "" : s.substr(0, end + 1);
        }

        static string trim(const string &s){
            return rtrim(ltrim(s));
        }

        // Return a never-matching regex in case a regex expression is not valid
        // to avoid throwing an exception
        static const char* validate_regex(const char *s) {
            try {
                regex re(s);
                return s;
            }
            catch (...) {
                return "$^"; // A regexp string impossible to match
            }
        }

        // Return if a string is a valid regexp (non-valid regexps raise exceptions)
        static bool is_valid_regex(const char *s) {
            try {
                regex re(s);
                return true;
            }
            catch (...) {
                return false;
            }
        }

        // Return if a string is prefix of another one
        static bool is_prefix(const string &prefix, const string &str)
        {
            auto mm = std::mismatch(prefix.begin(), prefix.end(), str.begin());
            return mm.first == prefix.end();
        }

    }; /* class IDA_parsing_utils */
    // Static member declarations
    const string IDA_parsing_utils::WHITESPACE = " \n\r\t\f\v";

    // Some useful methods when working with the file system
    class IDA_file_utils{
    public:
        // Return either the dirname, or empty string if there is a problem
        static string get_dirname(const string &s)
        {
            if (s.size() >= PATH_MAX)
                return "";
            char buff1[PATH_MAX];
            strcpy(buff1, s.c_str());
            char *d = dirname(buff1);
            if (d)
                return d;
            return "";
        }

        // Return either the basename, or empty string if there is a problem
        static string get_basename(const string &s)
        {
            if (s.size() >= PATH_MAX)
                return "";
            char buff1[PATH_MAX];
            strcpy(buff1, s.c_str());
            char *d = basename(buff1);
            if (d)
                return d;
            return "";
        }

        // Return either the realpath, or empty string if no realpath exists
        static string get_realpath(const string &s)
        {
            if (s.size() >= PATH_MAX)
                return "";
            char buff1[PATH_MAX], buff2[PATH_MAX];
            strcpy(buff1, s.c_str());
            char *r = realpath(buff1, buff2);
            if (r)
                return r;
            return "";
        }

        // Dump a text file to stdout
        static void cat(string filename)
        {
            ifstream f(filename);
            if (f.is_open()) {
                cout << f.rdbuf();
                f.close();
            }
        }

        static stack<string> dirstack;

        static int pushd(string dir)
        {
            char cwd[PATH_MAX];
            if (!getcwd(cwd, PATH_MAX - 1)) {
                return -1;
            }
            int err = chdir(dir.c_str());
            if (!err) dirstack.push(cwd); // chdir OK
            return err;
        }

        static int popd()
        {
            string topcwd = dirstack.top();
            dirstack.pop();
            return chdir(topcwd.c_str());
        }

        // Recursive file deletion (like rm -rf)
        // Use infix to avoid accidental deletions, it must be a substring
        // contained in the path to be deleted
        static int rrm(const string &s, const string &infix)
        {
            const char *path = s.c_str();
            if (!path || !*path) return 0; // Null or empty string: do nothing

            if (!infix.empty()) {
                string rl = get_realpath(s);
                if (rl.find(infix) == std::string::npos) {
                    cerr << "rrm: infix '" << infix << " not found in '" << s << "' or the directory cannot be deleted"
                         << endl;
                    return -1;
                }
            }

            char d[PATH_MAX];
            strcpy(d, path);

            // If path is file or link, it can be deleted directly
            int ret = unlink(d);
            if (ret >= 0) {
                return 0;
            }

            // Check if it is a directory
            DIR *od = opendir(d);
            if (od){
                closedir(od);
            } else {
                // It should be a directory, but it is not possible to open it
                return -1;
            }

            // It must be a readable directory at this point

            // Scan directory
            struct dirent **files;
            int nfiles = scandir(d, &files, NULL, alphasort);
            if (nfiles == -1) {
                //perror("scandir");
                return -1;
            }

            long status = 0;

            // Save current directory
            char currwd[PATH_MAX];
            char *w = getcwd(currwd, PATH_MAX-1);
            if (!w) { return -1; }

            // Remove children recursively
            if (chdir(d) == 0) {
                for (long k=0; k<nfiles; k++){
                    struct dirent *dd = files[k];
                    // Ignore . and .. , to avoid infinite recursion
                    if (strcmp(dd->d_name, ".") && strcmp(dd->d_name, "..")) {
                        #ifndef __ivm64__
                        status |= rrm(dd->d_name, infix);
                        #else
                        status |= rrm(dd->d_name, ""); // Only test infix in the first recursive call
                        #endif
                    }
                    free(dd);
                }
                if (chdir(w) < 0) return -1;
            }

            // Remove the dir itself
            ret = rmdir(d);
            if (ret < 0) {
                perror("rmdir");
                fprintf(stderr, "Removing dir '%s' FAILED!\n", d);
            }
            status |= ret;

            free(files);
            return status;
        }

        static bool is_absolute(const string &path){
            // TODO: generalize to URIs like file://....
            // According to POSIX.1-2017: Absolute Pathname: A pathname beginning with a
            // single or more than two <slash> characters
            // https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html
            // Note that the windows definition can differ from this
            if (!path.empty() && '/' == path[0]){
                return true;
            }
            return false;
        }

        static bool is_directory(const string &path)
        {
            struct stat s;
            if (::stat(path.c_str(), &s) != 0)
                return false;
            return S_ISDIR(s.st_mode);
        }

        // Get and optionally print the free space over the heap (until the top of stack)
        // For debugging purposes
        static unsigned long freeheap(int print=0)
        {
            char *pbrk = (char*)sbrk(0);
            char *pstk = (char*)&print;
            static char *pstk0 = NULL;
            if (!pstk0) {
                pstk0 = pstk; // Value of SP the first time this method is called
            }
            long freeheap  = pstk-pbrk;
            long freestack = pstk0-pstk;
            if (print) {
                fprintf(stderr, "Info: %.2f%sB of free space over heap (SP=%p, stack size=%.2f%sB)\n\n",
                          HUMANSIZE(freeheap), HUMANPREFIX(freeheap), pstk, HUMANSIZE(freestack), HUMANPREFIX(freestack));
            }
            return freeheap;
        }

    private:
        // Functions resolve_path_internal() and realpath_internal() are based on the
        // newlib routines by Werner Almesberger
        // This internal version does stat the file names or not (only expand the path
        // syntactically, in order to avoid infinite recursions) depending on argument
        // 'nocheck'
        static int resolve_path_internal(char *path, char *result, char *pos, bool nocheck) {
            long max_link_length = PATH_MAX;

            if (*path == '/') {
                *result = '/';
                pos = result + 1;
                path++;
            }
            *pos = 0;
            if (!*path) return 0;
            while (1) {
                char *slash;
                struct stat st;

                slash = *path ? strchr(path, '/') : NULL;
                if (slash) *slash = 0;

                if (!path[0] || (path[0] == '.' &&
                                 (!path[1] || (path[1] == '.' && !path[2])))) {
                    pos--;
                    if (pos != result && path[0] && path[1])
                        while (*--pos != '/');
                } else {
                    strcpy(pos, path);
                    if (!nocheck) {
                        if (lstat(result, &st) < 0) return -1;
                        if (S_ISLNK(st.st_mode)) {
                            char buf[PATH_MAX];
                            if (readlink(result, buf, sizeof(buf)) < 0) return -1;
                            max_link_length -= strnlen(buf, sizeof(buf)) + 2;
                            if (max_link_length <= 0) {
                                set_errno(ELOOP);
                                return -1;
                            }
                            *pos = 0;
                            if (slash) {
                                *slash = '/';
                                strcat(buf, slash);
                            }
                            strcpy(path, buf);
                            if (*path == '/') result[1] = 0;
                            pos = strchr(result, 0);
                            continue;
                        }
                    }
                    pos = strchr(result, 0);
                }
                if (slash) {
                    *pos++ = '/';
                    path = slash + 1;
                }
                *pos = 0;
                if (!slash) break;
            }
            return 0;
        }

        static char *realpath_internal(const char *__restrict path, char *__restrict resolved_path, int nocheck) {
            char cwd[PATH_MAX];
            char path_copy[PATH_MAX];
            int res;

            if (!path) { // Null (but it can be empty string)
                set_errno(ENOENT);
                return NULL;
            }

            if (strnlen(path, PATH_MAX + 1) > PATH_MAX) {
                set_errno(ENAMETOOLONG);
                return NULL;
            }

            if (!*path) {
                set_errno(ENOENT); /* SUSv2 */
                return NULL;
            }

            int allocated = 0;
            if (resolved_path == NULL) {
                // If  resolved_path is specified as NULL, then realpath() uses malloc(3)
                // to allocate a buffer of up to PATH_MAX bytes to hold the resolved
                // pathname, and returns a pointer to this buffer
                allocated = 1;
                resolved_path = (char *__restrict) malloc(PATH_MAX * sizeof(char));
                if (!resolved_path) return NULL;
            }

            if ('/' == path[0]) {
                // Do not resolve cwd if path start by '/'; e.g., "/", "/..", "/root"
                strcpy(cwd, "/");
            } else {
                if (!getcwd(cwd, sizeof(cwd))) {
                    if (allocated) free(resolved_path);
                    return NULL;
                }
                strcpy(resolved_path, "/");
                if (resolve_path_internal(cwd, resolved_path, resolved_path, nocheck)) {
                    if (allocated) free(resolved_path);
                    return NULL;
                }
                // If the resulting cwd="something/" do no add the separator '/'
                if (resolved_path[strlen(resolved_path) - 1] != '/') {
                    strcat(resolved_path, "/");
                }
            }
            strncpy(path_copy, path, PATH_MAX);
            path_copy[PATH_MAX - 1] = '\0';
            res = resolve_path_internal(path_copy, resolved_path, strchr(resolved_path, 0), nocheck);
            if (res) {
                if (allocated) free(resolved_path);
                return NULL;
            }

            if (!strcmp(resolved_path, "")) {
                strcpy(resolved_path, "/");
            }
            return resolved_path;
        }

        // This function only expands path syntactically without checking
        // partial paths (like 'realpath --canonicalize-missing' in command line)
        static char *realpath_nocheck(const char *path, char *resolved_path) {
            return realpath_internal(path, resolved_path, 1);
        }
    public:
        static void set_errno(int E) {errno  =  E;}

        static string get_canonical_file_name(const string &path){
            char resolved_path[PATH_MAX+1];
            char *rl = realpath_nocheck(path.c_str(), resolved_path);
            if (rl) {
                return resolved_path;
            }
            return "";
        }

        // Create temporary dir calling mkdtemp()
        // If template is an absolute path, use it as template
        // otherwise it is relative to env variable TMPDIR, but if this is not
        // defined, "/tmp" is usea
        // Template is of the form template.XXXXXX (man 3 mkdtemp)
        // Return empty string if it fails
        static string create_temp_dir(const string &dirtemplate = "tmpdir.XXXXXXX")
        {
            char *t = NULL;
            if (!is_absolute(dirtemplate)) {
                char *tmpbasedir = getenv("TMPDIR");
                // Use /tmp by default, if $TMPDIR does not exist
                if (!tmpbasedir || !*tmpbasedir) {
                    tmpbasedir = (char *) "/tmp";
                    struct stat stmp;
                    if (::stat("/tmp", &stmp)) {
                        // Directory /tmp does not exist, try to create it
                        mkdir("/tmp", 0700);
                    }
                }
                t = strdup((string(tmpbasedir) + "/" + dirtemplate).c_str());
            } else {
                t = strdup(dirtemplate.c_str());
            }
            string ret;  // empty string initialization
            if (t) {
                char *tmpdir = NULL;
                tmpdir = mkdtemp(t);
                if (tmpdir) {
                    ret = get_canonical_file_name(tmpdir);
                } else {
                    perror((string("") + "mkdtemp '" + t + "'").c_str());
                }
                free(t);
            }
            return ret;
        }

        // Delete a file only if it is placed in a given directory (supposedly temporary)
        static void delete_temp_file(const string &tmpdir, const string &filename)
        {
            if (IDA_parsing_utils::is_prefix(tmpdir, get_canonical_file_name(filename))) {
                fprintf(stdout, " ... deleting temporary file '%s'\n", filename.c_str());
                ::unlink(filename.c_str());
            }
        }

        // Extract a complex zip path (with .zip/.siard members):
        //     /path/to/zip1.zip/path1/to1/zip2.zip/path2/to2/abc.txt
        // into a temporary directory, returning the new path to the
        // extracted file.
        // If the path does not contain .zip/.siard the same path is returned.
        // Return empty string if not possible to extract
        // If tmpdir is empty a temporary directory is created and
        // tmpdir is set to the name of this temporary directory
        // Otherwise if tmpdir has a value, use it as temporary directory
        // Note that tmpdir may have been created although zip processing failed
        static string unzipURI(const string &zippath, string &tmpdir)
        {
            // Use syntactical realpath because pushd is done later
            string z = get_canonical_file_name(zippath);

            vector<string> zips;
            static string ext = R"(\.(zip|siard))";
            static regex zre("(^.*?" + ext + ")/(.*$)", regex::icase);
            smatch m;

            // First, check if it is a standard path, with no zips inside
            //if (!regex_match(z, zre))
            if (!strcasestr(zippath.c_str(),".zip")
                && !strcasestr(zippath.c_str(), ".siard"))
            {
                return zippath;
            }

            // Let's find the chain of .zip/.siard files
            while (!z.empty()){
                if (regex_search(z, m, zre)) {
                    string subzip  = m[1].str();
                    string subfile = m[3].str();
                    zips.push_back(subzip);
                    z = subfile;
                } else {
                    zips.push_back(z);
                    break;
                }
            }

            if (zips.size() <= 1) {
                // Standard path, with no zips inside
                // cerr << "URI='" << zippath << "'" << endl; // Debug
                return zippath;
            }

            //zips[0] = IDA_file_utils::get_realpath(zips[0]); // Use realpath because pushd is done later

            if (tmpdir.empty()) tmpdir = create_temp_dir();
            if (IDA_file_utils::pushd(tmpdir)) {
                // Error pushd, return the path itself
                perror((string("pushd")+ " '" + tmpdir + "'").c_str());
                return "";
            }

            int err = 0;
            for (unsigned long i=1; i<zips.size(); i++) {
                err = IDA_unzip(zips[i-1].c_str(), (char*)zips[i].c_str());
                // cerr << "From zip: '" << zips[i-1] << "' --extract--> '" << zips[i] << "' [" << ((!err)?"OK":"FAIL") << "]" << endl; // Debug
                if (err){
                    // Perhaps the .zip component is a directory; two possibilities:
                    if (IDA_file_utils::is_directory(zips[i-1])){
                        // It is a directory of the actual filesystem
                        zips[i] = zips[i-1] + "/" + zips[i];
                    } else {
                        // It is a directory inside a zip: let's try concatenating with the next element of the path
                        if (i+1 < zips.size()){
                            zips[i+1] = zips[i] + "/" + zips[i+1];
                            zips[i] = zips[i-1];
                        }
                    }
                } else {
                    zips[i] = tmpdir + "/" + zips[i];
                }
            }

            IDA_file_utils::popd();

            string &zret = zips[zips.size()-1];
            //cerr << "final URI='" << zips[zips.size()-1] << "'" << endl; // Debug
            if (!access(zret.c_str(), R_OK)) {
                return zret;
            } else {
                return "";
            }

        }

    }; /* class IDA_file_utils */
    stack<string> IDA_file_utils::dirstack = {};

    // Some useful methods to use when processing XML
    class IDA_xml_utils {
        // Main methods of library tinyxml2 used by this class:
        //      XMLDocument doc;
        //      XMLError result = doc.LoadFile(xmlfile);      // Load file
        //      XMLElement *pRootElem = doc.RootElement();    // Root node
        //      XMLElement *e;
        //      const char *e_name = e->Name();       // Get node name (tag), e.g., <name>...</name>
        //      const char *e_text = e->GetText();    // Get text inside, e.g. <element> text </element>
        //      XMLAttribute *a = e->FindAttribute("attribute_name");  // Get an attribute
        //      const char *aval = a->Value();
        //      // Traverse XML children:
        //      for (XMLElement *pe = e->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {...}
    private:
        // Traverse an XML tree, childs before siblings
        // executing in each node the function: process_node(XMLElement, path, level va_list)
        // The level of the node is passed both for this method
        // and the process_node function. Level=0 is the root node.
        // Also the path of the node ("/tag1/tag2..."); the root node has path ""
        static void process_tree_va(XMLElement *pElem, void(*process_node)(XMLElement *, string, long, va_list va),
                                    string path, long level, va_list va)
        {
            static const string pathsep = "/";
            // Make a copy of va for each process_node() invocation, because
            // process_node() modifies it (gets arguments) and we need to reuse it in
            // subsequent recursive calls
            va_list va_cpy;
            //if (level == 0) { cout << "[" << __func__  << "]" << endl; } // Debug

            if (pElem){
                va_copy(va_cpy, va);
                process_node(pElem, path, level, va_cpy);

                level++;
                string child_path = path + pathsep + pElem->Name();
                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    process_tree_va(pe, process_node, child_path, level, va);
                }
                level--;
            }
        }

    public:
        // Traverse the XML DOM executing a function "process node" in each node
        // A variable list of parameters can be passed to the function as a va_list
        static void process_tree(XMLElement *pElem, void(*process_node)(XMLElement *, string path, long, va_list va), string path, long level, ...)
        {
            va_list va;
            va_start(va, level);
            va_list va_cpy; // Always pass a copy of va to process_node()
            va_copy(va_cpy, va);
            process_tree_va(pElem, process_node, path, level, va);
            // After the tree has processed, call process_node() with
            // NULL, in case something has to be done after the tree is over
            process_node(NULL, path, -1, va_cpy);
        }

        static void print_tree(XMLElement *pElem)
        {
            static int level=-1;
            //if (level == -1) { cout << "[" << __func__  << "]" << endl; } // Debug
            if (pElem){
                const char *elemText = pElem->GetText();
                if (!elemText) elemText = ""; // This element has no string
                const char *elemName = pElem->Name();
                string indent(2*(level+1), ' '); // Indent with spaces

                cout << setw(3) <<  level << "> " << indent << "tagname='" << elemName << "' text='" << IDA_parsing_utils::trim(elemText) << "'" << endl;

                level++;
                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    print_tree(pe);
                }
                level--;
            }
        }

        // Breadth- or Depth- first traversal
        enum DOM_TRAVERSAL {BF, DF};

        // Recursively find the first child of an element with a given tag
        // Children are traversed level by level
        // Use this version when tags can be repeated inside one another, e.g. <u1> <u1>...</u1> <u1>...</u1> </u1>
        static XMLElement* breadth_first_search_element_by_tag(XMLElement *pElem, const string &tagname)
        {
            if (pElem){
                vector<XMLElement*> v;
                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    v.push_back(pe);
                }

                while (!v.empty()) {
                    XMLElement *pe = v[0];
                    if (!tagname.compare(pe->Name())) {
                        return pe;
                    }
                    for (XMLElement *ce = pe->FirstChildElement(); ce; ce = ce->NextSiblingElement()) {
                        v.push_back(ce);
                    }
                    v.erase(v.begin());
                }
            }
            return NULL;
        }
        // Children are traversed searching depth-first
        // This version is faster, but use it ONLY when tags are NOT repeated inside one another,
        // e.g. <row> <c1> </c1> <c1>
        static XMLElement* depth_first_search_element_by_tag(XMLElement *pElem, const string &tagname)
        {
            if (pElem){
                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    if (!tagname.compare(pe->Name())) {
                        return pe;
                    }
                    XMLElement *ret = depth_first_search_element_by_tag(pe, tagname);
                    if (ret) return ret;
                }
            }
            return NULL;
        }

        // Return the first element found matching the tag name
        static XMLElement* find_element_by_tag(XMLElement *pElem, const string &tagname,
                                               enum DOM_TRAVERSAL df=DF)
        {
            if (BF == df) {
                return breadth_first_search_element_by_tag(pElem, tagname);
            } else {
                return depth_first_search_element_by_tag(pElem, tagname);
            }
        }

        // Return the text of the first element found matching the tag name
        // If not found, empty string is returned
        static string find_elementText_by_tag(XMLElement *pElem, const string &tagname,
                                              enum DOM_TRAVERSAL df=DF)
        {
            XMLElement *e = find_element_by_tag(pElem, tagname, df);
            if (e){
                // GetText() may return null if no text found
                const char *t = e->GetText();
                if (t) return t;
            }
            return "";
        }

        // Return the first immediate child element found matching the tag name
        static XMLElement* find_first_child_element_by_tag(XMLElement *pElem, const string &tagname)
        {
            if (pElem) {
                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    if (!tagname.compare(pe->Name())) {
                        return pe;
                    }
                }
            }
            return NULL;
        }

        // Return the text of the first immediate child element found matching the tag name
        // If not found, empty string is returned
        static string find_first_child_elementText_by_tag(XMLElement *pElem, const string &tagname)
        {
            XMLElement *e = find_first_child_element_by_tag(pElem, tagname);
            if (e){
                // GetText() may return null if no text found
                const char *t = e->GetText();
                if (t) return t;
            }
            return "";
        }

        // Build an array will all elements found matching the tag name
        static void breadth_first_search_elements_by_tag(XMLElement *pElem, const string &tagname,
                                                         vector<XMLElement*> &elements, long maxdepth=9)
        {
            if (pElem){
                queue<XMLElement*> v;
                queue<long> d; // Node's depth (level) vector
                for (XMLElement *pe = pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    v.push(pe);
                    d.push(1); // level 1 for the children of the first node
                }

                while (!v.empty()) {
                    XMLElement *pe = v.front();
                    long nodelevel = d.front();
                    if (!tagname.compare(pe->Name())) {
                        elements.push_back(pe);
                    }
                    if (nodelevel < maxdepth) {
                        for (XMLElement *ce = pe->FirstChildElement(); ce; ce = ce->NextSiblingElement()) {
                            v.push(ce);
                            d.push(nodelevel+1); // Increase level for its children
                        }
                    }
                    v.pop();
                    d.pop();
                }
            }
        }

        static void depth_first_search_elements_by_tag(XMLElement *pElem, const string &tagname,
                                                         vector<XMLElement*> &elements, long maxdepth=9)
        {
            if (maxdepth <= 0) return;
            if (pElem){
                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    if (!tagname.compare(pe->Name())) {
                        elements.push_back(pe);
                    }
                    depth_first_search_elements_by_tag(pe, tagname, elements, maxdepth-1);
                }
            }
        }

        // Build an array will all elements found matching the tag name
        static void find_elements_by_tag(XMLElement *pElem, const string &tagname,
                                         vector<XMLElement*> &elements, long maxdepth = 9,
                                         enum DOM_TRAVERSAL df=DF)
        {
            if (BF == df) {
                breadth_first_search_elements_by_tag(pElem, tagname, elements, maxdepth);
            } else {
                depth_first_search_elements_by_tag(pElem, tagname, elements, maxdepth);
            }
            return;
        }

        // Build an array with all elements found whose tag name matches the regexp
        static void breadth_first_search_elements_by_tag_regex(XMLElement *pElem, const regex &tag_regex,
                                                               vector<XMLElement*> &elements, long maxdepth=9)
        {
            if (pElem){
                queue<XMLElement*> v; // XML node vector
                queue<long> d; // Node's depth (level) vector
                for (XMLElement *pe = pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    v.push(pe);
                    d.push(1); // level 1 for the children of the first node
                }

                while (!v.empty()) {
                    XMLElement *pe = v.front();
                    long nodelevel = d.front();
                    if (regex_match(pe->Name(), tag_regex)) {
                        elements.push_back(pe);
                    }
                    if (nodelevel < maxdepth) {
                        for (XMLElement *ce = pe->FirstChildElement(); ce; ce = ce->NextSiblingElement()) {
                            v.push(ce);
                            d.push(nodelevel+1);
                        }
                    }
                    v.pop();
                    d.pop();
                }
            }
        }

        static void depth_first_search_elements_by_tag_regex(XMLElement *pElem, const regex &tag_regex,
                                                               vector<XMLElement*> &elements, long maxdepth=9)
        {
            if (maxdepth <= 0) return;
            if (pElem){

                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    if (regex_match(pe->Name(), tag_regex)) {
                        elements.push_back(pe);
                    }
                    depth_first_search_elements_by_tag_regex(pe, tag_regex, elements, maxdepth-1);
                }
            }

        }

        static void find_elements_by_tag_regex(XMLElement *pElem, const regex &tag_regex,
                                               vector<XMLElement*> &elements, long maxdepth = 9,
                                               enum DOM_TRAVERSAL df=DF)
        {
            if (BF == df) {
                breadth_first_search_elements_by_tag_regex(pElem, tag_regex, elements, maxdepth);
            } else {
                depth_first_search_elements_by_tag_regex(pElem, tag_regex, elements, maxdepth);
            }
            return;

        }


        // Return the value of an attribute, or not_found_val if not found
        static string get_attribute_value(XMLElement *pElem, const char *val, const char *not_found_val){
            string ret = not_found_val;
            if (val) {
                const XMLAttribute *att_p = pElem->FindAttribute(val);
                if (att_p) {
                    ret = att_p->Value();
                }
            }
            return ret;
        }

        // Return a string with the XML of a tinyxml2 element
        // Very useful to debug elements
        static string get_XML(XMLElement *pElem){
            if (pElem) {
                XMLPrinter pr;
                pElem->Accept(&pr);
                const char *xmlstr = pr.CStr();
                return xmlstr;
            }
            return "";
        }
    }; /* class IDA_xml_utils */

    // This represents the features of a data type (as appearing in column and type attribute xml nodes):
    //   - for simple types only have the name of the SIARD type (e.g. <type>INTEGER</type>)
    //   - for udt we have two components the schema (<typeSchema>...</typeSchema) and the name (<typeName>...</typeName>)
    //   - for distinct we have the base
    //   - for arrays we have the cardinality
    class IDA_SIARD_type_attribute {
        string name; // Only udt attributes has a true name
        string type;
        string typeSchema;
        string typeName;
        unsigned long cardinality = 0;
        string base;
    public:
        IDA_SIARD_type_attribute() {} // Table Columns can be seen as anonymous type attributes
        explicit IDA_SIARD_type_attribute(const string &name) : name(name) {}
        IDA_SIARD_type_attribute(const string &name, const string &type,
                                 const string &typeSchema, const string &typeName,
                                 unsigned long cardinality,
                                 const string &base) : name(name), type(type),
                                                       typeSchema(typeSchema), typeName(typeName),
                                                       cardinality(cardinality),
                                                       base(base) {}
        // Construct from an XML node;
        // this node could be: a type attribute, or even a column
        explicit IDA_SIARD_type_attribute(XMLElement *att){
            name = IDA_xml_utils::find_elementText_by_tag(att, "name");
            type = IDA_xml_utils::find_elementText_by_tag(att, "type");
            typeSchema = IDA_xml_utils::find_elementText_by_tag(att, "typeSchema");
            typeName = IDA_xml_utils::find_elementText_by_tag(att, "typeName");
            cardinality = atol(IDA_xml_utils::find_elementText_by_tag(att, "cardinality").c_str());
            base = IDA_xml_utils::find_elementText_by_tag(att, "base");
        }

        const string &getName() const {
            return name;
        }

        void setName(const string &name) {
            IDA_SIARD_type_attribute::name = name;
        }

        const string &getType() const {
            return type;
        }

        const string &getTypeOrTypeName() const {
            return (type.empty())?typeName:type;
        }

        void setType(const string &type) {
            IDA_SIARD_type_attribute::type = type;
        }

        const string &getBase() const {
            return base;
        }

        void setBase(const string &base) {
            IDA_SIARD_type_attribute::base = base;
        }

        unsigned long getCardinality() const {
            return cardinality;
        }

        void setCardinality(unsigned long cardinality) {
            IDA_SIARD_type_attribute::cardinality = cardinality;
        }

        const string &getTypeSchema() const {
            return typeSchema;
        }

        void setTypeSchema(const string &typeSchema) {
            IDA_SIARD_type_attribute::typeSchema = typeSchema;
        }

        const string &getTypeName() const {
            return typeName;
        }

        void setTypeName(const string &typeName) {
            IDA_SIARD_type_attribute::typeName = typeName;
        }

        // This returns an 'extended' category string for attributes:
        // "distinct", "udt"; but also "array" or "simple"
        string get_extended_category() const {
            if (cardinality > 0){
                // Check array first; an array could be of simple type, of udt ...
                return "array";
            }
            else if (!type.empty()) {
                return "simple";
            }
            else if (!typeSchema.empty() && !typeName.empty()){
                return "udt";
            }
            else if (!base.empty()){
                return "distinct";
            }
            else {
                return "unknown";
            }
        }

        friend std::ostream &operator<<(std::ostream &out, const IDA_SIARD_type_attribute &a);
    };
    std::ostream &operator<<(std::ostream &out, const IDA_SIARD_type_attribute &a) {
        return out << "[" << a.getName()
                << " (" << a.get_extended_category() << ") "
                << "type='" << a.type << "' "
                << "cardinality=" << a.cardinality << " "
                << "base='" << a.base << "' "
                << "typeSchema='" << a.typeSchema << "' "
                << "typeName='" << a.typeName << "' "
                << "]";
    }

    // This class can represent a complex type:
    //   - a user-defined type (udt) (has an attribute list)
    //   - a distinct type (has a <base> type)
    //   - an array (has a either <type> or <typeSchema>+<typeName>, and a <cardinality>),
    //     note that arrays are anonymous but a name is assigned when found
    class IDA_SIARDtypenode {
        string schema;
        // schema where this type is defined
        string name;     // the name of the type (arrays has an assigned name, as they are anonymous)
        string category; // possible categories: distinct, udt, array

        vector<IDA_SIARD_type_attribute> attribute_list;

    public:
        IDA_SIARDtypenode() {
            schema.clear();
            name.clear();
            category.clear();
            attribute_list.clear();
        }

        IDA_SIARDtypenode(const string &s, const string &n, const string &c)
        {
            schema = s;
            name = n;
            category = c;
            attribute_list.clear();
        }

        bool empty(){
            return name.empty();
        }

        const string &getSchema() const {
            return schema;
        }

        void setSchema(const string &schema) {
            IDA_SIARDtypenode::schema = schema;
        }

        const string &getName() const {
            return name;
        }

        void setName(const string &name) {
            IDA_SIARDtypenode::name = name;
        }

        const string &getCategory() const {
            return category;
        }

        void setCategory(const string &category) {
            IDA_SIARDtypenode::category = category;
        }

        // Add a new attribute to the list of attributes of this data type
        void add_attribute(IDA_SIARD_type_attribute A) {
            attribute_list.push_back(A);
        }

        void add_attribute(const string &att_name, const string &type,
                           const string &base, unsigned long cardinality,
                           const string &typeSchema, const string &typeName){
            IDA_SIARD_type_attribute A(att_name);
            A.setType(type);
            A.setBase(base);
            A.setCardinality(cardinality);
            A.setTypeSchema(typeSchema);
            A.setTypeName(typeName);
            add_attribute(A);
        }

        const vector<IDA_SIARD_type_attribute> &getAttributeList() const {
            return attribute_list;
        }

        // Construct a new type node representing an array
        // Remember that array are anonymous complex data types
        // Use suffix to disambiguate different arrays
        // This type node has one only attribute with the 'type' or pair 'typeSchema/typeName'
        // of the array elements, as well as the cardinality
        IDA_SIARDtypenode(const string& schema_name, const string& suffix,
                          const string& type, string typeSchema, string typeName,
                          unsigned long cardinality) : IDA_SIARDtypenode(schema_name, "", "array") {
            string type_name = "ARRAY" + to_string(cardinality) + "_" + suffix;
            this->setName(type_name);
            IDA_SIARD_type_attribute A("ARRAY_ATT", type, typeSchema, typeName, cardinality, "");
            // Create one attribute for the type of the array elements
            // For the arrays, if the type of the array elements is complex (e.g. udt) we have <typeSchema>...</typeSchema>
            // and <typeName>...</typeName> instead of <type>...</type>, such information will be kept as an attribute
            // i.e., if the array typenode has no attribute is a simple type, otherwise it is a complex type and its information
            this->add_attribute(A);
        }

        // Construct a new type node representing a 'distinct' complex type
        // This type node has one only attribute with the 'base' siard type with must be
        // a simple type (INT, TEXT,...)
        IDA_SIARDtypenode(const string& schema_name, const string& type_name, const string& category,
                          const string& base): IDA_SIARDtypenode(schema_name, type_name, "distinct"){
            (void)category; // This unused argument is just used to disambiguate constructors
            IDA_SIARD_type_attribute A("DISTINCT_ATT", "", "", "", 0, base);
            // Create one attribute for the type of the array elements
            // For the arrays, if the type of the array elements is complex (e.g. udt) we have <typeSchema>...</typeSchema>
            // and <typeName>...</typeName> instead of <type>...</type>, such information will be kept as an attribute
            // i.e., if the array typenode has no attribute is a simple type, otherwise it is a complex type and its information
            this->add_attribute(A);
        }


        friend std::ostream &operator<<(std::ostream &out, const IDA_SIARDtypenode &a);
    }; /* class IDA_SIARDtypenode */
    std::ostream &operator<<(std::ostream &out, const IDA_SIARDtypenode &n) {
        out << " <TYPE:" << endl
            << "  schema=" << n.schema << " name=" << n.name << " category=" << n.category << endl;
        for (auto a : n.attribute_list) {
            cout << "  - UDT attribute: '" << a << endl;
        }
        out << " >" << endl;
        return out;
    }

    // The Data Type Table (DTT): this represents a table with all the COMPLEX types found
    // (distinct, udt and arrays) in each schema
    class IDA_SIARDdatatype_table {
        // Complex data type table implemented as a dictionary: ((typeSchema, typeName), typeNode)
        // The table is indexed by the pair (typeSchema, typeName)
        map<pair<string,string>, IDA_SIARDtypenode> datatype_dict;
        // We keep the order of insertion; to be use as index for aux tables (not actually used)
        map<pair<string,string>, unsigned long> datatype_order;
        unsigned long datatype_count = 0; // Count the complex data types inserted in the table
        //
        unsigned long datatype_array_count = 0; // Count the arrays found (in columns or in type attributes)
    public:
        IDA_SIARDdatatype_table()
        {
            datatype_array_count = 0;
            datatype_dict.clear();
        }

        // Gettter for the full table with entries of the form ( (typeSchema, typeName), typeNode)
        const map<pair<string, string>, IDA_SIARDtypenode> &getDatatypeDict() const
        {
            return datatype_dict;
        }

        // Get the typenode of an entry (needed when traversing the attributes of an utd)
        // If not found, a typenode with empty name is returned
        IDA_SIARDtypenode get_typenode(const string &type_schema, const string &type_name) {
            IDA_SIARDtypenode tnode; // tnode.name is empty by default
            try {
                auto key = pair<string,string>(type_schema, type_name);
                tnode = datatype_dict.at(key);
            } catch (...) {}
            return tnode;
        }

        // Generate the name for an auxiliary table associated to a complex data type (typeSchema, typeName)
        // In case this type is not found, empty string is returned
        // (Not actually used)
        string generate_aux_table_name(const string &type_schema, const string &type_name) {
            pair<string, string> key(type_schema, type_name);
            if (datatype_dict.find(key) != datatype_dict.end()) {
                // If this key is found, return the name of the aux table
                return "AUX_TABLE_" + to_string(datatype_order[key]) + "_" + type_schema + "_" + type_name;
            }
            return ""; // key not found in the complex type table
        }

        // Add a type (defined by its typenode) to the table, which is indexed
        // by its type schema and its type name
        void add_type(const string& type_schema, const string& type_name, const IDA_SIARDtypenode &typenode)
        {
            auto key = pair<string, string>(type_schema, type_name);
            datatype_dict[key] = typenode;
            datatype_order[key] = datatype_count++;
        }

        // Add an auxiliary array data type to the complex data type table, for arrays found in columns
        // or type attributes in a given schema
        // The subname string may be the name of the column or attribute
        string add_array_data_type(const string& schema_name, const string& subname, const string& type,
                                   string typeSchema, string typeName, unsigned long cardinality)
        {
            string suffix = subname + "_" + to_string(datatype_array_count++);
            IDA_SIARDtypenode tnode(schema_name, suffix, type, typeSchema, typeName, cardinality);
            string new_type_name = tnode.getName();

            // Add the found array to the Data Type Table
            this->add_type(schema_name, new_type_name, tnode);

            // Return the name of the name of the new array
            return new_type_name;

        }
        // Overloaded version using of previous method using an xml element;
        // Element e can be a column or a type attribute
        // TODO: what happens with array of arrays?
        string add_array_data_type(const string& schema_name, XMLElement *e) {
            string e_name =  IDA_xml_utils::find_elementText_by_tag(e, "name");
            string e_cardinality = IDA_xml_utils::find_elementText_by_tag(e, "cardinality");
            string e_type =  IDA_xml_utils::find_elementText_by_tag(e, "type"); // Used when declaring an array of a simple type
            string e_type_schema = IDA_xml_utils::find_elementText_by_tag(e, "typeSchema"); // Pair (typeSchema, typeName) used by array of complex types
            string e_type_name = IDA_xml_utils::find_elementText_by_tag(e, "typeName");
            string new_array_name = this->add_array_data_type(schema_name, e_name, e_type,
                                                              e_type_schema, e_type_name,
                                                              stoul(e_cardinality));
            return new_array_name;
        }
    }; /* class IDA_SIARDdatatype_table */

    // Global complex Data Type Table, this table is indexed by the pair (data_type_schema, data_type_name)
    IDA_SIARDdatatype_table DataType_Table;

    // lobFolder hierarchy structure for external files of a column
    class IDA_SIARDlobfolder {
        string column_name;

        // A map of the hierarchy of lob folders:
        // lobfolder_info[""] -> lob folder for siarArchive
        // lobfolder_info["siard_colname_v"] -> lob folder for the column 'siard_colname_v'
        // lobfolder_info["siard_colname_v/uatt"] -> lob folder of an attribute named "uatt" for a column with a user-defined type having this attribute

        class lobfolder_info {
        public:
            string lobfolder;      // Lob folder for a given element treepath
            string acc_lobfolder;  // Cumulative lob folder considering enclosing elements
            string real_lobfolder; // Real path of the cumulative lob folder relative to the base dir (siard_dir or siard_file)
        };

        unordered_map<string, lobfolder_info> lobfolder_info;

    public:
        explicit IDA_SIARDlobfolder() {}

        void init(const string &siardURI, const string &column_name,
                  XMLElement *column, const string &siard_lobfolder= "")
        {
            // Init the lobfolder_info object for a given column
            this->column_name = column_name;
            // The root of the path tree is "" for the "<siardArchive><lobFolder>...</lobFolder>"
            if (!siard_lobfolder.empty()) {
                string siard_real_lobfoler = generate_real_lobfolder(siardURI, siard_lobfolder);
                lobfolder_info[""] = {siard_lobfolder, siard_lobfolder, siard_real_lobfoler};
            }
            // "treepath" start in the column
            init_element_lobfolders(siardURI, column, "", siard_lobfolder);
        }

        // Build the lob folder information structure
        // Initially 'el' is a column xml element
        // In recursive calls, 'el' is a field xml element
        void init_element_lobfolders(const string &siardURI, XMLElement *el,
                                     const string &treepath= "", const string &curr_lobfolder= "")
        {
            if (!el) return;

            string name = IDA_xml_utils::find_elementText_by_tag(el, "name");

            // For arrays use a1, a2, ... because they appear using the name of the column or attribute as:
            // <column> or <attribute
            // <name>CARRAY</name>
            //  <fields>
            //  <field>
            //      <name>CARRAY[1]</name>
            //  </field>
            //  <field>
            //      <name>CARRAY[2]</name>
            //  </field>
            unsigned long idx; // Array index
            static regex arr_re("\\[([0-9]+)\\]$");
            smatch sm;
            if (regex_search(name, sm, arr_re) && !sm.empty()){
                idx = stoul(sm[1].str());
                name = "a" + to_string(idx); // "a1", "a2", ... note it's indexed starting at 1: CARRAY[1]
            }

            if (!name.empty()){
                // Update tree  treepath and lobfolder_info
                string new_treepath = treepath + "/" + name;

                string lobfolder;
                lobfolder = IDA_xml_utils::find_first_child_elementText_by_tag(el, "lobFolder");

                // Cumulative lob folder for this element
                string curr_acc_lobfolder = combine_lobfolders(curr_lobfolder, lobfolder);
                string real_cur_acc_lobfolder = generate_real_lobfolder(siardURI, curr_acc_lobfolder);
                // If lob folder found insert it into the dictionary
                if (!curr_acc_lobfolder.empty()) {
                    lobfolder_info[new_treepath] = {lobfolder, curr_acc_lobfolder, real_cur_acc_lobfolder};
                }

                XMLElement *fields_el = IDA_xml_utils::find_element_by_tag(el, "fields");
                vector<XMLElement*> fields;
                IDA_xml_utils::find_elements_by_tag(fields_el, "field", fields, 1);

                for (auto field : fields) {
                    init_element_lobfolders(siardURI, field, new_treepath, curr_acc_lobfolder);
                }
            }
        }

        // Combine the lobFolder of one element (lobfolder) with the lobFolder associated
        // to its enclosing element (parent_lobfoleer)
        static string combine_lobfolders(const string &parent_lobfolder, const string &lobfolder)
        {
            //TODO: how to combine a lob folder with its parent's lob folder???? they are relative???
            //TODO: what if lobfolders are exprssed as URIs
            // For now, a simple combination is applied (only checking absolute/relative folder paths)
            string combined_folder;
            if (parent_lobfolder.empty() || IDA_file_utils::is_absolute(lobfolder)){
                // Either the enclosing element has no lob folder
                // Or the lobfolder of this element is absolute
                combined_folder = lobfolder;
            }
            else {
                if (lobfolder.empty()){
                    // Enclosing element has lob folder but this element has not
                    combined_folder = parent_lobfolder;
                } else {
                    // Both this element and its enclosing element has lob folder:
                    // combine them
                    combined_folder = parent_lobfolder + "/" + lobfolder;
                }
            }
            return combined_folder;
        }

        // Generate the final lobfolder, relative to the siardURI (it can be a directory or the siard zip file
        // depending on unzipmode_e)
        // The lobfolder argument is already the combination of the lob foder of one element and their enclosing ones
        static string generate_real_lobfolder(const string &siardURI, const string &lobfolder)
        {
            string finalfolder;
            if (IDA_file_utils::is_absolute(lobfolder)) {
                finalfolder = lobfolder;
            } else {
                finalfolder = combine_lobfolders(siardURI, lobfolder);
            }
            finalfolder = IDA_file_utils::get_canonical_file_name(finalfolder);
            return finalfolder;
        }

        // Get the final lobfolder of an element (column/attribute) from its treepath as key
        string get_real_lobfoler(const string &treepathkey)
        {
            try {
                // Check the keey
                return lobfolder_info.at(treepathkey).real_lobfolder;
            } catch (...) {}
            return "";
        }

        friend std::ostream &operator<<(std::ostream &out, const IDA_SIARDlobfolder &a);
    };  /* class IDA_SIARDlobfolder */
    std::ostream &operator<<(std::ostream &out, const IDA_SIARDlobfolder &n) {
        if (n.lobfolder_info.empty()){
            out << "No lob folders for element '" << n.column_name << "'" << endl;
        } else {
            out << "Lob folders for column '" << n.column_name << "':" << endl;
            for (auto e: n.lobfolder_info) {
                out << "  ('" << e.first << "', '" << e.second.lobfolder << "' -> '"
                                                   << e.second.acc_lobfolder << "' -> '"
                                                   << e.second.real_lobfolder << "')" << endl;
            }
        }
        return out;
    }

    // Two possibilities: unzip the zip fully, or unzipping file by file
    enum unzipmode_e {SIARD_FULL_UNZIP, SIARD_FILE_BY_FILE_UNZIP};

    // Main class to process  "content/schema<M>/table<N>/table<N>.xml" archive
    class IDA_SIARDcontent{
        XMLDocument doc;
        XMLElement *pRootElem = NULL;

        string tablename;

        string siardURI;
        string tmpdir;
        enum unzipmode_e unzipmode;

        ostream &sqlout = cout;
        unsigned long ncols = 0;
        vector<string> siard_colname_v;
        vector<IDA_SIARD_type_attribute> siard_coltype_v;
        vector<IDA_SIARDlobfolder> siard_lobfolder_info_v;

    public:
        unsigned long current_col_id = 0;
        IDA_SIARDcontent(const string& tablename,
                         const string& siardURI, const string& tmpdir, enum unzipmode_e unzipmode,
                         ostream &sqlout, unsigned long ncols,
                         vector<string> siard_colname_v,
                         vector<IDA_SIARD_type_attribute> siard_coltype_v,
                         vector<IDA_SIARDlobfolder> siard_lobfolder_info_v)
                         :
                tablename(tablename), siardURI(siardURI), tmpdir(tmpdir),
                sqlout(sqlout), ncols(ncols),
                siard_colname_v(siard_colname_v),
                siard_coltype_v(siard_coltype_v), siard_lobfolder_info_v(siard_lobfolder_info_v)
        {
            this->unzipmode = unzipmode;
            clear();
        }

        void clear()
        {
            doc.Clear();
            pRootElem = NULL;
        }

        int load(const char *xmlfile)
        {
            clear();
            XMLError result = XML_ERROR_FILE_READ_ERROR;

            try {
                result = doc.LoadFile(xmlfile);
            } catch (const std::exception &e) {
                // catch anything thrown within try block that derives from std::exception
                cerr << "*EXCEPTION loading XML '" << xmlfile << "'" << endl << " what: '" << e.what() << "'" << endl;
                result = XML_ERROR_FILE_READ_ERROR;
                //- exit(126);
            } catch (...){
                cerr << "*Unknown EXCEPTION loading XML '" << xmlfile << "' result=" << result << "" << endl;
                result = XML_ERROR_FILE_READ_ERROR;
                //- exit(127);
            }

            if (result != XML_SUCCESS){
                //fprintf(stderr, "Error loading XML file '%s': %d\n", xmlfile, result); // Debug
                return -1;
            } else {
                //fprintf(stderr, "OK loading '%s'\n", xmlfile); // Debug
                pRootElem = doc.RootElement();
                // Let us store also the filename
                // -- xmlfilename = xmlfile;
                return 0;
            }
        }

        int load(string xmlfile)
        {
            return load(xmlfile.c_str());
        }

        // Method to print one xml element, to be used
        // in IDA_xml_utils::process_tree
        static void print_element(XMLElement *pElem, string path, long level, va_list va)
        {
            (void) path;
            (void) va;
            if (pElem) {
                const char *elemName = pElem->Name();
                const char *elemText = pElem->GetText();
                if (!elemText) elemText = ""; // This element has no string
                string indent(level, ' '); // Indent with spaces
                cout << setw(4) << level << "> " << indent << "tagname='" << elemName << "' text='"
                     << IDA_parsing_utils::trim(elemText) << "'" << " path=" << path << endl;
            }
        }

        // Print raw
        void print_tree(){
            if (pRootElem) {
                IDA_xml_utils::print_tree(pRootElem);
            }
        }

        // Print using process_tree() method with print_element() function
        void print_full_tree(){
            if (pRootElem) {
                IDA_xml_utils::process_tree(pRootElem, print_element, "", 0);
            }
        }

    private:
        // Get the content of an element, typically a column and append it to string s
        // If textifyblob=true, force sqlite blobs (X'00FF...') to be cast to TEXT; this feature is used when
        // generating json of complex data types
        //
        // Version using the SIARD type string ("CHAR(12)", "INTEGER", "TEXT", ...)
        void append_simple_data_type_content(string &s, XMLElement *el, const string &siard_type,
                                             bool textifyblob= false, const string &treepath = ""){
            enum IDA_siard_utils::SQLITE_COLTYPES simpletype = IDA_siard_utils::siard_type_to_sqlite3(siard_type);
            append_simple_data_type_content(s, el, simpletype, textifyblob, treepath);
            return;
        }
        // Version using the sqlite type enum (IDA_siard_utils::COLTYPE_INTEGER, ...)
        // It's more efficient for simple types because the enum can be precomputed
        void append_simple_data_type_content(string &s, XMLElement *el, enum IDA_siard_utils::SQLITE_COLTYPES simpletype,
                                             bool textifyblob= false, const string &treepath = "")
        {
            if (!el) {
                // Return empty content for void elements
                s.append("''");
                return;
            }

            //cerr << ANSI_COLOR_MAGENTA << "coltype=" << IDA_siard_utils::coltype_to_str(simpletype)
            //     << " treepath='" << treepath << "'" << ANSI_COLOR_RESET << endl; // Debug

            //-- string content;
            string el_file;
            el_file= IDA_xml_utils::get_attribute_value(el, "file", "");
            string el_filelen;
            el_filelen= IDA_xml_utils::get_attribute_value(el, "length", "");

            // If there is a file, the value to insert is the
            // hexadecimal sqlite blob form X'12abcdef' of
            // the file content  (text, blob, clob, vartext, ...)
            if (!el_file.empty()){
                string lob_file;
                string lob_literal;
                // Get the full canonical lobfoler asssociated to this treepath, if any
                string lobfolder = siard_lobfolder_info_v[current_col_id].get_real_lobfoler(treepath);

                if (lobfolder.empty()) {
                    //lob_file = siard_dir + "/" + el_file;
                    lob_file = IDA_SIARDlobfolder::combine_lobfolders(siardURI, el_file);
                } else {
                    //lob_file = lobfolder + "/" + el_file;
                    lob_file = IDA_SIARDlobfolder::combine_lobfolders(lobfolder, el_file);
                }

                if (simpletype == IDA_siard_utils::COLTYPE_TEXT || textifyblob) {
                    // If the affinity of this column is TEXT, cast the hex blob
                    // form to text type
                    //-- content = "CAST(";
                    s.append("CAST(");
                }

                // If we assume that the lob_file is not a ".zip"-addressed URI, we can optimize this reading
                // the file directly, without calling unzipURI(); nevertheless whe are going to be conservative
                // and not to assume that
                static bool optimize_lob_reading = false;
                if (optimize_lob_reading && SIARD_FULL_UNZIP == unzipmode) {
                    // The SIARD file has been already unzipped
                    //lob_literal = IDA_siard_utils::file_to_blob_literal_append(lob_file);
                    IDA_siard_utils::file_to_blob_literal_append(lob_file, s);
                #ifndef IDA_FULL_UNZIP
                    IDA_file_utils::delete_temp_file(tmpdir, lob_file);
                #endif
                } else {
                    string tmp_lob_file = IDA_file_utils::unzipURI(lob_file, tmpdir);
                    //lob_literal = IDA_siard_utils::file_to_blob_literal_append(tmp_lob_file);
                    IDA_siard_utils::file_to_blob_literal_append(tmp_lob_file, s);
                #ifndef IDA_FULL_UNZIP
                    IDA_file_utils::delete_temp_file(tmpdir, tmp_lob_file);
                #endif
                }


                if (simpletype == IDA_siard_utils::COLTYPE_TEXT || textifyblob) {
                    // If the affinity of this column is TEXT, cast the hex blob
                    // form to text type
                    //-- content.append(" AS TEXT)");
                    s.append(" AS TEXT)");
                }
            } else {
                const char *t = el?el->GetText():NULL;
                string col_text = t?t:"";
                if (el && el->GetText()) col_text = (string)(el->GetText());
                if (simpletype == IDA_siard_utils::COLTYPE_INTEGER
                    || simpletype == IDA_siard_utils::COLTYPE_REAL
                    || simpletype == IDA_siard_utils::COLTYPE_NUMERIC) {
                    // Integer, float
                    //-- content = col_text;
                    s.append(col_text);
                } else {
                    // Text -> if the text has no special siard chars (\u00...) simply quote it;
                    // otherwise, decode the siard-encoding string and express down as hex
                    if (!IDA_siard_utils::has_siard_special_chars(col_text)){
                        //content = IDA_siard_utils::enclose_sqlite_single_quote(col_text);
                        s.append(IDA_siard_utils::enclose_sqlite_single_quote(col_text));
                    } else {
                        uint8_t *col_text_decoded = NULL;
                        long size = 0;
                        bool has_specials = false;
                        col_text_decoded = IDA_siard_utils::siard_decode(col_text, size, has_specials);
                        assert(has_specials); // Here has_specials needs to be true
                        if (col_text_decoded) {
                            // Write as a blob cast to text, as there can be char(0) once decoded
                            //-- string lob_literal = IDA_siard_utils::char_array_to_blob_literal(col_text_decoded, size);
                            //-- //-- content = "CAST(" + lob_literal + " AS TEXT)";
                            //-- s.append("CAST(" + lob_literal + " AS TEXT)");
                            s.append("CAST(");
                            IDA_siard_utils::char_array_to_blob_literal_append(col_text_decoded, size, s);
                            s.append(" AS TEXT)");
                            free(col_text_decoded);
                        }
                        else {
                            cerr << "Error: malloc failed\n";
                            throw bad_alloc();
                        }
                    }
                }
            }
            //return content;
            return;
        }

        // Get the content of an element, typically a column, containing a complex data type
        // and append it to string s
        void append_complex_data_type_content(string &s, XMLElement *el,
                                              const string &siard_typeSchema, const string &siard_typeName,
                                              long depth= 0, const string &treepath = "")
        {
            //-- string content;
            string indent = string(1+depth, ' ');
            if (0 || el) {
                // Found the complex data type in the Data Type Table
                IDA_SIARDtypenode tnode = DataType_Table.get_typenode(siard_typeSchema, siard_typeName);

                // If the type is not found in the Data Type Table, it must be a simple type
                if (tnode.empty()){
                    // It SHOULD be a simple basic type
                    // Get the content as it is a simple basic type
                    // Force textify blobs to be json compliant
                    //-- string cell_content = append_simple_data_type_content(el, siard_typeName, true, treepath);
                    //-- //-- cell_content = cell_content.substr(0,63) + ((cell_content.size()>64)?"...":""); // debug get only the first part
                    //-- content = cell_content;
                    append_simple_data_type_content(s, el, siard_typeName, true, treepath);
                }
                if (!tnode.empty()) {
                    if (tnode.getCategory() == "array"){
                        // Arrays must have one unique attribute with is type (simple or complex) and the cardinality
                        string arr_cat; // Extended category of the type of the array elements
                        string arr_schema, arr_type; // Type of the element; for complex types arr_schema=typeSchmea, arr_type=typeName;
                                                     // for simple types only 'arr_type=type' makes sense
                        unsigned long arr_card;
                        for (const auto& att: tnode.getAttributeList()) {
                            arr_cat = att.get_extended_category();
                            arr_card = att.getCardinality();
                            arr_schema = att.getTypeSchema();
                            arr_type= att.getType().empty()?att.getTypeName():att.getType();
                            break;
                        }

                        //-- string json_str="json_array(\n";
                        s.append("json_array(\n");
                        for (unsigned long i=1; i <= arr_card ; i++){ // Note index starts at 1: <a1></a1>, <a2></a2>...
                            string atag = "a" + to_string(i);
                            XMLElement *a = IDA_xml_utils::find_element_by_tag(el, atag, IDA_xml_utils::BF);
                            if (0 || a) {
                                //-- string cell_content = append_complex_data_type_content(a, arr_schema,
                                //--                                                     arr_type, depth + 1,
                                //--                                                     treepath + "/" + atag);
                                //-- json_str.append(indent + cell_content);
                                //-- if (i < arr_card) json_str.append(",\n");

                                s.append(indent);
                                append_complex_data_type_content(s, a, arr_schema, arr_type, depth + 1,
                                                                 treepath + "/" + atag);
                                if (i < arr_card) s.append(",\n");
                            } else {
                                // <aN> tag not found for N: use empty content for this element
                                s.append(indent + "''");
                                if (i < arr_card) s.append(",\n");
                            }
                        }
                        s.append(")");
                        if (depth>0) s.append("\n");
                        //-- content = json_str;
                    }
                    else if (tnode.getCategory() == "distinct"){
                        // 'Distinct' types must have one unique attribute with is base type
                        string dis_base;
                        for (const auto& att: tnode.getAttributeList()) {
                            dis_base = att.getBase();
                            break;
                        }
                        // We assume that the base of a 'distinct' data type is always a simple type
                        string dis_schema = "";
                        //-- content = append_complex_data_type_content(el, dis_schema, dis_base, depth + 1, treepath);
                        append_complex_data_type_content(s, el, dis_schema, dis_base, depth + 1, treepath);
                    }
                    else if (tnode.getCategory() == "udt"){
                        //-- string json_str="json_object(\n";
                        s.append("json_object(\n");
                        // Get recursively the content of each UDT attribute
                        unsigned long att_no = 1;
                        for (const auto& att: tnode.getAttributeList()) {
                            string utag = "u" + to_string(att_no++);
                            XMLElement *u = IDA_xml_utils::find_element_by_tag(el, utag, IDA_xml_utils::BF);
                            const string& att_name = att.getName();
                            s.append(indent + "'" + att_name + "', ");
                            if (0 || u) {
                                string u_schema, u_type; // Type of the element; for complex types arr_schema=typeSchmea, arr_type=typeName;
                                                         // for simple types only 'arr_type=type' makes sense
                                u_schema = att.getTypeSchema();
                                u_type= att.getType().empty()?att.getTypeName():att.getType();
                                //-- string att_content = append_complex_data_type_content(u, u_schema,
                                //--                                                    u_type, depth + 1,
                                //--                                                    treepath + "/" + att_name);
                                //-- json_str.append(att_content);
                                append_complex_data_type_content(s, u, u_schema, u_type, depth + 1,
                                                                 treepath + "/" + att_name);
                            } else {
                                // <uN> tag not found for N: use empty content for this element
                                //json_str.append(indent + "'" + att_name + "', ");
                                s.append("''");
                            }
                            if (att_no <= tnode.getAttributeList().size()) s.append(",\n"); // Not add separator at the end
                        }
                        s.append(")");
                        if (depth>0) s.append("\n");
                        //-- content = json_str;
                    }
                }
            }
            else {
                // Elements missing in table.xml represented as empty
                //-- content = "''";
                s.append("''");
            }
            //-- return content;
            return;
        }

    public:

        // Out a string with the SQL statements to insert all data in columns
        // Extra info can be inserted as sql comments controled by verbose:
        //   verbose=0 (no info), 1 (table info), 2 (extra table info), 3 (per column info)
        void tree_to_sql(int verbose = 0)
        {
            if (pRootElem) {
                XMLElement *table = pRootElem;
                // TODO: check the tag of table is <table>

                string version;
                version = IDA_xml_utils::get_attribute_value(table, "version", "unknown");
                (verbose > 0) && sqlout << "-- table name=" << tablename << " version=" << version << endl;

                vector<XMLElement*> rows;
                IDA_xml_utils::find_elements_by_tag(pRootElem, "row", rows, 1);

                (verbose > 1)  && sqlout << "-- no. of rows=" << rows.size() << endl;

                // Expression for column tags: <c1>...</c1> <c2>...</c2>
                // const regex col_tag_re("c[0-9].*");

                // Precompute column invariants
                vector<string> col_cplx_typeSchema(ncols), col_cplx_type(ncols);
                vector<enum IDA_siard_utils::SQLITE_COLTYPES> col_simple_type(ncols);
                for (unsigned long colid = 0; colid < ncols; colid++){
                    col_cplx_typeSchema[colid] = siard_coltype_v[colid].getTypeSchema();
                    col_cplx_type[colid] = siard_coltype_v[colid].getTypeOrTypeName();
                    col_simple_type[colid] = IDA_siard_utils::siard_type_to_sqlite3(col_cplx_type[colid]);
                }

                //-- string colcontent;
                string SQL_insert_into_start = "INSERT INTO '" + tablename + "' VALUES (";

                for (unsigned long ir = 0; ir < rows.size(); ir++) {
                    XMLElement *row = rows[ir];
                    if (verbose > 1) {
                        string row_name = "r" + to_string(ir);
                        sqlout << "--  bogus rowname='" << row_name << "'" << endl;
                        sqlout << "--  no. of columns in table='" << ncols << "'" << endl;
                    }

                    // Traverse columns of the row and write its corresponding INSERT statement
                    string SQL_insert_into = SQL_insert_into_start;

                    // Iterate over the columns of this row
                    for (unsigned long colid = 0; colid < ncols; colid++){
                        XMLElement *col;

                        // Tags of the columns are <c1></c1> <c2></c2>...
                        // Column number is the integer after the 'c': c1, c2, ...
                        // Notice the first column is numbered with 1: c1 !!
                        string colname = "c" + to_string(colid + 1);
                        col = IDA_xml_utils::find_element_by_tag(row, colname);

                        // This is a little dirty trick, using a member for the col id as global
                        current_col_id = colid;

                        (verbose > 2) && sqlout << "--  bogus columnname='" << colname << "'" << endl;

                        // Let's generate the column content depending on it is simple or complex data type
                        string &col_siard_typeSchema = col_cplx_typeSchema[colid];
                        // The initial treepath is something like "/columnname"
                        string treepath0 = "/" + siard_colname_v[colid];
                        // Simple types has no typeSchema, so generate complex content (json) only for complex data types
                        if (col_siard_typeSchema.empty()) {
                            // Simple: INTEGER, REAL, NUMERIC, BLOB, TEXT
                            //-- colcontent = append_simple_data_type_content(col, col_simple_type[colid], false, treepath0); // It's fast using sqlite types
                            append_simple_data_type_content(SQL_insert_into, col, col_simple_type[colid], false,
                                                            treepath0); // It's fast using sqlite types
                        } else {
                            // Complex: distinct, udt, array
                            string &col_siard_type = col_cplx_type[colid];
                            //-- colcontent = append_complex_data_type_content(col, col_siard_typeSchema, col_siard_type, 0, treepath0);
                            append_complex_data_type_content(SQL_insert_into, col, col_siard_typeSchema, col_siard_type,
                                                             0, treepath0);
                        }


                        //-- SQL_insert_into += colcontent;
                        if (colid < ncols - 1) SQL_insert_into += ",\n";

                        #if 0
                        {
                            // Debug complex types
                            string col_siard_typeSchema = siard_coltype_v[colid].getTypeSchema();
                            string col_siard_type = siard_coltype_v[colid].getTypeOrTypeName();
                            string col_cplx_content = append_complex_data_type_content(siard_dir, col,
                                                                                    col_siard_typeSchema,
                                                                                    col_siard_type);
                            // For debugging purpose remove large hex sequences
                            static regex re_largehex("(X'[0-9a-fA-F]{1,31})", std::regex::optimize);
                            col_cplx_content = regex_replace(col_cplx_content, re_largehex, "$1...");
                            static regex re_largehex2("[0-9a-fA-F]{32,1000}", std::regex::optimize);
                            col_cplx_content = regex_replace(col_cplx_content, re_largehex2, "");

                            cerr << "\033[1;33m---complex data start column=" << tablename << ":" << siard_colname_v
                                 << " (" << col_siard_typeSchema << "," << col_siard_type << ")" << endl
                                 << "\033[1;31m"
                                 << col_cplx_content
                                 << endl << "\033[1;32m---complex data end\033[m" << endl;
                        }
                        #endif
                    }

                    SQL_insert_into += ");\n";
                    sqlout << SQL_insert_into;
                }
            } /* if (pRootElem) */
        }

    }; /* class IDA_SIARDcontent */

    // Main class to process the "header/metadata.xml" archive
    class IDA_SIARDmetadata {

        XMLDocument doc;
        XMLElement *pRootElem = NULL;
        //string siard_dir = ""; // Directory where "header/metadata.xml" is placed (after unzipping .siard)
        //string siard_file = ""; // Directory where .siard (zip) file is (must be set explicitly)

        string siardURI;
        string tmpdir;

        enum unzipmode_e unzipmode = SIARD_FULL_UNZIP;

        const string tmpdir_template = "_s2s_tmp";
        const string tmpdir_templateX = tmpdir_template + "XXXXXX";

    public:
        //-- IDA_SIARDmetadata()
        //-- {
        //--     clear();
        //-- }

        IDA_SIARDmetadata(const string &siard_uri)
        {
            clear();
            siardURI = IDA_file_utils::get_realpath(siard_uri);
            tmpdir = IDA_file_utils::create_temp_dir(tmpdir_templateX);
            if (tmpdir.empty()) {
                cerr << "Error creating temporary directory with template '" << tmpdir_templateX << "'" << endl;
                return;
            }
            if (IDA_file_utils::is_directory(siardURI)){
                // It should be a siard file already unzipped in this directory
                unzipmode = SIARD_FULL_UNZIP;
            } else {
                // It shoud be a siard file
                unzipmode = SIARD_FILE_BY_FILE_UNZIP;
            }
        }

        IDA_SIARDmetadata(const char *siardURI) : IDA_SIARDmetadata(string(siardURI)) {}

        ~IDA_SIARDmetadata()
        {
            // Delete recursively the temporary directory on destructing this object
            if (!tmpdir.empty()) {
                int rmerr = IDA_file_utils::rrm(tmpdir, tmpdir_template);
                if (!rmerr) cerr << "Temporary directory '" << tmpdir << "' deleted" << endl; // Debug
            }
            // Close all zips opened temporarily
            IDA_unzip_close_all();
        }

        void clear()
        {
            doc.Clear();
            pRootElem = NULL;
            siardURI.clear();
            tmpdir.clear();
        }

        // Parse the metadata.xml file, which must be "header/metadata.xml" relative to siardURI
        int load()
        {
            pRootElem = NULL; // If load fails, pRootElem is NULL

            // The metadata.xml file
            string metadatafile = siardURI + "/header/metadata.xml";

            if (SIARD_FILE_BY_FILE_UNZIP == unzipmode) {
                metadatafile = IDA_file_utils::unzipURI(metadatafile, tmpdir);
            }

            XMLError result = XML_ERROR_FILE_READ_ERROR;
            result = doc.LoadFile(metadatafile.c_str());
            if (result == XML_SUCCESS){
                cerr << "OK loading metadata xml file '" << metadatafile << "'" << endl; // Debug
                pRootElem = doc.RootElement();
                return 0;
            }
            cerr << "ERROR loading metadata xml file '" << metadatafile << "': " << result << endl; // Debug
            return -1;
        }

        // Unzip the siardURI if it is a file, and we pass to SIARD_FULL_UNZIP mode using the temporary directory
        // as base URI
        // If 'onlyheader' is true, only 'header/metadata.xml' is unzipped
        int unzip(bool onlyheader = false)
        {
            if (IDA_file_utils::is_directory(siardURI)){
                // Do nothing if siardURI is a directory
                return 0;
            }

            if (IDA_file_utils::pushd(tmpdir)) {
                perror("pushd");
                return -1;
            }

            int ziperr;
            cerr << "Unzip SIARD file '" << siardURI << "' in folder '" << tmpdir << "'" << endl;

            if (onlyheader) {
                ziperr = IDA_unzip_siard_metadata(siardURI.c_str()); // Remember we are using its realpath
            } else {
                ziperr = IDA_unzip_siard_full(siardURI.c_str()); // Remember we are using its realpath
            }

            if (ziperr) {
                cerr << "Error (" << ziperr << ") unzipping '" << siardURI << "'" << endl;
                (void) IDA_file_utils::popd(); // popd before quitting
                return -1;
            }
            puts("");

            // Print siard version found
            char buff[PATH_MAX];

            if (!onlyheader) {
                char *ver = IDA_get_siard_version_from_dir(".", buff, PATH_MAX);
                if (ver) {
                    cout << "SIARD version: " << ver << endl;
                    puts("");
                }
                cerr << "Done unzipping SIARD file '" << siardURI << "' in folder '" << tmpdir << "'" << endl;
            }

            //  Popd
            if (IDA_file_utils::popd()) {
                perror("popd");
                return -1;
            }

            // Now we are in  SIARD_FULL_UNZIP mode and the siard URI is the temporary directory
            // where the siard was unzipped
            unzipmode = SIARD_FULL_UNZIP;
            siardURI = tmpdir;
            return 0;
        }

    private:
        // Method to print one xml element, to be used
        // in IDA_xml_utils::process_tree
        static void print_element(XMLElement *pElem, string path, long level, va_list va)
        {
            (void) path;
            (void) va;
            if (pElem) {
                const char *elemName = pElem->Name();
                const char *elemText = pElem->GetText();
                if (!elemText) elemText = ""; // This element has no string
                string indent(level, ' '); // Indent with spaces
                cout << setw(4) << level << "> " << indent << "tagname='" << elemName << "' text='"
                     << IDA_parsing_utils::trim(elemText) << "'" << " path=" << path << endl;
            }
        }

        // Add the complex data types include in a schema (<types> <type>...</type> <type>...</type> ...</types>)
        // to the complex Data Type Table
        void add_complex_data_type(XMLElement *schema, string schema_name){
            XMLElement *schema_types= IDA_xml_utils::find_element_by_tag(schema, "types");
            if (schema_types){
                vector<XMLElement*> types;
                IDA_xml_utils::find_elements_by_tag(schema_types, "type", types, 1);
                for (unsigned long it = 0; it < types.size(); it++) {
                    XMLElement *ty = types[it];
                    if (ty) {
                        string type_category = IDA_xml_utils::find_elementText_by_tag(ty, "category");
                        string type_name = IDA_xml_utils::find_elementText_by_tag(ty, "name");
                        if (!type_category.empty() && !type_name.empty()){
                            IDA_SIARDtypenode typenode;
                            if (type_category == "distinct") {
                                // It is like an alias of the base type
                                string base = IDA_xml_utils::find_elementText_by_tag(ty, "base");
                                typenode = IDA_SIARDtypenode(schema_name, type_name, type_category, base);
                                // Add the type to the Data Type Table
                                DataType_Table.add_type(schema_name, type_name, typenode);
                            }
                            else if (type_category == "udt") {
                                typenode = IDA_SIARDtypenode(schema_name, type_name, type_category);
                                // User-define-type: need to get the attributes of the type
                                XMLElement *type_attributes= IDA_xml_utils::find_element_by_tag(ty, "attributes");
                                if (type_attributes) {
                                    vector<XMLElement *> attributes;
                                    IDA_xml_utils::find_elements_by_tag(type_attributes, "attribute", attributes, 1);
                                    // Traverse type attributes
                                    for (unsigned long ia = 0; ia < attributes.size(); ia++) {
                                        XMLElement *att = attributes[ia];
                                        IDA_SIARD_type_attribute A(att);
                                        // This is our extended category for attributes: "distinct", "udt" but also "array" or "simple"
                                        string type_category = A.get_extended_category();
                                        // Now, add the attribute to the attribute list of the declared type
                                        if (type_category == "udt") {
                                            typenode.add_attribute(A);
                                        }
                                        else if (type_category == "array") {
                                            // Create a new entry for the found array as a new complex type of the CURRENT schema
                                            // (like an udt pointing the array)
                                            string new_array_name = DataType_Table.add_array_data_type(schema_name, att);
                                            A.setType("");
                                            A.setCardinality(0);
                                            A.setTypeSchema(schema_name);
                                            A.setTypeName(new_array_name);
                                            // Add the array as a type to the attribute list of the udt type,
                                            typenode.add_attribute(A);
                                        }
                                        else if (type_category == "distinct") {
                                            // This cannot happen, an attribute cannot have "base"
                                            cerr << "A 'distinct' type is not allowed as type attribute" << schema_name << ":" << type_name << endl;
                                        }
                                        else { // "simple" type  by default
                                            typenode.add_attribute(A);
                                        }
                                    }
                                    // After set the type attributes, add the type to the data type table
                                    DataType_Table.add_type(schema_name, type_name, typenode);
                                }
                            }
                            // cerr << "<type></type> found in schema '" << schema_name << "':" << endl << typenode; // Debug
                            // cerr << "Aux table:'" << DataType_Table.generate_aux_table_name(schema_name, type_name) << endl << endl; // Debug
                        }
                        else {
                           cerr << "Found type with no name nor category" << endl;
                        }
                    }
                }
            }
        }

    public:
        // Print raw
        void print_tree(){
            if (pRootElem) {
                IDA_xml_utils::print_tree(pRootElem);
            }
        }

        // Print using process_tree() method with print_element() function
        void print_full_tree(){
            if (pRootElem) {
                IDA_xml_utils::process_tree(pRootElem, print_element, "", 0);
            }
        }

        // Build a list of strings with the name of those schemas in the siard file
        // matching the regex 'schema_filter'.
        // Set 'nschemas' the total number of schemas in the file.
        vector<string> get_schemas(const char* schema_filter, unsigned long &nschemas)
        {
            // This vector is a list with the name of schemas matching the regex filter
            vector<string> schema_list;

            nschemas = 0;
            if (pRootElem) {
                if (schema_filter == NULL) schema_filter = "";

                vector<XMLElement*> schemas;
                IDA_xml_utils::find_elements_by_tag(pRootElem, "schema", schemas, 2);

                regex schema_re(schema_filter, std::regex_constants::icase);
                nschemas = schemas.size();
                for (unsigned long is = 0; is < schemas.size(); is++){
                    XMLElement *sch = schemas[is];

                    string schema_name = IDA_xml_utils::find_elementText_by_tag(sch, "name");
                    if (!regex_search(schema_name, schema_re)) {
                        // Skip this schema if not matching the filter
                        continue;
                    }
                    schema_list.push_back(schema_name);
                }
            }

            return schema_list;
        }

        // Count the number of tables, rows and cells in a schema with name 'schema_name'
        // Return the number total of schemas in the siard file
        void get_schema_stats(const string &schema_name, long &ntables, long &nrows, long &ncells)
        {
            ntables = 0;    // tables of this schema
            nrows = 0;      // total rows of this schema
            ncells = 0;     // total cells of this schema
            if (pRootElem) {

                vector<XMLElement*> schemas;
                IDA_xml_utils::find_elements_by_tag(pRootElem, "schema", schemas, 2);

                XMLElement *schema_found = NULL;
                for (unsigned long is = 0; is < schemas.size(); is++) {
                    XMLElement *sch = schemas[is];
                    string this_schema_name = IDA_xml_utils::find_elementText_by_tag(sch, "name");
                    if (!this_schema_name.compare(schema_name)) {
                        // A schema with this name found
                        schema_found = sch;
                        break;
                    }
                }

                if (!schema_found) return; // No schema with this name

                XMLElement *schema_tables;
                vector<XMLElement*> tables; // <tables> <table> ...</table> ... </tables>
                schema_tables = IDA_xml_utils::find_element_by_tag(schema_found, "tables");
                IDA_xml_utils::find_elements_by_tag(schema_tables, "table", tables, 1);

                // Figures for this schema
                ntables = tables.size(); // tables of this schema
                long ntablerows = 0;     // rows of this table
                for (unsigned long it = 0; it < tables.size(); it++){
                    XMLElement *tab = tables[it];
                    string table_rows = IDA_xml_utils::find_elementText_by_tag(tab, "rows");
                    ntablerows =  stol(table_rows);
                    nrows +=  ntablerows;

                    XMLElement *table_columns;
                    vector<XMLElement*> columns;  // <columns> <column>...</column> ... </columns>
                    table_columns = IDA_xml_utils::find_element_by_tag(tab, "columns");
                    IDA_xml_utils::find_elements_by_tag(table_columns, "column", columns, 1);
                    ncells += ntablerows * columns.size();
                }
            }
        }

        void print_schemas(const char* schema_filter = ".")
        {
            unsigned long nschemas;  // Total schemas in siard file
            vector<string> schema_list = get_schemas(schema_filter, nschemas); // List of schemas matching regex filter
            cout << "SIARD version " << get_version_from_metadata_xml() << endl;
            if (schema_filter && *schema_filter)
                cout << "Found " << schema_list.size() << " schemas (out of " << nschemas << ") matching regexp '" << schema_filter << "':" << endl;
            else
                cout << "Found " << schema_list.size() << " schemas:" << endl;
            for (auto s: schema_list){
                long ntables=0, nrows=0, ncells=0;
                get_schema_stats(s, ntables, nrows, ncells);
                cout << "  " << s << ": " << ntables << " tables, " << nrows << " rows, " << ncells << " cells" << endl;
            }
        }

        string get_version_from_metadata_xml(){
            string version;
            XMLElement *siardArchive = pRootElem;
            // TODO: check siardArchive is of the tag <siardArchive>
            version = IDA_xml_utils::get_attribute_value(siardArchive, "version", "unknown");
            return version;
        }

        // Dump the siard tree as a sqlite3 SQL
        //
        // Only schemas matching the c++ regular expression 'schema_filter'
        // are converted
        //
        // Crontroling extra info as sql comments with verbose:
        //   verbose=0 (only siard version and no. of schemas), 1 (schema info),
        //           2 (table info w/o path), 3 (table info w/ paths)
        //          +4 (use (verbose-3) when parsing the contents, that is, the table<N>.xml files
        //
        // Call this method:
        //   To print the SQL to stdout:
        //      parsed_tree_to_sqlite3(cout)
        //
        //   To print the SQL to a file:
        //      ofstream sqlout(file);
        //      if (sqlout.good()) parsed_tree_to_sqlite3(sqlout)
        //
        void tree_to_sql(ostream &sqlout = cout, const char *schema_filter = "", int verbose= 2){
            // A header.xml needs to have been loaded
            if (pRootElem) {
                string SQL_create_table = "";

                unsigned long iuk = 0; // candidate key (=unique index) global counter

                const string siardURI  = this->siardURI;
                string tmpdir = this->tmpdir;

                string siard_lobfolder = IDA_xml_utils::find_first_child_elementText_by_tag(pRootElem, "lobFolder");
                #if 0
                // Debug siard lobfolder
                if (siard_lobfolder.empty()) cerr << ANSI_COLOR_BLUE << "No lobFolder for siardArchive" << ANSI_COLOR_RESET << endl;   // Debug
                else cerr << ANSI_COLOR_BLUE << "lobFolder for siardArchive: '" << siard_lobfolder << "'" << ANSI_COLOR_RESET << endl; // Debug
                #endif

                string version = get_version_from_metadata_xml();
                sqlout << "-- siard version=" << version << endl;

                vector<XMLElement*> schemas;
                IDA_xml_utils::find_elements_by_tag(pRootElem, "schema", schemas, 2);
                sqlout << "-- no. of schemas=" << schemas.size() << endl;

                set<string> seen_tables; // To skip replicated tables
                set<pair<string, string>> rep_tables;
                map<string,string> table_first_schema;

                // First, register all user-defined data types (distinct, udt)
                // Compute here because schemas can use udts defined in subsequent schemas
                for (unsigned long is = 0; is < schemas.size(); is++){
                    XMLElement *sch = schemas[is];
                    string schema_name = IDA_xml_utils::find_elementText_by_tag(sch, "name");
                    add_complex_data_type(sch, schema_name);
                }

                // Main loop to generate SQL of all schemas
                for (unsigned long is = 0; is < schemas.size(); is++){
                    XMLElement *sch = schemas[is];

                    string schema_name = IDA_xml_utils::find_elementText_by_tag(sch, "name");
                    string schema_folder = IDA_xml_utils::find_elementText_by_tag(sch, "folder");

                    if (schema_filter && *schema_filter) {
                        regex schema_re(schema_filter, std::regex_constants::icase);
                        if (!regex_search(schema_name, schema_re)) {
                            // Skip this schema if not matching the filter
                            continue;
                        }
                    }
                    (verbose > 0) && sqlout << "-- schema='" << schema_name << "'"<< endl;

                    XMLElement *schema_tables;
                    vector<XMLElement*> tables; // <tables> <table> ...</table> ... </tables>
                    schema_tables = IDA_xml_utils::find_element_by_tag(sch, "tables");
                    IDA_xml_utils::find_elements_by_tag(schema_tables, "table", tables, 1);
                    (verbose > 0) && sqlout << "-- no. of tables=" << tables.size() << endl;

                    for (unsigned long it = 0; it < tables.size(); it++){
                        XMLElement *tab = tables[it];

                        string table_name = IDA_xml_utils::find_elementText_by_tag(tab, "name");
                        string table_rows = IDA_xml_utils::find_elementText_by_tag(tab, "rows");
                        string table_folder = IDA_xml_utils::find_elementText_by_tag(tab, "folder");

                        // Skip replicated table names (tables with the same name appearing in different schemas)
                        // Only the first occurrence is left
                        if (seen_tables.count(table_name)){
                            rep_tables.insert(pair<string,string>(schema_name, table_name));
                            continue;
                        }
                        seen_tables.insert(table_name);
                        if (table_first_schema[table_name].empty()){
                           table_first_schema[table_name] = schema_name;
                        }

                        (verbose > 1) && sqlout << "--  table='" << table_name << "'"<< endl;
                        (verbose > 1) && sqlout << "--  rows='"  << table_rows << "'"<< endl;

                        SQL_create_table = "CREATE TABLE '" + table_name + "' (\n";

                        XMLElement *table_columns;
                        vector<XMLElement*> columns;  // <columns> <column>...</column> ... </columns>
                        table_columns = IDA_xml_utils::find_element_by_tag(tab, "columns");
                        IDA_xml_utils::find_elements_by_tag(table_columns, "column", columns, 1);
                        (verbose > 1) && sqlout << "--  no. of columns=" << columns.size() << endl;

                        // This array has the name of columns
                        vector<string> siard_colname_v(columns.size());
                        // This type attribute array has the siard type of each column
                        vector<IDA_SIARD_type_attribute> siard_coltype_v(columns.size());
                        // This array has the lob folder information for each column
                        vector<IDA_SIARDlobfolder> siard_lobfolder_info_v(columns.size());

                        for (unsigned long ic = 0; ic < columns.size(); ic++) {
                            XMLElement *col = columns[ic];

                            // Get the SIARD data type of the column
                            string column_name;
                            string siard_column_type;
                            column_name = IDA_xml_utils::find_elementText_by_tag(col, "name");
                            bool complex_type = false;
                            IDA_SIARD_type_attribute tname(col); // This represents the type (simple, array, udt, ...) for the column
                            siard_column_type = IDA_xml_utils::find_elementText_by_tag(col, "type");

                            string ext_category = tname.get_extended_category();
                            if (ext_category == "simple") {
                                // Basic simple type (INTEGER,TEXT,BLOB,...)
                                tname.setTypeSchema(""); // Simple types has no typeSchema
                            }
                            else if (ext_category == "array") {
                                // It's array, it has cardinality
                                // Array declaration found in column: as it is anonymous create one complex type for it
                                string new_array = DataType_Table.add_array_data_type(schema_name, col);
                                siard_column_type = "ARRAY(" + to_string(tname.getCardinality()) + ") of " + siard_column_type; // Adding this suffix will map type to default, i.e., TEXT
                                // Note that now it's a complex type: this new array
                                tname.setType("");
                                tname.setTypeSchema(schema_name);
                                tname.setTypeName(new_array);
                                complex_type = true;
                            } else {
                                // Distinct or user-defined type (udt)
                                siard_column_type = "(udt)";
                                string type_name = IDA_xml_utils::find_elementText_by_tag(col, "typeName");
                                string type_schema = IDA_xml_utils::find_elementText_by_tag(col, "typeSchema");
                                if (!type_name.empty()) siard_column_type = type_name;
                                complex_type = true;
                            }
                            if (complex_type){
                                cerr << "Notice: complex type in column '" << column_name << "' of table '" << schema_name << ":" << table_name << "' encoded as json text" << endl;
                                siard_column_type += " [complex type, encoded as json text]";
                            }

                            // The name of the column
                            siard_colname_v[ic] = column_name;

                            // The type of the column
                            siard_coltype_v[ic] = tname;

                            enum IDA_siard_utils::SQLITE_COLTYPES sqlite3_coltype;
                            sqlite3_coltype = IDA_siard_utils::siard_type_to_sqlite3(siard_column_type);
                            string sqlite3_type = IDA_siard_utils::coltype_to_str(sqlite3_coltype);
                            (verbose > 1) && sqlout << "--   column='" << column_name << "' (" << siard_column_type << " -> " << sqlite3_type << ")" << endl;

                            SQL_create_table += "'" + column_name + "' " + sqlite3_type;
                            if (ic < columns.size()-1 )
                                SQL_create_table += ",\n";

                            // External files (lobFolder information for this column)
                            siard_lobfolder_info_v[ic].init(siardURI, column_name, col, siard_lobfolder);
                            #if 0
                            // Debug table lobfolder
                            cerr << ANSI_COLOR_RED << "Table " << table_name << " -> "
                                 << ANSI_COLOR_BLUE << siard_lobfolder_info_v[ic] << ANSI_COLOR_RESET; // Debug
                            #endif
                        }

                        XMLElement *table_primarykey;
                        vector<XMLElement*> primarykey_columns;  // <table> <primaryKey> <name> <column> <column> ...
                        table_primarykey = IDA_xml_utils::find_element_by_tag(tab, "primaryKey");
                        IDA_xml_utils::find_elements_by_tag(table_primarykey, "column", primarykey_columns);

                        // Add primary key when creating the table
                        // CREATE TABLE table_name(c1, c2, ..., PRIMARY KEY (c1, c2))
                        string SQL_primary_key = ",\n   PRIMARY KEY (";
                        long cpk = 0;
                        for (auto s: primarykey_columns) {
                            string pk_column_name = s->GetText();
                            SQL_primary_key += "\n   " + pk_column_name + ",";
                            cpk++;
                        }
                        SQL_primary_key[SQL_primary_key.size()-1] = ')'; // Last ',' -> ')'
                        SQL_primary_key += "\n";
                        if (cpk++) {
                            // Add P.K. to the statement to create the table
                            SQL_create_table += SQL_primary_key;
                        }

                        SQL_create_table += ");\n" ;

                        // Print SQL "create table ..."
                        sqlout << SQL_create_table;

                        // Locating path of the file "table<N>.xml" with the content of the table
                        string table_path;
                        string table_file;
                        bool table_file_ok;

                        table_path = siardURI + "/content/" + schema_folder+ '/' + table_folder;
                        table_file = table_path + '/' + IDA_file_utils::get_basename(table_folder) + ".xml";
                        (verbose > 2) && sqlout << "--  path='" << table_path << endl;
                        (verbose > 2) && sqlout << "--  table file='" << table_file;
                        if (SIARD_FILE_BY_FILE_UNZIP == unzipmode) {
                            table_file = IDA_file_utils::unzipURI(table_file, tmpdir);
                        }
                        ifstream tf(table_file.c_str());
                        table_file_ok = tf.good();
                        (verbose > 2) && sqlout << "->" << (table_file_ok?" XML file OK":" XML file not found") << endl;


                        // Read the table file to generate SQL for data insertion
                        if (table_file_ok) {
                            // Parse and print the table xml file
                            IDA_SIARDcontent C(table_name,
                                               siardURI, tmpdir, unzipmode,
                                               sqlout,
                                               columns.size(),
                                               siard_colname_v,
                                               siard_coltype_v, siard_lobfolder_info_v);
                            int errl = C.load(table_file);
                            //C.print_tree();              //debug
                            //cerr << ">>>---<<<" << endl; // debug
                            //C.print_full_tree();         // debug
                            //cerr << ">>>---<<<" << endl; // debug
                            if (!errl) {
                                C.tree_to_sql(std::max(0, verbose - 3));
                                cerr << "OK converting '" << table_file << "' to sql" << endl; // Debug
                            } else {
                                cerr << "Error loading file '" << table_file << "'" << endl;
                            }
                            tf.close();
                        }

                    #ifndef IDA_FULL_UNZIP
                        IDA_file_utils::delete_temp_file(tmpdir, table_file);
                    #endif

                        // Add unique indexes (siard candidate keys)
                        // <table> <candidateKeys> <candidateKey> <name> <column> <column> ... </candidateKey> .... <candidateKeys> </table>
                        string SQL_unique_index;
                        XMLElement *table_candidate_keys = IDA_xml_utils::find_element_by_tag(tab, "candidateKeys");
                        vector<XMLElement*> candidate_keys;
                        IDA_xml_utils::find_elements_by_tag(table_candidate_keys, "candidateKey", candidate_keys, 2);
                        for (unsigned long ick = 0; ick < candidate_keys.size(); ick++) {
                            XMLElement *ck = candidate_keys[ick];
                            string candidatekey_name = IDA_xml_utils::find_elementText_by_tag(ck, "name");
                            vector<XMLElement*> candidatekey_columns;
                            IDA_xml_utils::find_elements_by_tag(ck, "column", candidatekey_columns, 2);
                            //CREATE UNIQUE INDEX name_idx ON table (column1, column2);
                            SQL_unique_index += "CREATE UNIQUE INDEX unique_idx" + to_string(iuk) + "_" + candidatekey_name;
                            SQL_unique_index += " ON " + table_name + " (";
                            for (auto s: candidatekey_columns) {
                                string ck_column_name = s->GetText();
                                SQL_unique_index += "\n  " + ck_column_name + ",";
                            }
                            SQL_unique_index[SQL_unique_index.size()-1] = ')'; // Last ',' -> ')'
                            SQL_unique_index += ";\n";
                            iuk++;
                        }
                        sqlout <<  SQL_unique_index;
                    }
                }

                if (!rep_tables.empty()) {
                    (verbose > 0) && cerr << endl;
                    (verbose > 0) && cerr << "Warning: found table names repeated in different schemas:" << endl;
                    for (auto it = rep_tables.begin(); it != rep_tables.end(); it++) {
                        string table_name = it->second, schema_name = it->first;
                        (verbose > 0) && cerr << "  skipped table '" << table_name << "' in schema '" << schema_name << "' (1st ocurrence in schema '" << table_first_schema[table_name] << "')" << endl;
                    }
                }

                #if 0
                // Debug: complex data type summary
                {
                    auto datatype_dict = DataType_Table.getDatatypeDict();
                    if (!datatype_dict.empty()) {
                        cerr << endl;
                        cerr << "Complex type summary:" << endl;
                        for (auto i: datatype_dict) {
                            auto key = i.first;
                            string schema_name = key.first;
                            string type_name = key.second;
                            cerr << " -- TYPE: " << '(' << schema_name << ", " << type_name << ')' << endl;
                            cerr << datatype_dict[key];
                            cerr << " Aux table:'" << DataType_Table.generate_aux_table_name(schema_name, type_name) << endl;
                            cerr << " --" << endl;
                        }
                    }
                    //cerr << "not existing type aux table:'" << DataType_Table.generate_aux_table_name("foo", "moo") << "'" << endl;
                }
                #endif
            } else {
                cerr << "No XML metadata file loaded yet" << endl;
            }
        }

        // This version of this method use a filename
        void tree_to_sql(string outfilename, const char *schema_filter = ".", int verbose= 2)
        {
            ofstream sqloutfile(outfilename);
            if (!sqloutfile.good()){
                cerr << "Error opening output sqlite file '" << outfilename << "'" << endl;
                return;
            }
            // Raise exception if the file has any bad bit (ofstream::badbit, ofstream::eofbit, ofstream::failbit)
            sqloutfile.exceptions(~std::ofstream::goodbit);
            try {
                tree_to_sql(sqloutfile, schema_filter, verbose);
            } catch (const std::exception &e) {
                // catch anything thrown within try block that derives from std::exception
                cerr << "*EXCEPTION converting to SQL; " << "  what: '" << e.what() << "'" << endl;
            } catch (...){
                cerr << "*Unknown EXCEPTION converting to SQL; " << endl;
            }
        }
    }; /* class IDA_SIARDmetadata */
} /* namespace IDA */

/* C public API */

#ifdef __cplusplus
extern "C" {
#endif
    // See these tutorials and info:
    // https://shilohjames.wordpress.com/2014/04/27/tinyxml2-tutorial/
    // https://terminalroot.com/how-to-parser-xml-with-tinyxml2-cpp/

    using namespace IDA;

    __attribute__((constructor)) void IDA_siard2sqlite_initializer_fun()
    {
        #ifdef __ivm64__
        srand(rand() + (unsigned long)__func__);
        #else
        srand(rand() + ::time(NULL));
        #endif
    }

    // This is the main C function in charge of converting SIARD to
    // sqlite3-compliant SQL
    //
    // Argument siardfilein can be:
    //   - A regular SIARD (.zip) file
    //   - A directory with the unzipped SIARD file (containing
    //     subdirectories 'header' and 'content')
    //
    // Argument sqlfileout can be:
    //   - A regular file where to write SQL to
    //   - NULL:   If sqlfileout is NULL, file /header/metadata.xml is only unzipped
    //             or searched in directory, and a summary of the schemas in metadata
    //             is printed instead of doing the conversion
    //
    // The schema_filter is a regular expression to filter schemas
    // by name; only those schema names matching it will be converted
    // Use "" to not filter.
    //
    int IDA_siard2sql(const char *siardfilein, const char *sqlfileout, const char *schema_filter)
    {
        string realsiard = IDA_file_utils::get_realpath(siardfilein);
        if (realsiard.empty()){
            fprintf(stderr, "File/directory '%s' not found\n", siardfilein);
            return -1;
        }

        if (!IDA_parsing_utils::is_valid_regex(schema_filter)){
            fprintf(stderr, "Schema filter '%s' is not a valid regexp expression\n", schema_filter);
            return -1;
        }

        // If schema_filter is NULL, no filter is applied
        if (!schema_filter){
            schema_filter = "";
        }

        IDA_SIARDmetadata M(siardfilein);
#ifdef IDA_FULL_UNZIP
        M.unzip(!sqlfileout);
#endif
        int lerr = M.load();
        if (lerr == -1){
            cerr << "Error opening metadata file " << endl;
            return -1;
        }

        //  If sqlfileout is not null generate sqlite3 SQL from the siard just parsed
        //  else print only a summary of schemas
        if (sqlfileout) {
            M.tree_to_sql(sqlfileout, schema_filter);
        }

        // Printing schemas requires only header/metadata.xml
        puts("");
        M.print_schemas(schema_filter);
        puts("");

        // After conversion, print the size of the generated SQL file
        if (sqlfileout) {
            struct stat stmp;
            int rc = stat(sqlfileout, &stmp);
            long fs =  rc == 0 ? stmp.st_size : -1;
            printf("SQL file: '%s' %ld bytes (%.2f%sB)\n",
                IDA_file_utils::get_realpath(sqlfileout).c_str(), fs, (fs>0)?HUMANSIZE(fs):-1, (fs>0)?HUMANPREFIX(fs):"");
        }

        #ifdef __ivm64__
        {
        char *pbrk = (char*)sbrk(0);
        char *pstk = (char*)&sqlfileout;
        long freeheap = pstk-pbrk;
        cerr << "\nInfo: " << HUMANSIZE(freeheap) << HUMANPREFIX(freeheap) << "B" << " of free space over heap" << endl;
        }
        #endif

        return 0;
    }

#ifdef __cplusplus
}
#endif
