/*
    siard2sql - A library to translate SIARD format
    to sqlite-compliant SQL

    Immortal Database Access (iDA) EUROSTARS project

    Eladio Gutierrez, Sergio Romero, Oscar Plata
    University of Malaga, Spain

    Aug 2023
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

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <libgen.h>
#include <set>

#include "tinyxml2.h"
#include "siard2sql.h"

using namespace tinyxml2;
using namespace std;

namespace IDA {

    // Some useful methods to use when parsing
    class IDA_sqlite_utils{
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
        //   https://siard.dilcis.eu/SIARD%202.2/SIARD%202.2.pdf (pag. 18)
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

            if (regex_search(s, re_int)) {
                return COLTYPE_INTEGER;
            }
            else if (regex_search(s, re_numeric)) {
                return COLTYPE_NUMERIC;
            }
            else if (regex_search(s, re_real)) {
                return COLTYPE_REAL;
            }
            else if (regex_search(s, re_blob)) {
                return COLTYPE_BLOB;
            }
            else {
                // text by default
                return COLTYPE_TEXT;
            }
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
        static string char_array_to_blob_literal(const uint8_t *s, long size)
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

        // Convert the content of a file to a sqlite3 BLOB hex literal
        // "SOS" -> "X'534f53'"
        static string file_to_blob_literal(const string &file)
        {
            FILE* f;
            unsigned char buf[1024]; // This MUST be unsigned
            f = fopen(file.c_str(), "r");
            if (!f) {
                cerr << "Error opening '" << file << "'" << endl;
                return "X''";
            }
            std::ostringstream ss;
            ss << "X'";
            long n;
            while ((n = fread(buf, 1, 1024, f )) > 0) {
                for (long k=0; k<n; k++){
                    ss << setfill('0') << setw(2) << hex << (unsigned long)(unsigned char)buf[k];
                }
            }
            fclose(f);
            ss << "'";
            return ss.str();
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
        static uint8_t* siard_decode(const string &siard_str, bool &has_specials, long &size){
            has_specials = false;
            size = 0;
            if (siard_str.empty()){
                return NULL;
            }
            const char *s_encod = siard_str.c_str();
            uint8_t *s_decod = (uint8_t*) malloc(strlen(s_encod) * sizeof(char));
            if (s_decod) {
                for (long i = 0; i < strlen(s_encod); i++) {
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

    }; /* class IDA_sqlite_utils */

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

        static int pod()
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

            string rl = get_realpath(s);
            if (rl.find(infix) == std::string::npos) {
                cerr << "rrm: infix '" << infix << " not found in '" << s << "' or the directory cannot be deleted" << endl;
                return -1;
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
                        status |= rrm(dd->d_name, infix);
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
                string indent(level, ' '); // Indent with spaces

                cout << setw(3) <<  level << "> " << indent << "tagname='" << elemName << "' text='" << IDA_parsing_utils::trim(elemText) << "'" << endl;

                level++;
                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    print_tree(pe);
                }
                level--;
            }
        }

        // Return the first element found matching the tag name
        static XMLElement* find_element_by_tag(XMLElement *pElem, const string &tagname)
        {
            if (pElem){
                if (!tagname.compare(pElem->Name())) {
                    return pElem;
                }

                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    XMLElement *ret = find_element_by_tag(pe, tagname);
                    if (ret) return ret;
                }
            }
            return NULL;
        }

        // Build an array will all elements found matching the tag name
        static void find_elements_by_tag(XMLElement *pElem, const string &tagname,
                                         vector<XMLElement*> &elements, long maxdepth = 9)
        {
            if (maxdepth < 0) return;
            if (pElem){
                if (!tagname.compare(pElem->Name())) {
                    elements.push_back(pElem);
                }

                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    find_elements_by_tag(pe, tagname, elements, maxdepth-1);
                }
            }
        }

        // Build an array with all elements found whose tag name matches the regexp
        // Version with string: too slow
        static void find_elements_by_tag_regex(XMLElement *pElem, const string &tag_regex_str,
                                               vector<XMLElement*> &elements, long maxdepth = 9)
        {
            if (maxdepth < 0) return;
            if (pElem){
                regex tag_re(tag_regex_str);
                if (regex_match(pElem->Name(), tag_re)) {
                    elements.push_back(pElem);
                }

                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    find_elements_by_tag_regex(pe, tag_regex_str, elements, maxdepth - 1);
                }
            }
        }

        // Version with regex: better performance if regex is constantly the same
        static void find_elements_by_tag_regex(XMLElement *pElem, const regex &tag_regex,
                                               vector<XMLElement*> &elements, long maxdepth = 9)
        {
            if (maxdepth < 0) return;
            if (pElem){
                if (regex_match(pElem->Name(), tag_regex)) {
                    elements.push_back(pElem);
                }

                for (XMLElement *pe=pElem->FirstChildElement(); pe; pe = pe->NextSiblingElement()) {
                    find_elements_by_tag_regex(pe, tag_regex, elements, maxdepth-1);
                }
            }
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
    }; /* class IDA_xml_utils */

    // Clases to handle SIARD header.xml

    // Main class to process  "content/schema<M>/table<N>/table<N>.xml" archive
    class IDA_SIARDcontent{
        XMLDocument doc;
        XMLElement *pRootElem = NULL;
        string xmlfilename;

    public:
        IDA_SIARDcontent()
        {
            clear();
        }

        void clear()
        {
            doc.Clear();
            pRootElem = NULL;
            xmlfilename.clear();
        }

        int load(const char *xmlfile)
        {
            clear();
            XMLError result = doc.LoadFile(xmlfile);
            if (result != XML_SUCCESS){
                // fprintf(stderr, "Error loading XML file '%s': %d\n", xmlfile, result); // Debug
                return -1;
            } else {
                // fprintf(stderr, "OK loading '%s'\n", xmlfile); // Debug
                pRootElem = doc.RootElement();
                // Let us store also the filename
                xmlfilename = xmlfile;
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

        // Out a string with the SQL statements to insert all data in columns
        //
        // The number of columns of the table (ncols) is needed because
        // empty blank columns are not included in the file
        //
        // Extra info can be inserted as sql comments controled by verbose:
        //   verbose=0 (no info), 1 (table info), 2 (extra table info), 3 (per column info)
        void tree_to_sql(string tablename, string siard_dir, ostream &sqlout,
                         unsigned long ncols, enum IDA_sqlite_utils::SQLITE_COLTYPES coltype[],
                         int verbose= 0)
        {
            if (pRootElem) {
                XMLElement *table = IDA_xml_utils::find_element_by_tag(pRootElem, "table");

                string version;
                version = IDA_xml_utils::get_attribute_value(table, "version", "unknown");
                (verbose > 0) && sqlout << "-- table name=" << tablename << " version=" << version << endl;

                //--> vector<IDA_SIARDrow> &rows= dtable.get_subnodes();
                vector<XMLElement*> rows;
                IDA_xml_utils::find_elements_by_tag(pRootElem, "row", rows, 2);
                (verbose > 1)  && sqlout << "-- no. of rows=" << rows.size() << endl;

                // Expression for column tags: <c1>...</c1> <c2>...</c2>
                const regex col_tag_re("c[0-9].*");

                // Arrays to store contents of each column for this row as
                // columns might appear unordered in the xml file? c2,c1,c4... ?
                string colcontent[ncols], colname[ncols], colfilename[ncols], colfilelength[ncols];
                unsigned long colindex[ncols];
                for (unsigned long ec=0; ec<ncols; ec++){
                    colindex[ec] = -1;     // Mark a column not appearing in the xml file
                    colcontent[ec] = "''"; // Empty sql column value
                    // These next are only for information
                    colname[ec] = "";
                    colfilename[ec] = "";
                    colfilelength[ec] = "";
                }
                for (unsigned long ir = 0; ir < rows.size(); ir++){
                    //--> IDA_SIARDrow &row= rows[ir];
                    XMLElement *row= rows[ir];
                    string row_name = "r" + to_string(ir);
                    (verbose > 1) && sqlout << "--  bogus rowname='" << row_name << "'"<< endl;

                    //--> vector<IDA_SIARDcolumn> &columns= row.get_subnodes();
                    vector<XMLElement*> columns;
                    IDA_xml_utils::find_elements_by_tag_regex(row, col_tag_re, columns, 1);

                    (verbose > 1) && sqlout << "--  no. of columns in file=" << columns.size() << endl;
                    if (columns.size() < ncols) {
                        // If the last n columns are empty they are not included in the xml file;
                        // thus, the table could have more columns that the row in the file table<N>.xml
                        (verbose > 1) && sqlout << "--  no. of columns in table='" << ncols << "'" << endl;
                        // cerr << "columns in file=" << columns.size() <<  " | columns in table=" << ncols << endl; // Debug
                    }

                    // Traverse columns of the row, in order of appearence in the file
                    for (unsigned long ic = 0; ic < columns.size(); ic++){
                        //--> IDA_SIARDcolumn &col = columns[ic];
                        XMLElement *col = columns[ic];
                        // Column index is the number after the 'c': c1, c2, ...
                        // Notice the first column has index #1: c1 !!
                        long colid = atol(&(col->Name()[1])) - 1 ;
                        if (colid < 0 || colid >= ncols){
                            cerr << "Column index out of range: " << col->Name() << " (must be positive < " << ncols << ") (" << xmlfilename << ")" << endl;
                            return;
                        }

                        colname[colid] = col->Name();

                        string col_file;
                        col_file= IDA_xml_utils::get_attribute_value(col, "file", "");
                        colfilename[colid] = col_file;

                        string col_filelen;
                        col_filelen= IDA_xml_utils::get_attribute_value(col, "length", "");
                        colfilelength[colid] = col_filelen;

                        // If there is a file, the value to insert is the
                        // hexadecimal sqlite blob form X'12abcdef' of
                        // the file content  (text, blob, clob, vartext, ...)
                        if (!col_file.empty()){
                            string lob_file =  siard_dir + "/" + col_file;
                            string lob_literal = IDA_sqlite_utils::file_to_blob_literal(lob_file);
                            if (coltype[colid] == IDA_sqlite_utils::COLTYPE_TEXT) {
                                // If the affinity of this column is TEXT, cast the hex blob
                                // form to text type
                                colcontent[colid] = "CAST(" + lob_literal + " AS TEXT)";
                            } else {
                                // Affinity BLOB, ...
                                colcontent[colid] = lob_literal;
                            }
                        } else {
                            const char *t = col->GetText();
                            string col_text = t?t:"";
                            if (coltype[colid] == IDA_sqlite_utils::COLTYPE_INTEGER
                                || coltype[colid] == IDA_sqlite_utils::COLTYPE_REAL) {
                                // Integer, float
                                colcontent[colid] = col_text;
                            } else {
                                // Text -> decode siard-encoding strings and express as hex; if no decoding needed, simply quote it
                                bool has_specials = false;
                                long size = 0;
                                uint8_t *col_text_decoded = NULL;
                                col_text_decoded = IDA_sqlite_utils::siard_decode(col_text, has_specials, size);
                                if (col_text_decoded && has_specials) {
                                    // Write as a blob cast to text, as there can be char(0) once decoded
                                    string lob_literal = IDA_sqlite_utils::char_array_to_blob_literal(col_text_decoded, size);
                                    colcontent[colid] = "CAST(" + lob_literal + " AS TEXT)";
                                    free(col_text_decoded);
                                }
                                else {
                                    colcontent[colid] = IDA_sqlite_utils::enclose_sqlite_single_quote(col_text);
                                }
                            }
                        }

                        colindex[colid] = ic;
                    }

                    // Traverse all columns in the row (in file and empty ones)
                    string SQL_insert_into = "INSERT INTO '" + tablename + "' VALUES (";
                    for (unsigned long ec = 0; ec < ncols; ec++){
                        if (colindex[ec] >= 0 && colindex[ec] < columns.size()) {
                            (verbose > 2) && sqlout << "--  bogus columnname='" << colname[ec] << "'" << endl;
                            (verbose > 2) && sqlout << "--    contents='" << colcontent[ec] << "'" << endl;
                            (verbose > 2) && sqlout << "--    file='" << colfilename[ec] << "'" << endl;
                            (verbose > 2) && sqlout << "--    file length='" << colfilelength[ec] << "'" << endl;
                            if (!colfilename[ec].empty()) {
                                string lob_file = siard_dir + "/" + colfilename[ec];
                                (verbose > 2) && sqlout << "--    file path='" << lob_file << "'" << endl;
                            }
                            // if (ec != colindex[ec]) sqlout << "-- column out of order " << col.get_name() << " (" << xmlfilename << ")" << endl; // Debug
                        }

                        SQL_insert_into += colcontent[ec];
                        if (ec < ncols - 1) SQL_insert_into += ",\n";

                        colcontent[ec] = "''"; // Reset contents for next column
                        colindex[ec] = -1;
                    }
                    SQL_insert_into += ");\n";
                    sqlout << SQL_insert_into;
                }
            }
        }
    }; /* class IDA_SIARDcontent */

    // Main class to process the "header/metadata.xml" archive
    class IDA_SIARDmetadata {

        XMLDocument doc;
        XMLElement *pRootElem = NULL;
        string siard_dir = "";

    public:
        IDA_SIARDmetadata()
        {
            clear();
        }

        void clear()
        {
            doc.Clear();
            siard_dir.clear();
            pRootElem = NULL;
        }

        int load(const char *xmlfile)
        {
            clear();
            XMLError result = doc.LoadFile(xmlfile);
            if (result != XML_SUCCESS){
                //fprintf(stderr, "Error loading XML file '%s': %d\n", xmlfile, result);
                return -1;
            } else {
                //fprintf(stderr, "OK loading '%s'\n", xmlfile);
                pRootElem = doc.RootElement();
                // Save the path of the file
                siard_dir = IDA_file_utils::get_realpath(IDA_file_utils::get_dirname(xmlfile) + "/..");
                return 0;
            }
        }

        int load(string xmlfile)
        {
            return load(xmlfile.c_str());
        }

        XMLElement * get_root_element(){
            return this->pRootElem;
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

                    string schema_name = IDA_xml_utils::find_element_by_tag(sch, "name")->GetText();
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
                    string this_schema_name = IDA_xml_utils::find_element_by_tag(sch, "name")->GetText();
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
                    string table_rows = IDA_xml_utils::find_element_by_tag(tab, "rows")->GetText();
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
            //--> string version = header.get_version();
            string version;
            XMLElement *siardArchive = IDA_xml_utils::find_element_by_tag(pRootElem, "siardArchive");
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
        void tree_to_sql(ostream &sqlout = cout, const char *schema_filter = ".", int verbose= 2){
            // A header.xml needs to have been loaded
            if (pRootElem) {
                if (schema_filter == NULL) schema_filter = "";
                string SQL_create_table = "";

                unsigned long iuk = 0; // candidate key (=unique index) global counter

                string siard_dir = this->siard_dir;
                string version = get_version_from_metadata_xml();

                sqlout << "-- siard version=" << version << endl;
                //--> vector<IDA_SIARDschema> &schemas = header.get_schemas();
                vector<XMLElement*> schemas;
                IDA_xml_utils::find_elements_by_tag(pRootElem, "schema", schemas, 2);

                sqlout << "-- no. of schemas=" << schemas.size() << endl;

                set<string> seen_tables; // To skip replicated tables
                set<pair<string, string>> rep_tables;
                map<string,string> table_first_schema;

                for (unsigned long is = 0; is < schemas.size(); is++){
                    //--> IDA_SIARDschema &sch = schemas[is];
                    XMLElement *sch = schemas[is];
                    regex schema_re(schema_filter, std::regex_constants::icase);

                    //--> string schema_name = sch.get_name();
                    string schema_name = IDA_xml_utils::find_element_by_tag(sch, "name")->GetText();
                    //--> string schema_folder = sch.get_folder();
                    string schema_folder = IDA_xml_utils::find_element_by_tag(sch, "folder")->GetText();

                    if (!regex_search(schema_name, schema_re)) {
                        // Skip this schema if not matching the filter
                        continue;
                    }
                    (verbose > 0) && sqlout << "-- schema='" << schema_name << "'"<< endl;

                    //--> vector<IDA_SIARDtable> &tables = sch.get_subnodes();
                    XMLElement *schema_tables;
                    vector<XMLElement*> tables; // <tables> <table> ...</table> ... </tables>
                    schema_tables = IDA_xml_utils::find_element_by_tag(sch, "tables");
                    IDA_xml_utils::find_elements_by_tag(schema_tables, "table", tables, 1);
                    (verbose > 0) && sqlout << "-- no. of tables=" << tables.size() << endl;

                    for (unsigned long it = 0; it < tables.size(); it++){
                        //--> IDA_SIARDtable &tab = tables[it];
                        XMLElement *tab = tables[it];

                        string table_name = IDA_xml_utils::find_element_by_tag(tab, "name")->GetText();
                        string table_rows = IDA_xml_utils::find_element_by_tag(tab, "rows")->GetText();
                        //--> string table_folder = tab.get_folder();
                        string table_folder = IDA_xml_utils::find_element_by_tag(tab, "folder")->GetText();

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

                        // Locating path of the file "table<N>.xml" with the content of the table
                        string table_path = siard_dir + "/content/" + schema_folder+ '/' + table_folder;
                        string table_file = table_path + '/' + IDA_file_utils::get_basename(table_folder) + ".xml";
                        (verbose > 2) && sqlout << "--  path='" << table_path << endl;
                        (verbose > 2) && sqlout << "--  table file='" << table_file;
                        ifstream tf(table_file.c_str());
                        bool table_file_ok = tf.good();
                        (verbose > 2) && sqlout << "->" << (table_file_ok?" XML file OK":" XML file not found") << endl;

                        //--> vector<IDA_SIARDcolumn> &columns = tab.get_subnodes();
                        XMLElement *table_columns;
                        vector<XMLElement*> columns;  // <columns> <column>...</column> ... </columns>
                        table_columns = IDA_xml_utils::find_element_by_tag(tab, "columns");
                        IDA_xml_utils::find_elements_by_tag(table_columns, "column", columns);
                        (verbose > 1) && sqlout << "--  no. of columns=" << columns.size() << endl;

                        enum IDA_sqlite_utils::SQLITE_COLTYPES sqlite3_coltype[columns.size()];
                        for (unsigned long ic = 0; ic < columns.size(); ic++) {
                            //--> IDA_SIARDcolumn &col = columns[ic];
                            XMLElement *col = columns[ic];

                            // Get the SIARD data type of the column
                            string column_name;
                            string column_type;
                            XMLElement *column_name_elem, *column_type_elem;
                            column_name_elem = IDA_xml_utils::find_element_by_tag(col, "name");
                            column_type_elem = IDA_xml_utils::find_element_by_tag(col, "type");
                            column_name = column_name_elem->GetText();
                            bool type_not_supported = false;
                            if (column_type_elem){
                                column_type = column_type_elem->GetText();
                                // Perhaps array
                                XMLElement *col_fields = IDA_xml_utils::find_element_by_tag(col, "fields");
                                if (col_fields){
                                    column_type += "(ARRAY)"; // Adding this suffix will map type to default, i.e., TEXT
                                    type_not_supported = true;
                                }
                            } else {
                                // Not supported type: probably a user-defined type
                                column_type = "<UNKNOWN>";
                                XMLElement *type_name = IDA_xml_utils::find_element_by_tag(col, "typeName");
                                if (type_name) column_type = type_name->GetText();
                                type_not_supported = true;
                            }
                            if (type_not_supported){
                                cerr << "Error: not supported type in column '" << column_name << "' of table '" << schema_name << ":" << table_name << "'" << endl;
                                column_type += " [not supported]";
                            }

                            sqlite3_coltype[ic] = IDA_sqlite_utils::siard_type_to_sqlite3(column_type);
                            string sqlite3_type = IDA_sqlite_utils::coltype_to_str(sqlite3_coltype[ic]);
                            (verbose > 1) && sqlout << "--   column='" << column_name << "' ("<< column_type << " -> " << sqlite3_type << ")" << endl;

                            SQL_create_table += "'" + column_name + "' " + sqlite3_type;
                            if (ic < columns.size()-1 )
                                SQL_create_table += ",\n";
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

                        // Read the table file to generate SQL for data insertion
                        if (table_file_ok) {
                            // Parse and print the table xml file
                            IDA_SIARDcontent C;
                            int errl = C.load(table_file);
                            //C.print_tree();              //debug
                            //cerr << ">>>---<<<" << endl; // debug
                            //C.print_full_tree();         // debug
                            //cerr << ">>>---<<<" << endl; // debug
                            if (!errl) {
                                C.tree_to_sql(table_name, siard_dir, sqlout,
                                              columns.size(), sqlite3_coltype, std::max(0, verbose - 3));
                            } else {
                                cerr << "Error loading file '" << table_file << "'" << endl;
                            }
                        }

                        // Add unique indexes (siard candidate keys)
                        // <table> <candidateKeys> <candidateKey> <name> <column> <column> ... </candidateKey> .... <candidateKeys> </table>
                        string SQL_unique_index = "";
                        XMLElement *table_candidate_keys = IDA_xml_utils::find_element_by_tag(tab, "candidateKeys");
                        vector<XMLElement*> candidate_keys;
                        IDA_xml_utils::find_elements_by_tag(table_candidate_keys, "candidateKey", candidate_keys, 1);
                        for (unsigned long ick = 0; ick < candidate_keys.size(); ick++) {
                            XMLElement *ck = candidate_keys[ick];
                            string candidatekey_name = IDA_xml_utils::find_element_by_tag(ck, "name")->GetText();
                            vector<XMLElement*> candidatekey_columns;
                            IDA_xml_utils::find_elements_by_tag(ck, "column", candidatekey_columns, 1);
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
            tree_to_sql(sqloutfile, schema_filter, verbose);
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

        // The SIARD directory
        string siardDIR = "";

        // Check if siardfilein is a SIARD unzipped directory
        bool validsiarddir = false;
        DIR *d = opendir(siardfilein);
        if (d) {
            FILE *f = fopen((string(siardfilein) + "/header/metadata.xml").c_str(), "r");
            if (f){
                validsiarddir = true;
                fclose(f);
            }
            closedir(d);
            //cerr << "valid siard dir: " << siardfilein << endl; // Debug
        }

        // If siardfilein is NOT a valid SIARD directory,
        // it should be a regular SIARD file; so, create
        // a temporary folder and unzip the file
        if (!validsiarddir) {
            char *tmpbasedir = getenv("TMPDIR");
            // Use /tmp by default, but if $TMPDIR exists
            // it must point to a valid directory
            if (!tmpbasedir || !*tmpbasedir) {
                tmpbasedir = (char *) "/tmp";
                struct stat stmp;
                if (::stat("/tmp", &stmp)) {
                    // Directory /tmp does not exist, try to create it
                    mkdir("/tmp", 0700);
                }
            }
            static long tmpdirno = 0;
            string tmpdir = string(tmpbasedir) + string("/_tmp_siard2sql_") + to_string(tmpdirno++) + "_" + to_string(rand());
            if (mkdir(tmpdir.c_str(), 0700)) {
                perror(string("making temporary dir ").append(tmpdir).c_str());
                return -1;
            }

            //  pushd tmpdir
            if (IDA_file_utils::pushd(tmpdir)){
                perror("pushd");
                return -1;
            }

            // fprintf(stderr, "[%s] siardfilein='%s' sqlfileout='%s' tmpdir='%s' pwd='%s'\n", __func__, siardfilein, sqlfileout, tmpdir.c_str(), cwd); // Debug

            // Unzip siard file
            int ziperr;
            fprintf(stderr, "Unzip SIARD file '%s' in folder '%s' ... \n", siardfilein, tmpdir.c_str());
            if (sqlfileout) {
                // Full unzip
                ziperr = IDA_unzip_siard_full(realsiard.c_str()); // We are using its realpath
            } else {
                // Unzip only metadata
                ziperr = IDA_unzip_siard_metadata(realsiard.c_str()); // We are using its realpath
            }

            if (ziperr) {
                fprintf(stderr, "Error (%d) unzipping '%s'\n", ziperr, siardfilein);
                (void) IDA_file_utils::pod(); // popd before quitting
                return -1;
            }
            puts("");

            // Print siard version found
            char buff[PATH_MAX];
            char *ver = IDA_get_siard_version_from_dir(".", buff, PATH_MAX);
            if (ver) {
                printf("SIARD version: %s\n", ver);
                puts("");
            }

            fprintf(stderr, "Done unzipping SIARD file '%s' in folder '%s' \n", siardfilein, tmpdir.c_str());
            fprintf(stderr, "Converting to sqlite3-compliant SQL ...\n");

            //  Popd
            if (IDA_file_utils::pod()) {
                perror("popd");
                return -1;
            }
            siardDIR = tmpdir;
        } else {
            // If siardfilein is a valid siard unzipped directory
            // use it
            siardDIR = realsiard;
        }

        // cerr << "siarDIR=" << siardDIR << endl; // Debug

        // Load and parse XML files
        IDA_SIARDmetadata S;
        string metadatafile = siardDIR + "/header/metadata.xml";
        int lerr = S.load(metadatafile);
        if (lerr == -1){
            cerr << "Error opening metadata file '" << metadatafile << "'" << endl;
            return -1;
        }
        //S.print_tree();             // Debug
        //S.print_full_tree();        // Debug

        //  If sqlfileout is not null generate sqlite3 SQL from the siard just parsed
        //  else print only a summary of schemas
        if (sqlfileout) {
            S.tree_to_sql(sqlfileout, schema_filter);
        }

        // Printing schemas requires only header/metadata.xml
        puts("");
        S.print_schemas(schema_filter);
        puts("");
        //IDA_parsing_utils::cat(sqlfileout); // Debug

        // Delete recursively the temporary directory
        // If these two strings are NOT equal we are using a temporary directory
        if (siardDIR.compare(realsiard)) {
            int rmerr = IDA_file_utils::rrm( siardDIR, "_siard2sql_");
            if (!rmerr) cerr << "Temporary directory '" << siardDIR << "' deleted" << endl; // Debug
        }

        // After conversion, print the size of the generated SQL file
        if (sqlfileout) {
            struct stat stmp;
            int rc = stat(sqlfileout, &stmp);
            printf("SQL file: '%s' (size: %ld bytes)\n", sqlfileout, rc == 0 ? stmp.st_size : -1);
        }

        return 0;
    }

#ifdef __cplusplus
}
#endif
