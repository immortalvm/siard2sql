/*
    siard2sql - A library to translate SIARD format
    to sqlite-compliant SQL

    Immortal Database Access (iDA) EUROSTARS project

    Eladio Gutierrez, Sergio Romero, Oscar Plata
    University of Malaga, Spain

    Oct 2023 - May 2024
*/

#include <iostream>
#include <string>
#include <queue>
#include <set>
#include <regex>

#include "unzip.h"

using namespace std;

namespace IDA {

    // This index is a dictionary with pairs (filename, position)
    // There must be an index for each open zip
    class IDA_ZIP_index {
        map<string, unz_file_pos> zipindex;
        friend class IDA_ZIP_opentable;
    };

    // Zip Table, a table for open zips, each one with its index
    // A cache for open zips with its index (list of its files)
    class IDA_ZIP_opentable {
        map<unzFile, IDA_ZIP_index> ZT; // (descriptor, index)
        map<string, unzFile> ZN;        // (name of the zip, descriptor)
        map<unzFile, string> ZNr;       // reverse ZN map (descriptor, name of the zip)
        set<unzFile> CZ;                // a set of open zips pending to close
    public:

        // Add a new zip to the table of open zip files and
        // an empty index created for it
        void add_open_zip(unzFile uf, const string &zipname) {
            if (ZT.find(uf) == ZT.end()) { // Not yet this key in the mapping
                IDA_ZIP_index zindex;
                ZT[uf] = zindex;   // Copy ctor.; zip index not yet initialized
                ZN[zipname] = uf;  // Open zipfile cache
                ZNr[uf] = zipname; // Reverse open zipfile cache
            }
        }

        // Get an open zip from the table by a file name
        unzFile get_open_zip_by_name(const string &zipname, int *open) {
            if (ZN.find(zipname) != ZN.end()) {
                *open = 1;
                return ZN[zipname];
            } else {
                *open = 0;
                return NULL;
            }
        }


        // Add a new file to the index of an open zip
        // This index is a dictionary with pairs (filename, position)
        void add_file_pos(unzFile uf, const string &filename, unz_file_pos pos) {
            try {
                ZT.at(uf).zipindex[filename] = pos;
            } catch (...) {
                // TODO: show error message or something
            }
        }

        // Get the index position in the zip associated to a filename
        // Return 0 if OK, or an error code otherwise
        int get_file_pos(unzFile uf, const string &filename, unz_file_pos *pos) {
            try {
                *pos = ZT.at(uf).zipindex[filename];
                return 0;
            } catch (...) {
                // TODO: show error message or something
            }
            return 1;
        }

        // Remove an open zip from the table (but not the file itself)
        void remove_open_zip(unzFile uf) {
            try {
                ZT.erase(uf);
                string filename = ZNr.at(uf);
                ZNr.erase(uf);
                ZN.erase(filename);
                CZ.erase(uf);
            } catch (...) {
            }
        }

        // Add an open zip descriptor to the list of open zip files
        // pending to close
        void add_zip_pending_to_close(unzFile uf) {
            try {
                CZ.insert(uf);
            } catch (...) {
            }
        }

        // Get one of the open zip files pending to close
        // Return NULL if none in the set of zips pending to close
        unzFile get_zip_pending_to_close() {
            if (CZ.empty()) return NULL;
            else return *(CZ.begin());
        }

        // Return the number of files in the ZIP index, for an open zip file
        long get_zip_number_of_entries(unzFile uf) {
            try {
                return  ZT.at(uf).zipindex.size();
            } catch (...) {
            }
            return 0;
        }

        // Debugging function, print the ZIP index for an open zip file
        void print_zip_index(unzFile uf, long limit=0) {
            cout << "Index has " << ZT.at(uf).zipindex.size() << " entries" << endl;
            if (limit > 0){
                cout << "Showing the first " << limit << " ones:" << endl;
            }
            long c = 0;
            for (auto i = ZT.at(uf).zipindex.begin(); i != ZT.at(uf).zipindex.end(); i++) {
                cout << i->first << " \t\t\t" << i->second.num_of_file << endl;
                if (limit > 0)
                    if (c++ > limit)
                        break;
            }
        }
    };

}  /* namespace IDA */


/* C public API */

#ifdef __cplusplus
extern "C" {
#endif
    using namespace IDA;

    // Global Table of open zips
    static IDA_ZIP_opentable Z;

    // Get the descriptor of an open zip present in the table
    unzFile get_open_zip_by_name(const char* zipname, int *open) {
        return Z.get_open_zip_by_name(zipname, open);
    }

    // Add an open zip to the table
    void IDA_ZIP_add_open_zip(unzFile uf, const char* zipname) {
        Z.add_open_zip(uf, zipname);
    }

    // Add a file to the index of an open zip
    // A pair (filename,position) is added to the index
    void IDA_ZIP_add_file_to_index(unzFile uf, const char *filename, unz_file_pos pos) {
        Z.add_file_pos(uf, filename, pos);
    }

    // Get the index position in the zip associated to a filename
    // Return 0 if OK, or an error code otherwise
    int IDA_ZIP_get_file_pos(unzFile uf, const char *filename, unz_file_pos *pos) {
        return Z.get_file_pos(uf, filename, pos);
    }

    // Remove an open zip from the table (but not the file itself)
    void IDA_ZIP_remove_open_zip(unzFile uf) {
        Z.remove_open_zip(uf);
    }

    // Add an open zip descriptor to the list of open zip files
    // pending to close
    void IDA_ZIP_add_zip_pending_to_close(unzFile uf) {
        Z.add_zip_pending_to_close(uf);
    }

    // Get one of the open zip files pending to close
    // Return NULL if none in the set of zips pending to close
    unzFile IDA_ZIP_get_zip_pending_to_close() {
        return Z.get_zip_pending_to_close();
    }

    // Print the index of an open zip (for debugging)
    long IDA_ZIP_get_zip_number_of_entries(unzFile uf) {
        return Z.get_zip_number_of_entries(uf);
    }

    // Print the index of an open zip (for debugging)
    void IDA_ZIP_print_index(unzFile uf) {
        return Z.print_zip_index(uf, 128);
    }

#ifdef __cplusplus
}
#endif

