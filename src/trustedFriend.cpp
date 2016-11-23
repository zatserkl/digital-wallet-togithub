// Andriy Zatserklyaniy <zatserkl@ucsc.edu> Nov 10, 2016

/**
 * @file trustedFriend   Implementation of the Graph class for PayMo network.
 * 
 * @author Andriy Zatserklyaniy
 * @date 11/10/16
 */

//
//  changes Nov 12, 2016
//
//  1) turn off search for the batch dataset
//  2) fix processing line reporting
//

#include "Graph.h"

#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdarg>
#include <queue>
#include <ctime>

using std::cout;    using std::endl;

namespace namespace_debug
{
    bool debug = false;

    int test_id1 = -1;
    //-- int test_id1 = 67213;          // test id1 = 67213. He made the smallest number of payments(13)
    //-- int test_id1 = 2481;           // test id1 = 2481 made the max number of payments of 2576
    //-- int test_id1 = 49466;          // the first id1 in data file
    //-- int test_id1 = 52349;          // the 2nd id1 in data file
    //-- int test_id1 = 32639;          //-- the 3rd id1 in data file: the most interesting: 7 id2 with 4 to 7 payments

    int ntest_id1 = 0;                  // counter for lines with test_id1
}

class TrustedPay: public Graph {
protected:
    int maxdepth_;
    // buffers for bfsSearch
    std::vector<int> dist_;
    std::vector<int> pred_;
public:
    TrustedPay(int maxdepth): Graph(), maxdepth_(maxdepth) {cout<< "TrustedPay::TrustedPay: numVertices() = " << numVertices() <<endl;}

    void processPayment(int id1, int id2)
    {
        //
        //  just connect two vertices with edge: transaction no question asked
        //

        unsigned max = id1 > id2? id1: id2;
        if (max >= vertices_.size()) {
            vertices_.resize(max+1);
            dist_.resize(max+1, std::numeric_limits<int>::max());
            pred_.resize(max+1, -1);
        }

        addEdgeSort(id1, id2, 1);    // register transaction
    }

    void processPayment(int id1, int id2, std::ofstream& ofile1, std::ofstream& ofile2, std::ofstream& ofile3)
    {
        //
        //  Launches Breadth-First Search Algorithm and handles the results
        //

        unsigned max = id1 > id2? id1: id2;
        if (max >= vertices_.size()) {
            vertices_.resize(max+1);
            dist_.resize(max+1, std::numeric_limits<int>::max());
            pred_.resize(max+1, -1);
        }

        int id_search_distance = bfsSearch(id1, id2);

        if (id_search_distance < 0) {
            ofile1 << "unverified" <<endl;
            ofile2 << "unverified" <<endl;
            ofile3 << "unverified" <<endl;
        }

        if (id_search_distance >= 0 && id_search_distance <= 1) {
            ofile1 << "trusted" <<endl;
            ofile2 << "trusted" <<endl;
            ofile3 << "trusted" <<endl;
        }

        if (id_search_distance > 1 && id_search_distance <= 2) {
            ofile1 << "unverified" <<endl;
            ofile2 << "trusted" <<endl;
            ofile3 << "trusted" <<endl;
        }

        if (id_search_distance > 2 && id_search_distance <= 4) {
            ofile1 << "unverified" <<endl;
            ofile2 << "unverified" <<endl;
            ofile3 << "trusted" <<endl;
        }

        addEdgeSort(id1, id2, 1);    // register transaction
    }

    int bfsSearch (int s, int id_search)
    {
        //
        //  Implementation of the Breadth-First Search Alogorithm 
        //  for search for friends at the maximum depth maxdepth_
        //
        //  Input parameters:
        //
        //  s:          vertex to start
        //  id_search:  vertex to search up to depth of maxdepth_
        //
        //  Output:
        //
        //  distance of the id_search to s
        //  Returns -1 if vertex id_search was not found within maxdepth_
        //

        int id_search_distance = -1;    // return value: distance to vertex id_search

        // initialize for all vertices in the graph
        pred_.assign(numVertices(), -1);                                // no predessor
        dist_.assign(numVertices(), std::numeric_limits<int>::max());   // infinite distance
        std::vector<vertexColor> color(numVertices(), White);       // all White: unvisited

        dist_[s] = 0;
        color[s] = Gray;

        std::queue<int> queue;
        queue.push(s);

        while (!queue.empty())
        {
            int u = queue.front();  // take vertex from queue and find its neighbors

            if (maxdepth_ < 0 || dist_[u] == maxdepth_) {
                // no sense to explore this vertex and its children
                queue.pop();
                color[u] = Black;
                continue;
            }

            if (binarySearch(u, id_search)) return dist_[u]+1;

            // enqueue white neighbors
            for (VertexList::const_iterator it=begin(u); it!=end(u); ++it) {
                int v = it->first;

                // NB: all vertices here have dist_[v] = dist_[u] + 1

                //
                // we can check v == id_search right here!
                //
                //-- if (v == id_search) return dist_[u]+1;

                if (color[v] == White) {
	            dist_[v] = dist_[u]+1;
	            pred_[v] = u;
	            color[v] = Gray;
	            queue.push(v);
                }
            }

            queue.pop();
            color[u] = Black;

            //
            // probably we don't need this check
            //
            // if (u == id_search) {
            //     id_search_distance = dist_[u];
            //     break;
            // }
        }

        return id_search_distance;
    }
};

void trustedFriend(const char* ifname_batch, const char* ifname_stream, const char* ofname1, const char* ofname2, const char* ofname3)
{
    //
    //  degree of the friends network
    //
    int maxdepth = 4;

    // int nlines = 100000;
    int nlines = 0;

    // const char* ifname_batch = "paymo_input/batch_payment.csv";
    // const char* ifname_stream = "paymo_input/stream_payment.csv";
    // const char* ofname1 = "paymo_output/output1.txt";
    // const char* ofname2 = "paymo_output/output2.txt";
    // const char* ofname3 = "paymo_output/output3.txt";

    cout<< ifname_batch <<endl;
    cout<< ifname_stream <<endl;
    cout<< ofname1 <<endl;
    cout<< ofname2 <<endl;
    cout<< ofname3 <<endl;

    std::string line;

    char date[16];
    char time[16];
    int id1, id2;
    double amount;

    std::ifstream ifile_batch(ifname_batch);
    if (!ifile_batch) {
        cout<< "Could not find input file " << ifname_batch <<endl;
        return;
    }

    // read header
    std::getline(ifile_batch, line);
    // cout<< line <<endl;

    TrustedPay tructedPay(maxdepth);    // let's start from the size 0

    int nprint = 0;         // the number of lines to print

    for (int iline=0; std::getline(ifile_batch, line); ++iline)
    {
        if (nlines > 0 && iline >= nlines) break;
        //
        // simplifications because we process just one dataset provided
        // 1) assume the same format for all records
        // 2) no out-of-range check for vector
        //

        if (false
            || iline < 10
            || (iline < 10000 && iline % 1000 == 0)
            || (iline < 100000 && iline % 10000 == 0)
            || (iline % 100000 == 0)
           ) {
            time_t rawtime;
            std::time(&rawtime);
            cout<< "processing line " << iline << "\t " << std::ctime(&rawtime);    // NB: ctime inserts \n
        }

        sscanf(line.c_str(), "%s %s %d, %d, %lf", date, time, &id1, &id2, &amount);
        if (iline < nprint) cout<< date << " " << time << " " << id1 << ", " << id2 << ", " << amount <<endl;

        tructedPay.processPayment(id1, id2);

        if (id1 == namespace_debug::test_id1) cout<< ++namespace_debug::ntest_id1 << "\tid1 = " << id1 << " id2 = " << id2 << " amount = " << amount << " size = " << tructedPay.getVertex(id1).size() <<endl;
    }

    // cout<< "tructedPay.numVertices() = " << tructedPay.numVertices() <<endl;

    if (namespace_debug::debug) {
        if (namespace_debug::test_id1 >= 0) {
            cout<< "tructedPay.getVertex(" << namespace_debug::test_id1 << ").size() = " << tructedPay.getVertex(namespace_debug::test_id1).size() <<endl;
            for (VertexList::const_iterator it=tructedPay.getVertex(namespace_debug::test_id1).begin(); it!=tructedPay.getVertex(namespace_debug::test_id1).end(); ++it) {
                cout<< "id1 = " << namespace_debug::test_id1 << " id2: it->first = " << it->first << " weight: it->second = " << it->second <<endl;
            }
        }
    }

    ifile_batch.close();

    //
    // process stream and write output files
    //

    std::ifstream ifile_stream(ifname_stream);
    if (!ifile_stream) {
        cout<< "Could not find input file " << ifname_stream <<endl;
        return;
    }

    std::ofstream ofile1(ofname1);
    std::ofstream ofile2(ofname2);
    std::ofstream ofile3(ofname3);

    // read header
    std::getline(ifile_stream, line);
    // cout<< line <<endl;

    for (int iline=0; std::getline(ifile_stream, line); ++iline)
    {
        if (nlines > 0 && iline >= nlines) break;

        if (false
            || iline < 10
            || (iline < 10000 && iline % 1000 == 0)
            || (iline < 100000 && iline % 10000 == 0)
            || (iline % 100000 == 0)
           ) {
            time_t rawtime;
            std::time(&rawtime);
            cout<< "processing line " << iline << "\t " << std::ctime(&rawtime);    // NB: ctime inserts \n
        }

        sscanf(line.c_str(), "%s %s %d, %d, %lf", date, time, &id1, &id2, &amount);
        if (iline < nprint) cout<< date << " " << time << " " << id1 << ", " << id2 << ", " << amount <<endl;

        tructedPay.processPayment(id1, id2, ofile1, ofile2, ofile3);
    }
}

int main(int argc, char *argv[])
{
    cout<< "argc = " << argc <<endl;  for (int i=0; i<argc; ++i) cout<< i <<" "<< argv[i] <<endl;

    if (argc < 5) {
        cout<< "Usage:\n" << argv[0] << " par1 par2 par3" <<endl;
        return 0;
    }

    const char* ifname_batch = argv[1];
    const char* ifname_stream = argv[2];
    const char* ofname1 = argv[3];
    const char* ofname2 = argv[4];
    const char* ofname3 = argv[5];

    trustedFriend(ifname_batch, ifname_stream, ofname1, ofname2, ofname3);
    return 0;
}
